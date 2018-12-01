import os
import re
from collections import OrderedDict
import matplotlib.pyplot as plt

import pandas as pd
from typing import List, Tuple

import shutil as sh

import astropy.units as u
from astropy.table import Table
from scipy.interpolate import interp1d
from spiders.utils.spark import GreatCircleDistanceDeg, ColumnIndexer, \
    ProperVelocity

from pyspark.ml.feature import Bucketizer, SQLTransformer

from pyspark.ml import Transformer, Pipeline
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from spiders.objects.misc import WrapperBase
import corner
import src.globals as glo
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame, Window
import spiders.mamposst.utils as mam

import numpy as np

from astropy import cosmology


@f.pandas_udf(StringType(), f.PandasUDFType.SCALAR)
def udf_bucket(key: float):
    bins = np.arange(0, 500)
    labels = pd.cut(bins, bins=bins).astype(str)
    buckets = dict(zip(np.arange(0, labels.size, 1), labels))
    return key.map(lambda x: buckets[int(x)])


@f.pandas_udf(FloatType(), f.PandasUDFType.SCALAR)
def udf_comoving_to_redshift(dist: pd.Series) -> pd.Series:
    x, = glo.cosmo.comoving_distance(np.arange(0, 1.3, 0.0001)),
    y = np.arange(0, 1.3, 0.0001)
    dC_2_z = interp1d(x, y)
    return pd.Series(dC_2_z(dist))


@f.pandas_udf(FloatType(), f.PandasUDFType.SCALAR)
def udf_project_radius(sep, z, u_sep, u_radius):
    u_sep, u_radius = u.Unit(u_sep[0]), u.Unit(u_radius[0])
    sep = (sep.values * u_sep).to(u.arcsec)
    r = sep / cosmology.WMAP7.arcsec_per_kpc_comoving(z.values)
    return pd.Series(r.to(u_radius).value)


# TODO: generalise cosmology and move to spark utils
class ProjectedRadius(Transformer):

    def __init__(self, sepCol, zCol, outputCol, cosmo, inUnit='deg',
                 outUnit='kpc'):
        self.sepCol = sepCol
        self.zCol = zCol
        self.outputCol = outputCol
        self.inUnit = inUnit
        self.outUnit = outUnit
        self.metadata = dict(metadata=dict(unit=self.outUnit))

    def _transform(self, df: DataFrame) -> DataFrame:
        units = f.lit(self.inUnit), f.lit(self.outUnit)
        radius = udf_project_radius(self.sepCol, self.zCol, *units)
        df = df.withColumn(self.outputCol, radius.alias('', **self.metadata))
        return df


class RichnessBucketFixer(Transformer):
    def __init__(self, inputCol: str = 'temp',
                 outputCol: str = 'richness_bucket'):
        super().__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(self.outputCol, udf_bucket(self.inputCol))
        return df.drop(self.inputCol)


def build(sc: SparkContext, out_dir: str):
    def gen_kwargs(char_prefix: str = 'mamposst',
                   models: List[str] = ['iso', 'cst', 'Tiret', 'ML', 'OM']):
        for d in os.listdir(out_dir):
            for model in models:
                path = os.path.join(out_dir, d)
                # prefix (guid) must start with a char, add char_prefix to fix.
                prefix = f'{char_prefix}.{path[-36:]}.{model}'
                yield dict(path=path, prefix=prefix, model=model)

    def _build(path: str, prefix: str, model: str):

        # TODO: weight posterior by n_passages
        # TODO: move buildini to data dir of cluster.

        # Fix file names.
        try:
            # Copy and rename file to be consistent with MAMPOSSt format.
            old = [_ for _ in os.listdir(path) if _.endswith('.csv')][0]
            args = [old, f'{prefix}.dat']
            sh.copy(*[os.path.join(path, _) for _ in args])
        except (OSError, IndexError) as e:
            # File is already renamed, so need to panic.
            pass

        # Create and return builder object.
        builder = mam.IniBuilder(
            data=mam.Data(data=f'{prefix}.dat', dir=path, chaindir='TESTS'),
            models=mam.Models(components=['rs'], anismodel=model,
                              darknormflag=0, darkscaleflag=2),
            variables=mam.Variables(norm=[13.5, 15.3], lrtr=[2, 4],
                                    lc=[-0.5, 1.5], anis0=[-0.95, 0.95],
                                    anisinf=[-0.95, 0.95], anisflag=0),
            cosmology=mam.Cosmology(cosmo=cosmology.WMAP7),
            extra=mam.Extra(prefix=prefix))

        return builder

    builders = sc.parallelize(gen_kwargs()).filter(
        lambda kw: os.path.isdir(kw['path'])).map(lambda kw: _build(**kw))

    return builders


def compile_cosmomc(builder: mam.IniBuilder, numhard: int) -> str:
    exe = f'cosmomc_mamposst_nh{numhard}_{os.environ["HOST"]}'
    if not os.path.isfile(os.path.join(glo.DIR_MAMPOSST, 'bin', exe)):
        prefix = builder._args['prefix']
        cmd_complie = ['autoruncosmomc', '-norun', '-noclean', '-ifort', prefix]
        compiler = WrapperBase(cmd=cmd_complie, logname='_compile.log')
        compiler.run_cmd()
    return exe


def launch(builder: mam.IniBuilder, exe: str, partition: str, nodes: int,
           tasks: int, cpus: int):
    prefix = builder._args['prefix']
    # TODO: provide fulll path to ini.
    # TODO: test from spark.cmd, as I suspect this may not work.
    ini = os.path.join(os.getcwd(), f'{prefix}.ini')
    cmd_run = ['srun', '-p', partition, '-N', nodes, '--ntasks-per-node', tasks,
               '--cpus-per-task', cpus, '--output', f'{prefix}.srun',
               sh.which(exe), ini]
    WrapperBase(cmd=cmd_run, logname='_run.log').run_cmd()


def mamposst_meta(builder: mam.IniBuilder):
    args = builder._args
    prefix, chaindir = args['prefix'], args['chaindir']

    # schema
    ini = open(f'{prefix}.ini', 'r').read()
    cols_params = re.findall(r'param\[(.*?)\]', ini, re.DOTALL)
    numhard = len(cols_params)

    # likelihood is actualy -log(likelihood)
    cols_meta = ['n_passages', 'likelihood']
    cols = cols_meta + cols_params
    fields = [StructField(field, FloatType(), True) for field in cols]

    # Priors
    _priors = builder.variables.__dict__
    priors = {k: v for k, v in _priors.items() if isinstance(v, list)}
    priors = OrderedDict(priors)
    priors.move_to_end('lc', last=False)

    meta = dict(schema=StructType(fields),
                chains=os.path.join(glo.DIR_CHAINS, chaindir))

    return meta, numhard, priors


def mamposst_priors(builder: mam.IniBuilder):
    pass


def mosaic(df: pd.DataFrame) -> pd.DataFrame:
    # Extract metadata from dataframe.
    meta = ['guid', 'model', 'aic', 'bic', 'b']
    guid, model, aic, bic, b = df[meta].iloc[0].values

    # Free-params are those which are not str, int (idx), null or constant.
    par = df.select_dtypes(exclude=['str', 'int']).dropna(axis=1)
    par = par.drop(par.std()[(par.std() == 0)].index, axis=1)

    plt.clf()

    # https://corner.readthedocs.io/en/latest/pages/sigmas.html
    q = [0.16, 0.5, 0.84]
    fig = corner.corner(par.values, labels=par.columns, quantiles=q)

    fig.suptitle(f'{model} | {guid} | AIC {aic:.2f} | BIC {bic:.2f}')

    # TODO: plot maximum likelihood for each param
    theta = list(map(lambda v: (v[1], v[2] - v[1], v[1] - v[0]),
                     zip(*np.quantile(par.values, q=q, axis=0))))

    # https://corner.readthedocs.io/en/latest/pages/custom.html
    ndim = len(theta)
    axs = np.array(fig.axes).reshape((ndim, ndim))

    # Loop over the diagonal
    for i in range(ndim):
        ax = axs[i, i]
        k, v = par.columns[i], theta[i]
        ax.axvline(v[0], color="b")
        label = f'${k} = {v[0]:.2f}^{{{v[1]:.2f}}}_{{-{v[2]:.2f}}}$'
        ax.set_title(label)

    # Loop over the histograms
    for yi in range(ndim):
        for xi in range(yi):
            ax = axs[yi, xi]
            vx, vy = theta[xi][0], theta[yi][0]
            ax.axvline(vx, color='b')
            ax.axhline(vy, color='b')
            ax.plot(vx, vy, 'sb')

    fig.savefig(os.path.join('tri', f'{guid}.{model}.triangle.png'))
    plt.close(fig)

    return df


def mampost_summary(sc: SparkContext, schema: StructType, chains: str,
                    pattern: str = r'mamposst\.(.*?)\.(.*?)_(.*?)\.txt'):
    """
    Two sets of output files are saved to the specified subdirectory of the
    COSMOMC/chains directory.

    mamposst_prefix_n.txt: the chain file, with columns: 1) number of
    passages through the point of parameter space; 2) likelihood; 3+)
    ajustable (free and fixed) parameters.

    mamposst_prefix_n.log: the log file, which provides, for the nth chain,
    the evolution of the MCMC acceptance ratios, minimum negative log
    likelihoods, and the R−1 convergence test values.

    Parameters
    ----------
    sc
    schema
    chains

    Returns
    -------

    """
    # files
    sqlc = SQLContext(sc)
    # files = os.path.join(chains, '*_*.txt')
    files = os.path.join(chains, pattern)
    txt = sc.textFile(files).map(lambda line: [float(x) for x in line.split()])
    df = sqlc.createDataFrame(txt, schema=schema)

    # Extract guid and model from input file name.
    df = df.withColumn("file", f.input_file_name())
    df = df.withColumn("guid", f.regexp_extract(f.col("file"), pattern, 1))
    df = df.withColumn("model", f.regexp_extract(f.col("file"), pattern, 2))
    df = df.withColumn("chain", f.regexp_extract(f.col("file"), pattern, 3))

    # Get step number foreach sample in each chain. This keeps track of shuffle.
    df = ColumnIndexer.transform(df)
    window = Window.partitionBy(f.col('file')).orderBy(f.col("idx"))
    df = df.withColumn("step", f.row_number().over(window=window))
    meta = ['file', 'guid', 'model', 'chain', 'step', 'idx', 'n_passages',
            'likelihood']

    # Drop all columns which are lits.
    _cols = set(df.columns) - set(meta)
    exps = [f.countDistinct(col).alias(col) for col in _cols]
    lits = df.groupby('model').agg(*exps).toPandas().set_index('model')
    lits = lits.drop(meta, axis=1, errors='ignore')
    lits = lits.loc[:, lits.nunique(axis=0).gt(1)]

    # replace default values of fixed parameters with null.
    idx = lits.stack()[lits.stack() == 1].index
    exprs = [f.expr(f'first({col}) as first_{col}_{m}') for m, col in idx]
    first = df.groupby("model").agg(*exprs)
    df = df.join(sqlc.createDataFrame(first.rdd, first.schema), ['model'])

    for m, col in idx:
        fixed = f.expr(f'{col} == first_{col}_{m} AND model == "{m}"')
        df = df.withColumn(col, f.when(fixed, 'null').otherwise(f.col(col)))
    df = df.select(*(meta + lits.columns.tolist()))

    # Calculate the number of free-parameters for each model.
    d = lits.apply(lambda x: x.value_counts(), axis=1).fillna(0)[1]
    d = sqlc.createDataFrame(d.reset_index()).withColumnRenamed('1', 'd')
    df = df.join(d.withColumn('d', f.expr(f'{len(lits.columns)} - d')), 'model')
    # burn first 2000 * number of free-parameters elements for each chain.
    df = df.withColumn('burn', f.expr('2000 * d')).where(f.expr('step > burn'))

    # Evidence approx: https://arxiv.org/abs/0801.0638
    ic = f'-2 * log(max(likelihood)) + 2 * first(d)'
    aic, bic = f'{ic} AS aic', f'{ic} * log(count(likelihood)) AS bic'
    ab = df.groupby('guid', 'model').agg(f.expr(aic), f.expr(bic))
    ab = ab.join(ab.groupby('guid').agg(f.expr('max(bic) AS bic_max')), 'guid')
    ab = ab.withColumn('b', f.expr('exp(-(bic_max - bic)/2) AS b'))
    # clone df to prevent AnalysisException: resolved attribute(s)
    df = sqlc.createDataFrame(ab.rdd, ab.schema).join(df, ['guid', 'model'])

    # TODO: weight by combination of number of passges and baysefactor
    # TODO: make dynamics its own project and repo!

    udf_mosaic = f.pandas_udf(mosaic, df.schema, f.PandasUDFType.GROUPED_MAP)

    sample = df.where(
        'guid == "4bed2804-9e69-6f60-44d8-8100b0d748d1" and model == '
        '"OM"').toPandas()
    # df.groupby('guid', 'model').apply(mosaic).collect()
    udf_mosaic.func(df=sample)


def main(sc: SparkContext, N_Rvir: float = 2):
    sqlc = SQLContext(sc)

    DIR_INTERIM_SIM = glo.DIR_INTERIM.replace('mcmf', 'magneticum')

    files = os.path.join(DIR_INTERIM_SIM, 'wmap.cluster.*.projected.fits')

    # Load cluster photometric calogs as concatenated spark DataFrames.
    df = sqlc.read.format("fits").option("hdu", 1).load(files)
    df = df.withColumn('cp',
                       f.expr(f'dist_comoving_Mpc + ({N_Rvir} * (_Rvir/1000))'))
    df = df.withColumn('cm',
                       f.expr(f'dist_comoving_Mpc - ({N_Rvir} * (_Rvir/1000))'))
    df = df.withColumn('zp', udf_comoving_to_redshift('cp'))
    df = df.withColumn('zm', udf_comoving_to_redshift('cm'))

    df = GreatCircleDistanceDeg(ra1Col='longitude', dec1Col='latitude',
                                ra2Col='_longitude', dec2Col='_latitude',
                                outputCol='sep').transform(df)

    in1d = '(z_true < zp) AND (z_true > zm)'
    df = df.withColumn('inRvir1D', f.when(f.expr(in1d), True).otherwise(False))
    in2d = 'sep < _Rvir_deg'
    df = df.withColumn('inRvir2D', f.when(f.expr(in2d), True).otherwise(False))
    in3d = 'inRvir1D AND inRvir2D'
    df = df.withColumn('inRvir3D', f.when(f.expr(in3d), True).otherwise(False))

    mamposst_prep = Pipeline(
        stages=[# TODO: change cosmollogy when not using simulations.
            ProjectedRadius(sepCol='sep', zCol='z_obs', outputCol='proj_r',
                            cosmo=cosmology.WMAP7, inUnit='deg', outUnit='kpc'),
            ProperVelocity(zgalCol='z_obs', zclusCol='_z_obs',
                           outputCol='prop_v', outUnit='km/s')])

    df = mamposst_prep.fit(df).transform(df)
    df = df.where('inRvir3D')
    # Table.from_pandas(df.toPandas()).write('mamposst.fits',
    # overwrite=True)

    df = df.join(df.where('inRvir3D').groupby('CLUS_ID').count(), 'CLUS_ID')
    df = df.where(f.expr('count > 40'))
    df = df.withColumn('components', f.lit('rs'))

    # To avoid BCG
    df = df.where('proj_r > 50')

    out_dir = os.path.join(os.getcwd(), 'magneticum')

    # write and build
    # df.select('CLUS_ID', 'proj_r', 'prop_v', 'components').write.partitionBy(
    #     "CLUS_ID").csv(out_dir, sep=' ', mode='overwrite')

    builders = build(sc=sc, out_dir=out_dir)
    # builders.foreach(lambda x: x.run_cmd())
    meta, numhard, priors = mamposst_meta(builder=builders.first())

    # Submit mpi jobs from spark.
    exe = compile_cosmomc(builder=builders.first(), numhard=numhard)
    kwargs = dict(exe=exe, partition='express', nodes=1, tasks=8, cpus=4)
    # builders.foreach(lambda _: launch(_, **kwargs))

    # TODO: for some reason some trace files are empty!
    # TODO: change chain dir for each candidate, this will make it easier to
    #  seperate datasets.

    # p = sqlc.createDataFrame(pd.DataFrame(priors))

    summary = mampost_summary(sc=sc, **meta)

    # TODO: velocity errors, put in the  3rd column (in km/s).  #  need   #
    #  to  #  #  add cleaning stuff.


if __name__ == '__main__':
    serializer = "org.apache.spark.serializer.KryoSerializer"
    pkgs = ['com.github.astrolabsoftware:spark-fits_2.11:0.7.1']
    jars = ['/draco/u/jacobic/.local/spark-fits/target/scala-2.11'
            '/spark-fits_2.11-0.7.1.jar']
    # 'databricks:spark-deep-learning:1.2.0-spark2.3-s_2.11']

    spark = SparkSession.builder.master('local[25]').appName('mamposst').config(
        'spark.jars', ','.join(jars)).config("spark.serializer",
                                             serializer).config(
        "spark.driver.extraClassPath", ':'.join(jars)).config(
        "spark.executor.extraClassPath", ':'.join(jars)).getOrCreate(

    ).sparkContext

    main(sc=spark)

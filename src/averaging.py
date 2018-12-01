# TODO: place holder for bayesian model averaging code!

import numpy as np
from pymultinest.solve import solve
import os

if not os.path.exists("chains"): os.mkdir("chains")


# probability function, taken from the eggbox problem.

def myprior(cube):
    return cube * 10 * np.pi


def myloglike(cube):
    chi = (np.cos(cube / 2.)).prod()
    return (2. + chi) ** 5


# number of dimensions our problem has
parameters = ["x", "y"]
n_params = len(parameters)
# name of the output files
prefix = "chains/3-"

# run MultiNest
result = solve(LogLikelihood=myloglike, Prior=myprior, n_dims=n_params,
               outputfiles_basename=prefix, verbose=True)

print()
print('evidence: {logZ:.1f} +- {logZerr:.1f}'.format(**result))
print()
print('parameter values:')
for name, col in zip(parameters, result['samples'].transpose()):
    print(f'{name:>15} : {col.mean():.3f} +- {col.std():.3f}')

# make marginal plots by running:
# $ python multinest_marginals.py chains/3-
# For that, we need to store the parameter names:
import json

with open(f'{prefix}params.json', 'w') as f:
    json.dump(parameters, f, indent=2)

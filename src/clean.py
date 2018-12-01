import numpy as np
import src.globals as glo


def rvirfromPPS(R, v_z, cosmo=glo.comso, gap_parameter, subsample_chocice, rvir,
                i_good, first_step):
    """
    compute virial radius from projected phase space with iterative rejection
    of velocity interlopers

    Parameters
    ----------
    R
    v_z
    h
    gap_parameter
    subsample_chocice
    rvir
    i_good
    first_step

    Returns
    -------

    Notes
    -----
    source: Mamon, Biviano & Boue' 13, appendix
    args:    R v_z h(z) h gap-parameter (0 for no check, <0 for sigma_los
    normalization) subsample-chocice (1: largest, 0: closest to v=0) (all
    input) rvir i_good (both output) [not 1st-step?]
    gap parameter recommended: 4 (without sigma_los normalization)
    """

    if len(R) == 0:
        # #echo Zero particles left!
        return None

    if 9:
        set
        i = $8
    else:
        i = 0, len(R) - 1

    # cosmological and physical parameters
    G = Delta * glo.h

    # estimates of global velocity dispersion
    sigmav = pd.Series.std(v_z)
    # median absolute deviation scale from Beers et al. 1990, AJ 100, 32
    sigmavmad = (pd.Series.median(v_z) - v_z).abs() / 0.6745


    # 1st iteration
    if not 9:
        # set sigma_ap to sigma_MAD
        c = 4
        ipass = 1
        sigmaap = sigmavmad

    else:
        # Use standard unbiased std. dev. AND check for maximum iterations
        if rvguess:
            rvguessold = rvguess
        else:
            rvguessold = (max(R))

            sigmaap = sigmav
        ipass += 1
        if ipass > 3:
            # #echo could not converge after 3 passes ... saving last values
            return None

    # LCDM concentration
    if Delta == 200:
        # Maccio, Dutton & van den Bosch 08, relaxed halos, WMAP5, Delta=200
        slope, norm = -0.098, 6.76
    else:
        # Maccio, Dutton & van den Bosch 08, relaxed halos, WMAP5, Delta=95
        slope, norm = -0.094, 9.35

    Mvtmp = (1e11 * Delta / 2 * glo.cosmo.H(0) * 2 * rvguess ** 3 / G)
    # NFW/Moore/Nav04 concentration parameter (Delta=200) for LCDM
    foo = glo.h * Mvtmp / 1e12
    c = norm * foo ** (slope + coeff * np.log10(foo))

    # guess r_vir using sigma_ap/v_vir as predicted from NFW with
    # concentration of c (=4 on 1st pass)
    vvguess = sigmaap / sigapovervvatrvirnfw(c, ML)
    rvguess = sqrt(2 / Delta) / (0.001 * _h) * vvguess / 100
    Mvguess = 1e11 * rvguess * (vvguess / 100) ** 2 / G

    # printout

    # #echo ipass=ipass (len(R)) particles sigmav=sigmav sigmamad=sigmavmad
    # rvguess=rvguess c=c

    # check for wide gaps on 1st step

    if not 9 and subsample_choice != 0 and len(R) >= 30:
        # set i = 0, len(R) - 1
        # dim i 1
        if subsample_choice > 0:
            # #echo running gapclean with C=subsample_choice ...
            iclean = gapclean(vz, subsample_choice,$6,$rvguess)  # #echo done
            # with gapclean ...
            else:
            if subsample_choice < 0:
                # #echo running gapclean with C=subsample_choice ...
                iclean = gapclean(vz, abs(subsample_choice),$6,$rvguess, R)  #
            #echo
            done
            with gapclean...
        else:
            iclean = 0 * int(R)

    dim
    iclean
    1
    foreach
    _vec(R
    vz
    i) {set
    goodv_$_vec = $_vec if (iclean)
    if (len(goodv_$_vec) == 0) {$7 = 0.0
    set $8 = {}
    return None}

    set
    badv_$_vec = $_vec if (! iclean)# ##echo past gapclean v filter ...
    }
    dim
    goodv_R
    1
    dim
    badv_R
    1} else:
    set
    goodv_R = R
    set
    goodv_vz = vz
    set
    goodv_i = i
    dim
    goodv_R
    1

    # filter R < r_vir^guess

    foreach
    vec(R
    vz
    i) {set
    goodR_$vec = goodv_$vec if (goodv_R < $rvguess)
    if (len(goodR_$vec) == 0):
        $
        7 = 0.0
    set $8 = {}
    return None

    set
    badR_$vec = goodv_$vec if (goodv_R >= $rvguess)
    # ##echo past R filter ...
    dim
    goodR_R
    1
    dim
    badR_R
    1}
    vzmed = (median(goodR_vz))

    # reject interlopers a la MBM10

    # #echo velocity interlopers from median v=$vzmed ...

    set
    vzmax = 2.7 * siglosNFWML1apxnew($c * goodR_R /$rvguess)*sqrt(
        mnfw(1 /$c, 1 /$c) * $c) * $vvguess
    verbose = | verbose
    0
    foreach
    vec(R
    vz
    i) {
    set
    good_$vec = goodR_$vec if (abs(goodR_vz -$vzmed) < vzmax)
    if (len(good_$vec) == 0) {$7 = 0.0
    set $8 = {}
    return}
    set
    bad_$vec = goodR_$vec if (abs(goodR_vz -$vzmed) >= vzmax)}
    verbose $verbose
    verbose = delete

    # printout

    # #echo dim good_R = $(len(good_R)) ... max good_i = $(max(good_i)) min
    # max-good-vz=$(min(good_vz)) $(max(good_vz))

    # check for convergence

    if (9)
    { if (abs($rvguess / $rvguessold-1) < 0.001 & & len(R) == len(good_R)) {
    # ##echo assigning rvir = $rvguess ...
    $7 = $rvguess
    set $8 = good_i
    # #echo converged on pass $ipass rvir=$rvguess ...
    # dim R 1
    # dim good_R 1
    return}}

    # iterate

    $7 = $rvguess
    set $8 = good_i
    set
    inp_R$ipass = good_R
    set
    inp_vz$ipass = good_vz
    # ##echo ending pass $ipass rv = $rvguess
    rvirfromPPS
    good_R
    good_vz
    h
    gap_paramater
    0 $6 $7 $8 $ipass


def gapclean(v_z, wtd_gap_max, subsample_choice, r_vir, R):
    """

    Parameters
    ----------
    v_z
    wtd_gap_max
    subsample_choice
    r_vir
    R

    Returns
    -------

    Notes
    -----
    returns list of 1s and 0s for particles
    velocities are normalized to expected velocity dispersion
    args: v_z wtd-gap-max subsample-choice (1: biggest, 0: closest to v_median) [r_vir (if wtd-gap-max < 0)] [R]
    plot PPS if plotgap is defined
    """
    vorig = v_z
    # vecminmax vorig vzmin vzmax

    # sorted vector of velocities (save indices for 2nd sort back to original order)

    i = 1, len(vorig)
    v = vorig - np.median(vorig)

    # normalize by expected sigma_LOS(R) for c=4 NFW with beta_ML

    if R:
        # normalize by local LOS velocity dispersion
        Rorig = R
        c = 4
        H= 0 (100*h)
        vvi= r (np.sqrt(Delta/2)*H0*r_vir/1000)
        siglos = np.sqrt(c)*np.sqrt(mnfw(1/c,1/c))*vvir*siglosNFWML1apxnew(c*Rorig/r_vir)
        nu = v/siglos
        sort <nu i Rorig vorig>

    else:
        nu = v
        sort <nu i vorig>


    # gaps in [v-Median(v)] [/ sigma_LOS^NFW(R)]

    N = (len(nu))
    N= 1 (N-1)
    nu1 = nu[0] concat nu
    nu2 = nu concat nu[N1]
    j0 = 0, N
    gap = nu2-nu1 if (j0 > 0 && j0 < N)

    # weights

    j = 1, N1
    wt = j*(N1-j)

    # sorted weighted gap

    wtdgap = np.sqrt(wt*gap)
    _fac = wtdgap/midmean(wtdgap)

    # max weighted gap and normalized one

    wtdgapsort = wtdgap
    sort <wtdgapsort j>
    wtdgapma= x (wtdgapsort[len(wtdgapsort)-1])
    fa= c (wtdgapmax/midmean(wtdgap))
    #echo wtdgapmax = wtdgapmax fac = fac

    # if normalized weighted gap is small return 1 everywhere
    # otherwise:
    #	1) determine if largest gap is in lower of higher half of velocity vector
    #	2) place 1s on the side with the most velocities (arg2=1) or closest to v=0 (arg2=2)
    #	3) sort back velocities to original order

    if (fac < wtd_gap_max): # all good
       return_value = 0*v_z + 1

    else :
         jbigga= p (j[N1-1])
         #echo jbiggap = jbiggap ...
         set ii = 1, N
         if subsample_choice == 1:
              if (jbiggap > N1/2) :
                 ga= p hi
              else :
                 ga= p lo

         else :
            # velocity medians on both sides
            dim vorig 1
            vmedori= g (median(vorig))
            set vlo = abs(vorig-vmedorig) if (ii < jbiggap)
            set vhi = abs(vorig-vmedorig) if (ii >= jbiggap)
            if (abs(median(vlo)) < abs(median(vhi))):
               ga= p hi
            else:
               ga= p lo

         set return_value = 0*v_z
         if ('gap' == 'hi') :
            #echo gap = gap: keeping low velocities ...
            set return_value = ii < jbiggap ? 1 : 0
          else:
            #echo gap = gap: keeping high velocities ...
            set return_value = ii < jbiggap ? 0 : 1

         # sort back to initial order
         sort <i return_value vorig>
         # p vorig return_value
         if (?plotgap) {
            devx11
            set j1 = 0, len(nu) - 1
            window 1 2 1 2
            plot2 j1 nu
            limits j1 0 1
            ctype cyan
            uparrow jbiggap 0 0.5
            ctype red
            window 1 2 1 1
            plot2 j wtdgapsort
            ctype 0
            con= t y
            con= t ? <continue? >
       }

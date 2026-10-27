#!/usr/bin/env python
"""
Aperture photometry pipeline for FRAM science frames.

Processes individual FITS files: applies dark and flat calibration, performs
astrometric refinement, detects sources, calibrates photometry against a
reference catalogue, and stores per-object results as Parquet files in a
mirrored directory tree.

Usage:
    python3 extract_photometry.py [options] file1.fits [file2.fits ...]

Options:
    -p / --photodir   Base directory for output Parquet files (default: photometry)
    -D / --dark       Master dark frame
    -F / --flat       Master flat frame
    -C / --cat        Reference catalogue (Parquet)
    -r / --replace    Overwrite existing output files
    -v / --verbose    Verbose output

Requires: astropy, numpy, scipy, stdpipe, statsmodels, calibrate.py
"""

import os
import sys
import datetime
import warnings

import numpy as np

from astropy.wcs import WCS, FITSFixedWarning
from astropy.io import fits
from astropy.io.fits.verify import VerifyWarning
from astropy.table import Table
from astropy.stats import mad_std
from astropy.time import Time

import calibrate
from stdpipe import astrometry, photometry, pipeline, utils

from statsmodels.tools.sm_exceptions import ConvergenceWarning

warnings.simplefilter(action='ignore', category=FITSFixedWarning)
warnings.simplefilter(action='ignore', category=VerifyWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)
warnings.simplefilter(action='ignore', category=ConvergenceWarning)


def get_night(time, lon=None, site=None):
    '''Get night name from UTC time (given as datetime object). Should be identical to how RTS2 does it.'''
    if lon is None:
        if site == 'auger':
            lon = -69.4497
        elif site == 'cta-n':
            lon = -17.89
        elif site == 'cta-s0' or site == 'cta-s1':
            lon = -70.32482
        else:
            lon = 0

    time1 = time + datetime.timedelta(seconds=lon*86400/360 - 86400/2)

    return time1.strftime('%Y%m%d')


def save_objects(filename, obj):
    # Create directory hierarchy as necessary
    dirname = os.path.split(filename)[0]

    try:
        os.makedirs(dirname)
    except:
        pass

    if obj is None:
        # Create dummy empty file
        with open(filename, 'w') as f:
            pass
        return

    obj.write(filename, format='parquet', overwrite=True)


def process_file(filename, night=None, site=None, darkname=None, flatname=None, catname=None, verbose=False, replace=False, base='photometry', _tmpdir=None):

    # Base settings
    _workdir = None

    sn = 3
    initial_aper = 3
    rel_aper = 1.5
    rel_bkgann = None # [5, 7]
    bg_size = 64
    minarea = 3
    # spatial_order = 2
    # fwhm_spatial_order = 2
    sip_order = 2
    # use_color = False # True

    log = (verbose if callable(verbose) else print) if verbose else lambda *args, **kwargs: None

    if not os.path.exists(filename):
        log(f"No such file: {filename}")
        return

    if site is None:
        # Simple heuristics to derive the site name
        for _ in ['auger2', 'auger', 'cta-n', 'cta-s0', 'cta-s1']:
            if _ in filename:
                site = _
                break

    # Rough but fast checking of whether the file is already processed
    if not replace and os.path.exists(os.path.splitext(os.path.join(base, site, '/'.join(filename.split('/')[-4:])))[0] + '.parquet'):
        return

    header = fits.getheader(filename, -1)

    if header['IMAGETYP'] != 'object':
        log(f"Incorrect file type: {header['IMAGETYP']}")
        return

    ccd = header.get('CCD_NAME')
    fname = header.get('FILTER', 'unknown')
    time = utils.get_obs_time(header)
    target = header.get('TARGET', -1)

    if fname not in ['B', 'V', 'R', 'I', 'z', 'N']:
        return

    if fname == 'N': # and site == 'cta-n':
        effective_fname = 'R'
    else:
        effective_fname = fname

    if night is None:
        if header.get('LONGITUD') is not None:
            night = get_night(time, lon=header['LONGITUD'])
        else:
            night = get_night(time, site=site)

    dirname = '%s/%s/%s/%05d/%s' % (base, site, night, target, ccd)
    basename = os.path.splitext(os.path.split(filename)[-1])[0]
    basename = dirname + '/' + basename
    objname = basename + '.parquet'

    if not replace and os.path.exists(objname):
        return

    log(filename, site, night, ccd, fname, effective_fname)

    image = fits.getdata(filename, -1).astype(np.float32)

    log(f"Image: {image.shape[1]} x {image.shape[0]}")

    # Basic calibration
    if darkname:
        dark = fits.getdata(darkname)
    else:
        dark = None

    if flatname:
        flat = fits.getdata(flatname)
    else:
        flat = None

    if dark is None:
        log("No master dark provided, exiting")
        save_objects(objname, None)
        return

    image0,_ = calibrate.crop_overscans(image, header.copy(), subtract=False)
    image,header = calibrate.calibrate(image, header, dark=dark)

    if flat is None:
        flat = np.ones_like(image)

    image *= np.nanmedian(flat)/flat

    log(f"Image calibrated")

    # Basic masking
    mask = ~np.isfinite(image)

    satlevel = 60000
    smask = image0 > satlevel # Saturation, on original image

    dmask = dark > np.median(dark) + 50.0*mad_std(dark) # Hot pixels

    mask |= flat < 0.5 # Highly vignetted regions

    # WCS + catalogue
    wcs = WCS(header)
    pixscale = astrometry.get_pixscale(wcs=wcs)

    ra0,dec0,sr0 = astrometry.get_frame_center(header=header)

    gain = header.get('GAIN', 1.0)
    if gain > 100:
        gain /= 1000

    if catname:
        cat = Table.read(catname)
    else:
        log("No catalogue provided, exiting")
        save_objects(objname, None)
        return

    log(f"{len(cat)} catalogue stars")

    mask_full = mask | smask | dmask

    # Camera-specific configuration
    if ccd in ['C0']:
        spatial_order = 2
        fwhm_spatial_order = 0
        refine_astrometry = False
        use_nonlin = False

    elif ccd in ['NF3', 'NF4']:
        rel_aper = 1
        spatial_order = 2
        fwhm_spatial_order = 2
        refine_astrometry = False
        use_nonlin = True

    elif ccd in ['WF8']:
        spatial_order = 3
        fwhm_spatial_order = 2
        refine_astrometry = False
        use_nonlin = True

    else:
        spatial_order = 3
        fwhm_spatial_order = 2
        refine_astrometry = True
        use_nonlin = False

    # Object extraction
    obj = photometry.get_objects_sep(
        image, mask=mask_full,
        mask_detect=mask, # Simple mask for detection
        thresh=3, r0=0, bg_size=bg_size, minarea=minarea,
        aper=rel_aper, bkgann=rel_bkgann,
        gain=gain,
        fwhm=True, fwhm_spatial_order=fwhm_spatial_order,
        optimal=False, centroid=False, group_sources=True,
        verbose=verbose,
    )
    log(f"{len(obj)} final objects")

    fwhm = photometry.estimate_fwhm_from_objects(obj)
    if not fwhm or not np.isfinite(fwhm):
        fwhm = obj.meta['fwhm_phot']
    log(f"FWHM = {fwhm:.2f} pix")

    # Some fallbacks
    if ccd in ['C0'] and len(obj) < 30:
        spatial_order = 0

    # Astrometric refinement
    if refine_astrometry:
        wcs = pipeline.refine_astrometry(
            obj, cat, wcs=wcs,
            cat_col_ra='ra', cat_col_dec='dec', cat_col_mag=effective_fname,
            order=sip_order, # projection='ZPN',
            verbose=verbose, update=True
        )

        if not wcs or not wcs.is_celestial:
            log(f"Astrometric refinement failed for {filename}")
            save_objects(objname, None)
            return

    obj['ra'], obj['dec'] = wcs.all_pix2world(obj['x'], obj['y'], 0)

    # Match with catalogue
    cidx = np.isfinite(cat[effective_fname])
    cidx &= cat['B'] - cat['V'] > 0.2
    cidx &= cat['B'] - cat['V'] < 1.2

    if site == 'cta-n' and ccd == 'C0':
        color_term = None # {'B': -0.01, 'V': -0.04, 'R': -0.03}.get(effective_fname, None)
    else:
        color_term = None

    col_mag1 = 'B'
    col_mag2 = 'V'

    # Calibration without color term
    m = pipeline.calibrate_photometry(
        obj, cat[cidx], sr=0.5*fwhm*pixscale,
        order=spatial_order,
        # use_color=use_color, force_color_term=color_term,
        use_color=False, # force_color_term=-0.07,
        accept_flags=0x01, max_intrinsic_rms=0.02,
        cat_col_ra='ra', cat_col_dec='dec',
        cat_col_mag=effective_fname, cat_col_mag1=col_mag1, cat_col_mag2=col_mag2,
        verbose=verbose, bg_order=None, nonlin=use_nonlin,
    )

    # Calibration with color term
    mc = pipeline.calibrate_photometry(
        obj, cat[cidx], sr=0.5*fwhm*pixscale,
        order=spatial_order,
        # use_color=use_color, force_color_term=color_term,
        use_color=True, # force_color_term=-0.07,
        accept_flags=0x01, max_intrinsic_rms=0.02,
        cat_col_ra='ra', cat_col_dec='dec',
        cat_col_mag=effective_fname, cat_col_mag1=col_mag1, cat_col_mag2=col_mag2,
        verbose=verbose, bg_order=None, nonlin=use_nonlin,
        update=False, # Do not update the object list!
    )

    if not m or not mc:
        log(f"Photometric match failed for {filename}")
        save_objects(objname, None)
        return


    obj['zp'] = m['zero_fn'](obj['x'], obj['y'], mag=obj['mag'])
    obj['zp_err'] = m['zero_fn'](obj['x'], obj['y'], mag=obj['mag'], get_err=True)
    obj['mag_calib_err'] = np.hypot(obj['magerr'], obj['zp_err'])

    obj['mag_calib_color'] = obj['mag'] + mc['zero_fn'](obj['x'], obj['y'], mag=obj['mag'])


    # obj['time'] = time
    # obj['mjd'] = time.mjd
    # obj['filename'] = r['filename']
    obj.meta['filter_name'] = effective_fname
    obj.meta['color_term'] = mc['color_term']

    obj.meta['nstars_initial'] = np.sum(m['idx0'])
    obj.meta['nstars'] = np.sum(m['idx'])
    obj.meta['final_frac'] = np.mean(m['idx'])

    obj.meta['nstars_color'] = np.sum(mc['idx'])
    obj.meta['final_frac_color'] = np.mean(mc['idx'])

    resid = m['zero'] - m['zero_model']
    obj.meta['std'] = np.std(resid[m['idx']])

    resid_color = mc['zero'] - mc['zero_model']
    obj.meta['std_color'] = np.std(resid_color[mc['idx']])

    obj.meta['mag_limit'] = pipeline.get_detection_limit(obj, sn=5, verbose=verbose)

    # obj['resid_mad'] = mad_std(resid[m['idx']])
    # obj['norm_resid_mad'] = mad_std(resid[m['idx']]/np.max(m['zero_err']))

    obj.meta['filename'] = filename
    obj.meta['mjd'] = time.mjd
    obj.meta['time'] = time
    obj.meta['filter'] = fname
    obj.meta['cat_filter'] = effective_fname
    obj.meta['cat_filter1'] = col_mag1
    obj.meta['cat_filter2'] = col_mag2
    obj.meta['site'] = site
    obj.meta['night'] = night
    obj.meta['ccd'] = ccd

    log(f"{filename}: {obj.meta['nstars']} good matches, std = {obj.meta['std']:.2f} limit = {obj.meta['mag_limit']}")

    # Store results
    try:
        os.makedirs(dirname)
    except:
        pass

    # Remove unserializable fields
    obj.meta.pop('fwhm_phot_model', None)

    save_objects(objname, obj)


if __name__ == '__main__':
    from optparse import OptionParser

    parser = OptionParser(usage="usage: %prog [options] arg")
    parser.add_option('-p', '--photodir', help='Base dir to store photometry', action='store', dest='photodir', type='str', default='photometry')
    parser.add_option('-t', '--tmpdir', help='Temporary dir', action='store', dest='tmpdir', type='str', default=None)
    parser.add_option('-r', '--replace', help='Replace already existing records in database', action='store_true', dest='replace', default=False)
    parser.add_option('-v', '--verbose', help='Verbose', action='store_true', dest='verbose', default=False)
    parser.add_option('-D', '--dark', help='Master dark frame', action='store', dest='dark', type='str', default=None)
    parser.add_option('-F', '--flat', help='Master flat frame', action='store', dest='flat', type='str', default=None)
    parser.add_option('-C', '--cat', help='Reference catalogue', action='store', dest='cat', type='str', default=None)

    (options,files) = parser.parse_args()

    for i,filename in enumerate(files):
        if len(files) > 1 and options.verbose:
            print(i, '/', len(files), filename)
        try:
            process_file(
                filename,
                darkname=options.dark, flatname=options.flat,
                catname=options.cat,
                verbose=options.verbose, replace=options.replace,
                base=options.photodir, _tmpdir=options.tmpdir
            )
        except KeyboardInterrupt:
            raise
        except:
            print('\nException while processing:', filename, file=sys.stderr)
            import traceback
            traceback.print_exc()
            # raise

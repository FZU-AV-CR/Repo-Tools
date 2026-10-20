from __future__ import absolute_import, division, print_function, unicode_literals

import os

import numpy as np

import datetime

from .db import DB
from .calibrate import get_cropped_shape

basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

__all__ = [
    'Fram',
    'parse_iso_time',
    'get_iso_time',
    'get_night',
    'get_night_time',
]


def fix_fram_path(filename):
    if filename is None or os.path.isabs(filename):
        return filename

    return os.path.normpath(os.path.join(basedir, filename))


class Fram(DB):
    def __init__(self, latitude=-35.4959, longitude=-69.4497, elevation=1430, **kwargs):
        if 'dbname' not in kwargs:
            kwargs['dbname'] = 'fram'

        DB.__init__(self, **kwargs)

        self.latitude = latitude
        self.longitude = longitude

    def find_image(self, type='masterdark', night=None, site=None, ccd=None, serial=None, exposure=None, cropped_width=None, cropped_height=None, filter=None, header=None, debug=False, full=False, fix_path=False):
        '''Find the calibration image of given type not later than given night
        and suitable for a given chip and filter'''
        where = []
        opts = []

        # Guess some parameters from header
        if header is not None:
            if night is None:
                night = get_night(parse_iso_time(header.get('DATE-OBS')), lon=header.get('LONGITUD'), site=site)

            if ccd is None:
                ccd = header.get('CCD_NAME')

            if serial is None:
                serial = header.get('product_id')

            if cropped_width is None and cropped_height is None:
                cropped_height,cropped_width = get_cropped_shape(header=header)

            if filter is None:
                filter = header.get('FILTER')

            if exposure is None:
                exposure = header.get('EXPOSURE')

        # Construct the query
        if type is not None:
            where.append('type=%s')
            opts.append(type)

        if ccd is not None:
            where.append('ccd=%s')
            opts.append(ccd)

        if serial is not None:
            where.append('serial=%s')
            opts.append(int(serial))

        if exposure is not None and type not in ['bias', 'dcurrent', 'masterflat']:
            where.append('exposure=%s')
            opts.append(float(exposure))

        if filter is not None and type in ['masterflat']:
            where.append('filter=%s')
            opts.append(filter)

        if cropped_width is not None:
            where.append('cropped_width=%s')
            opts.append(int(cropped_width))

        if cropped_height is not None:
            where.append('cropped_height=%s')
            opts.append(int(cropped_height))

        if night is not None:
            where.append('night<=%s')
            opts.append(night)

        where_string = " AND ".join(where)
        if where_string:
            where_string = "WHERE " + where_string

        res = self.query("SELECT * FROM calibrations " + where_string + " ORDER BY night DESC LIMIT 1;", opts, simplify=False)#, debug=debug)
        if not len(res):
            if debug:
                print("Can't find preceding frame, looking for succeeding one")
            where_string = where_string.replace('night<=', 'night>=')
            res = self.query("SELECT * FROM calibrations " + where_string + " ORDER BY night ASC LIMIT 1;", opts, simplify=False)#, debug=debug)

        if full:
            if not len(res):
                return None

            row = res[0]
            if fix_path:
                row['filename'] = fix_fram_path(row['filename'])

            return row

        filename = res[0]['filename'] if len(res) else None
        if fix_path:
            filename = fix_fram_path(filename)
        if debug:
            print('find_image:', filename)
        return filename

    # Convenience
    def parse_iso_time(self, string=None, header=None):
        return parse_iso_time(string, header)

    def get_iso_time(self, time):
        return get_iso_time(time)

    def get_night(self, time, lon=None, site=None):
        if lon is None and site is None:
            lon = self.longitude

        return get_night(time, lon, site)

    def get_night_time(self, night, lon=None, site=None):
        if lon is None and site is None:
            lon = self.longitude

        return get_night_time(night, lon, site)


def parse_iso_time(string=None, header=None):
    if string is None and header is not None:
        string = header['DATE-OBS']

    return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%S.%f')


def get_iso_time(time):
    return time.strftime('%Y-%m-%dT%H:%M:%S.%f')


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


def get_night_time(night, lon=None, site=None):
    '''Get UTC time (given as datetime object) corresponding to the RTS2 night name.'''
    if lon is None:
        if site == 'auger':
            lon = -69.4497
        elif site == 'cta-n':
            lon = -17.89
        elif site == 'cta-s0' or site == 'cta-s1':
            lon = -70.32482
        else:
            lon = 0

    time = datetime.datetime.strptime(night, '%Y%m%d')
    time -= datetime.timedelta(seconds=lon*86400/360 - 86400/2)

    return time

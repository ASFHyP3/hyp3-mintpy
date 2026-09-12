import pandas as pd
import pytest

from hyp3_mintpy import gps


def test_find_reference_station():
    stations = ['OKFG', 'OKCE', '00NA']
    lons_gps = [192.0884-360, 191.8338-360, 130.8440]
    lats_gps = [53.4107, 53.4262, -12.4666]
    ref_sta, ref_lon, ref_lat = gps.find_reference_station('data/avgSpatialCoh.h5', stations, lons_gps, lats_gps)
    assert ref_sta == 'OKFG'


def test_get_vel():
    df = pd.read_pickle('data/test.pkl')
    vel = gps.get_vel(df)
    assert float(vel['east']['m']) == -0.005291043406543734

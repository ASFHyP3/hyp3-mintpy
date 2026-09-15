from pathlib import Path

import pandas as pd

from hyp3_mintpy import gps


def test_find_reference_station():
    stations = ['OKFG', 'OKCE']
    lons_gps = [192.0884 - 360, 191.8338 - 360]
    lats_gps = [53.4107, 53.4262]
    current_dir = Path(__file__).parent.resolve()
    coherence = current_dir / 'data/avgSpatialCoh.h5'
    ref_sta, _, _ = gps.find_reference_station(str(coherence), stations, lons_gps, lats_gps)
    assert ref_sta == 'OKFG'


def test_get_vel():
    current_dir = Path(__file__).parent.resolve()
    test = current_dir / 'data/test_gps.pkl'
    df = pd.read_pickle(test)
    vel = gps.get_vel(df)
    assert round(float(vel['east']['m']), 6) == round(-0.005291043406543734, 6)

import subprocess
from pathlib import Path

import geopandas as gpd
import opensarlab_lib as osl
import pytest

from hyp3_mintpy import util
from hyp3_mintpy.process import check_extent, check_product, rename_products, set_same_epsg, set_same_frame, write_cfg


def test_rename_products_new():
    Path('test/S1_000_000000s0n00-000000s0n00-000000s0n00_IW_00000000_00000000_VV_INT80_0000').mkdir(parents=True)
    with Path(
        'test/S1_000_000000s0n00-000000s0n00-000000s0n00_IW_00000000_00000000_VV_INT80_0000/S1_000_000000s0n00-000000s0n00-000000s0n00_IW_00000000_00000000_VV_INT80_0000.txt'
    ).open('w') as test:
        test.write('S1_000000_IW1_00000000T000000_VV_AAAA-BURST')

    rename_products('test')
    folder = Path('test/S1_000000_IW1_00000000_00000000_VV_INT80_0000')
    txt = folder / 'S1_000000_IW1_00000000_00000000_VV_INT80_0000.txt'

    assert folder.is_dir()
    assert txt.exists()

    subprocess.call('rm -rf test', shell=True)


def test_rename_products_old():
    Path('test/S1A_000_W000_0_N00_0_E000_0_N00_0_00000000_00000000_VV_INT80_0000').mkdir(parents=True)
    with Path(
        'test/S1A_000_W000_0_N00_0_E000_0_N00_0_00000000_00000000_VV_INT80_0000/S1A_000_W000_0_N00_0_E000_0_N00_0_00000000_00000000_VV_INT80_0000.txt'
    ).open('w') as test:
        test.write('S1_000000_IW1_00000000T000000_VV_AAAA-BURST')

    rename_products('test')
    folder = Path('test/S1_000000_IW1_00000000_00000000_VV_INT80_0000')
    txt = folder / 'S1_000000_IW1_00000000_00000000_VV_INT80_0000.txt'

    assert folder.is_dir()
    assert txt.exists()

    subprocess.call('rm -rf test', shell=True)


def test_set_same_epsg(test_data_directory):
    tiff_path = list(test_data_directory.glob('*.tif'))
    gdf = gpd.GeoDataFrame(
        {
            'tiff_path': tiff_path,
            'EPSG': [util.get_epsg(p) for p in tiff_path],
            'geometry': [util.get_geotiff_bbox(p) for p in tiff_path],
        }
    )
    gdf = set_same_epsg(gdf)
    assert gdf['EPSG'].nunique() == 1


def test_check_extent(test_data_directory):
    tiff_path = list(test_data_directory.glob('test_*.tif'))
    gdf = gpd.GeoDataFrame(
        {
            'tiff_path': tiff_path,
            'EPSG': [util.get_epsg(p) for p in tiff_path],
            'geometry': [util.get_geotiff_bbox(p) for p in tiff_path],
        }
    )
    with pytest.raises(Exception, match='Error determining area of common coverage'):
        check_extent(gdf, [0, 0, 1, 1])

    assert check_extent(gdf, [670000.0, 5900000.0, 840000.0, 5950000.0]) is None  # type: ignore


def test_set_same_frame(test_data_directory):
    data = test_data_directory

    Path('test/test').mkdir(parents=True)
    sdata = str(data)
    test = Path('test/test')
    stest = str(test)
    subprocess.call(f'cp {sdata}/test_*.tif {stest}/', shell=True)

    set_same_frame('test', wgs84=True)

    extent_unw = osl.get_common_coverage_extents([test / 'test_unw_phase.tif'])
    extent_mask = osl.get_common_coverage_extents([test / 'test_water_mask.tif'])

    assert extent_unw == extent_mask

    epsg_unw = util.get_epsg(str(test / 'test_unw_phase.tif'))
    epsg_mask = util.get_epsg(str(test / 'test_water_mask.tif'))

    assert epsg_unw == epsg_mask

    subprocess.call('rm -rf test', shell=True)


def test_write_cfg():
    job_name = 'test_job'
    min_coherence = '0.5'
    write_cfg(job_name, min_coherence)

    assert Path(f'{job_name}/MintPy/{job_name}.txt').exists()

    with Path(f'{job_name}/MintPy/{job_name}.txt').open() as cfg:
        lines = cfg.readlines()

    minCoh = 0.0
    for line in lines:
        if 'minCoherence' in line:
            minCoh = float(line.split('=')[1])

    assert minCoh == float(min_coherence)

    subprocess.call(f'rm -rf {job_name}', shell=True)


def test_check_product():
    filename = 'S1_064_000000s1n00-136231s2n02-000000s3n00_IW_20200604_20200616_VV_INT80_0000.zip'

    assert check_product(filename, None, None)
    assert not check_product(filename, '2021-01-01', None)
    assert check_product(filename, '2019-01-01', None)
    assert check_product(filename, None, '2021-01-01')
    assert not check_product(filename, None, '2019-01-01')
    assert check_product(filename, '2019-01-01', '2021-01-01')
    assert not check_product(filename, '2019-01-01', '2020-06-10')
    assert not check_product(filename, '2020-06-10', '2021-01-01')

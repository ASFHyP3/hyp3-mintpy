"""mintpy processing."""

import datetime as dt
import logging
import os
import shutil
import subprocess
import warnings
from pathlib import Path

import boto3
import botocore
import geopandas as gpd
import hyp3_sdk as sdk
import opensarlab_lib as osl
import shapely.wkt
from osgeo import gdal
from tqdm.auto import tqdm

import hyp3_mintpy
from hyp3_mintpy import util


log = logging.getLogger(__name__)


def rename_products(folder: str) -> None:
    """Rename downloaded products to make them compatible with MintPy.

    Args:
        folder: Path for the folder that has the downloaded products.
    """
    cwd = Path.cwd()
    os.chdir(folder)
    folders = list(Path('./').glob('*'))
    folders = [fol for fol in folders if Path(fol).is_dir()]
    for fol in folders:
        new = True
        if str(fol).count('_') > 8:
            new = False
        os.chdir(str(fol))
        fs = list(Path('./').glob('*'))
        txts = [t for t in fs if '.txt' in str(t) and 'README' not in str(t)]
        ar = txts[0].open()
        lines = ar.readlines()
        ar.close()
        burst = lines[0].split('_')[1] + '_' + lines[0].split('_')[2]
        for f in fs:
            name = f.name
            if new:
                newname = 'S1_' + burst + '_' + '_'.join([n for n in name.split('_')[4:]])
            else:
                newname = 'S1_' + burst + '_' + '_'.join([n for n in name.split('_')[10:]])
            if '.txt' in newname and 'README' not in newname:
                foldername = newname.split('.')[0]
            subprocess.call('mv ' + name + ' ' + newname, shell=True)
        os.chdir(cwd)
        os.chdir(folder)
        subprocess.call('mv ' + fol.name + ' ' + foldername, shell=True)
    os.chdir(cwd)


def download_job_pairs(
    job_name: str, start: str | None = None, end: str | None = None, folder: str | None = None
) -> str:
    """Downloads HyP3 products and renames files to meet MintPy standards.

    Args:
        job_name: Name of the HyP3 project.
        start: Start date for the timeseries if one of the product dates is before this, it won't be downloaded.
        end: End date for the timeseries if one of the product dates is after this, it won't be downloaded.
        folder: Folder name that will contain the downloaded products. If None it will create a folder with the project name.
    """
    hyp3 = sdk.HyP3()
    jobs = hyp3.find_jobs(name=job_name)

    if folder is None:
        folder = job_name
    if not Path(folder).is_dir():
        Path.mkdir(Path(folder))

    file_list = jobs.download_files(Path(folder))
    for z in file_list:
        if check_product(z.name, start, end):
            shutil.unpack_archive(str(z), folder)
        z.unlink()

    rename_products(folder)

    return folder


def download_bucket_pairs(
    key: str | None = None,
    start: str | None = None,
    end: str | None = None,
    path: str = 'multiburst_products/',
    bucket: str = 'volcsarvatory-data-test',
) -> str:
    """Downloads multiburst products from bucket and renames files to meet MintPy standards.

    Args:
        key: Folder name that contains the multiburst product.
        start: Start date for the timeseries if one of the product dates is before this, it won't be downloaded.
        end: End date for the timeseries if one of the product dates is after this, it won't be downloaded.
        path: Additional prefix to the products.
        bucket: Name of the bucket.
    """
    s3 = boto3.resource('s3', config=boto3.session.Config(signature_version=botocore.UNSIGNED))
    buck = s3.Bucket(bucket)
    folder = str(key).split('/')[-1]
    Path.mkdir(Path(folder))
    for s3_object in tqdm(buck.objects.filter(Prefix=f'{path}{key}')):
        path, filename = os.path.split(s3_object.key)
        if check_product(filename, start, end):
            buck.download_file(s3_object.key, f'{folder}/{filename}')
            z = Path(f'{folder}/{filename}')
            shutil.unpack_archive(str(z), folder)
            z.unlink()
    rename_products(folder)

    return folder


def check_product(filename: str, start: str | None = None, end: str | None = None) -> bool:
    """Check if products are within a given time interval.

    Args:
        filename: Product name.
        start: Start date for the timeseries if one of the product dates is before this, it won't be downloaded.
        end: End date for the timeseries if one of the product dates is after this, it won't be downloaded.
    """
    date1 = dt.datetime.strptime(filename.split('_')[4], '%Y%m%d')
    date2 = dt.datetime.strptime(filename.split('_')[5], '%Y%m%d')
    cond1 = True
    cond2 = True
    if start is not None:
        start_date = dt.datetime.strptime(start, '%Y-%m-%d')
        cond1 = date1 >= start_date and date2 >= start_date

    if end is not None:
        end_date = dt.datetime.strptime(end, '%Y-%m-%d')
        cond2 = date1 <= end_date and date2 <= end_date

    return cond1 and cond2


def set_same_epsg(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Checks if the EPSG is the same to all files if not it reprojects them.

    Args:
        gdf: Geopandas dataframe with all the tiff files.

    Returns:
        Geopandas dataframe with reprojected files.
    """
    proj_count = gdf['EPSG'].value_counts()
    predominant_epsg = proj_count.idxmax()
    print(f'reprojecting to predominant EPSG: {predominant_epsg}')
    tiff_path = gdf['tiff_path'].tolist()
    for _, row in gdf.loc[gdf['EPSG'] != predominant_epsg].iterrows():
        pth = row['tiff_path']
        no_data_val = util.get_no_data_val(pth)
        res = util.get_res(pth)

        temp = pth.parent / f'temp_{pth.stem}.tif'
        pth.rename(temp)
        src_epsg = row['EPSG']

        warp_options = {
            'dstSRS': f'EPSG:{predominant_epsg}',
            'srcSRS': f'EPSG:{src_epsg}',
            'targetAlignedPixels': True,
            'xRes': res,
            'yRes': res,
            'dstNodata': no_data_val,
        }
        gdal.Warp(str(pth), str(temp), **warp_options)
        temp.unlink()

    gdf = gpd.GeoDataFrame(
        {
            'tiff_path': tiff_path,
            'EPSG': [util.get_epsg(p) for p in tiff_path],
            'geometry': [util.get_geotiff_bbox(p) for p in tiff_path],
        }
    )

    return gdf


def check_extent(gdf: gpd.GeoDataFrame, common_extents: list) -> None:
    """Checks the geometries in the GeoDataFrame are within the common_extents.

    Args:
        gdf: Geopandas dataframe with all the tiff files.
        common_extents: List with the common extent coordinates.
    """
    wkt = (
        f'POLYGON(({common_extents[0]} {common_extents[1]}, {common_extents[2]} {common_extents[1]}, {common_extents[2]} '
        f'{common_extents[3]}, {common_extents[0]} {common_extents[3]}, {common_extents[0]} {common_extents[1]}))'
    )

    wkt_shapely_geom = shapely.wkt.loads(wkt)
    if not util.check_within_bounds(wkt_shapely_geom, gdf):
        print('WKT exceeds bounds of at least one dataset')
        raise Exception('Error determining area of common coverage')


def set_same_frame(folder: str, wgs84: bool = False) -> None:
    """Checks the coordinate system for all the files in the folder and reprojects them if necessary.

    Args:
        folder: Path to the folder that has the HyP3 products.
        wgs84: If True reprojects all the files to WGS84 system.
    """
    data_path = Path(folder)
    dem = sorted(list(data_path.glob('*/*dem*.tif')))
    lv_phi = sorted(list(data_path.glob('*/*lv_phi*.tif')))
    lv_theta = sorted(list(data_path.glob('*/*lv_theta*.tif')))
    water_mask = sorted(list(data_path.glob('*/*_water_mask*.tif')))
    unw = sorted(list(data_path.glob('*/*_unw_phase*.tif')))
    corr = sorted(list(data_path.glob('*/*_corr*.tif')))
    conn_comp = sorted(list(data_path.glob('*/*_conncomp*.tif')))
    tiff_path = dem + lv_phi + lv_theta + water_mask + unw + corr + conn_comp

    gdf = gpd.GeoDataFrame(
        {
            'tiff_path': tiff_path,
            'EPSG': [util.get_epsg(p) for p in tiff_path],
            'geometry': [util.get_geotiff_bbox(p) for p in tiff_path],
        }
    )

    # check for multiple projections and project to the predominant EPSG
    if gdf['EPSG'].nunique() > 1:
        gdf = set_same_epsg(gdf)

    # check the file extent is within the common extent
    common_extents = osl.get_common_coverage_extents(unw)
    check_extent(gdf, common_extents)

    # reprojects all files to the common extent
    for pth in tqdm(gdf['tiff_path']):
        print(f'Subsetting: {pth}')
        temp_pth = pth.parent / f'subset_{pth.name}'
        gdal.Translate(
            destName=str(temp_pth),
            srcDS=str(pth),
            projWin=[common_extents[0], common_extents[3], common_extents[2], common_extents[1]],
        )
        pth.unlink()
        temp_pth.rename(pth)

    # reprojects all files to WGS84 if necessary
    if wgs84:
        for pth in tqdm(gdf['tiff_path']):
            print(f'Converting {pth} to WGS84')
            gdal.Warp(str(pth), str(pth), dstSRS='EPSG:4326')


def write_cfg(output_name: str, min_coherence: str) -> None:
    """Creates a basic config file from a template.

    Args:
        output_name: Name of the HyP3 project.
        min_coherence: Minimum coherence for timeseries processing.
    """
    cfg_folder = Path(hyp3_mintpy.__file__).parent / 'schemas'

    with Path(f'{cfg_folder}/config.txt').open() as cfg:
        lines = cfg.readlines()

    abspath = Path(output_name).resolve()
    Path(f'{output_name}/MintPy').mkdir(parents=True)
    with Path(f'{output_name}/MintPy/{output_name}.txt').open('w') as cfg:
        for line in lines:
            newstring = ''
            if 'folder' in line:
                newstring += line.replace('folder', str(abspath))
            elif 'min_coherence' in line:
                newstring += line.replace('min_coherence', min_coherence)
            else:
                newstring = line
            cfg.write(newstring)


def run_mintpy(output_name: str) -> Path:
    """Calls mintpy and prepares a zip file with the outputs.

    Args:
        output_name: Name of the HyP3 project.

    Returns:
        Path for the output zip file.
    """
    subprocess.call(
        f'smallbaselineApp.py {output_name}/MintPy/{output_name}.txt --work-dir {output_name}/MintPy', shell=True
    )
    subprocess.call(f'mv {output_name}/MintPy/*.h5 {output_name}/', shell=True)
    subprocess.call(f'mv {output_name}/MintPy/inputs/geometry*.h5 {output_name}/', shell=True)
    subprocess.call(f'mv {output_name}/MintPy/*.txt {output_name}/', shell=True)
    subprocess.call(f'rm -rf {output_name}/MintPy {output_name}/S1_* {output_name}/shape_*', shell=True)
    output_zip = shutil.make_archive(base_name=output_name, format='zip', base_dir=output_name)

    return Path(output_zip)


def process_mintpy(
    job_name: str | None, prefix: str | None, min_coherence: float, start: str | None = None, end: str | None = None
) -> Path:
    """Create a greeting product.

    Args:
        job_name: Name of the HyP3 project.
        prefix: Folder that contains multiburst products.
        min_coherence: Minimum coherence for timeseries processing.
        start: Start date for the timeseries
        end: End date for the timeseries

    Returns:
        Path for the output zip file.
    """
    if job_name is None and prefix is None:
        raise ValueError('You should give a job name or a bucket to pull the data from')
    elif job_name is not None and prefix is not None:
        warnings.warn('Both job name and prefix were given. You should give just one. Using job name...')

    if job_name is not None:
        output_name = download_job_pairs(job_name, start, end)
    else:
        output_name = download_bucket_pairs(prefix, start, end)
    set_same_frame(output_name, wgs84=True)

    write_cfg(output_name, str(min_coherence))

    product_file = run_mintpy(output_name)
    return product_file

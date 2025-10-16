"""mintpy processing."""

import glob
import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import opensarlab_lib as osl
import hyp3_mintpy
import hyp3_sdk as sdk
import shapely.wkt
from hyp3_mintpy import util
from osgeo import gdal, ogr
from rasterio.warp import transform_bounds
from tqdm.auto import tqdm

log = logging.getLogger(__name__)


def download_pairs(job_name, folder = None):
    """
    Downloads HyP3 products and renames files to meet MintPy standards
    
    Args:
        project_name: Name of the HyP3 project.
        hyp3: Instance of HyP3 where the user has been logged in.
        folder: Folder name that will contain the downloaded products. If None it will create a folder with the project name.
    """
    hyp3 = sdk.HyP3()
    jobs = hyp3.find_jobs(name=job_name)

    cwd = os.getcwd()
    if folder is None:
        folder = job_name
    if not os.path.isdir(folder):
        os.mkdir(folder)
    folder = Path(folder)
    file_list = jobs.download_files(folder)
    for z in file_list:
        shutil.unpack_archive(str(z), str(folder))
        z.unlink()

    os.chdir(str(folder))
    folders=glob.glob('./*')
    folders=[fol for fol in folders if os.path.isdir(fol)]

    for fol in folders:
        os.chdir(fol)
        fs=glob.glob('./*')
        txts=[t for t in fs if '.txt' in t and 'README' not in t]
        ar=open(txts[0])
        lines=ar.readlines()
        ar.close()
        burst=lines[0].split('_')[1]+'_'+lines[0].split('_')[2]
        for f in fs:
            name=os.path.basename(f)
            newname='S1_'+burst+'_'+'_'.join([n for n in name.split('_')[10::]])
            if '.txt' in newname and 'README' not in newname:
                foldername=newname.split('.')[0]
            subprocess.call('mv '+name+' '+newname,shell=True)
        os.chdir(cwd)
        os.chdir(str(folder))
        subprocess.call('mv '+os.path.basename(fol)+' '+foldername,shell=True)
    os.chdir(cwd)


def set_same_frame(folder, wgs84 = False):
    """
    Checks the coordinate system for all the files in the folder and reprojects them if necessary
    
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
        proj_count = gdf['EPSG'].value_counts()
        predominant_epsg = proj_count.idxmax()
        print(f'reprojecting to predominant EPSG: {predominant_epsg}')
        for _, row in gdf.loc[gdf['EPSG'] != predominant_epsg].iterrows():
            pth = row['tiff_path']
            no_data_val = util.get_no_data_val(pth)
            res = util.get_res(pth)
        
            temp = pth.parent/f"temp_{pth.stem}.tif"
            pth.rename(temp)
            src_epsg = row['EPSG']

            warp_options = {
                "dstSRS":f"EPSG:{predominant_epsg}", "srcSRS":f"EPSG:{src_epsg}",
                "targetAlignedPixels":True,
                "xRes":res, "yRes":res,
                "dstNodata": no_data_val
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
    common_extents = osl.get_common_coverage_extents(unw)
    xmin, ymin, xmax, ymax = transform_bounds(int(osl.get_projection(str(unw[0]))), 3857, *common_extents)
    common_extents_3857 = [xmin, ymin, xmax, ymax]
    print(common_extents)
    correct_wkt_input = False
    while not correct_wkt_input:
        epsg = int(gdf.iloc[0]['EPSG'])
        wkt = (f'POLYGON(({common_extents[0]} {common_extents[1]}, {common_extents[2]} {common_extents[1]}, {common_extents[2]} '
               f'{common_extents[3]}, {common_extents[0]} {common_extents[3]}, {common_extents[0]} {common_extents[1]}))')
        print(wkt)
        wkt_shapely_geom = shapely.wkt.loads(wkt)
        wkt_ogr_geom = ogr.CreateGeometryFromWkt(wkt)
        if not util.check_within_bounds(wkt_shapely_geom, gdf):
            print('WKT exceeds bounds of at least one dataset')
            raise Exception('Error determining area of common coverage')

        correct_wkt_input = True

    shp_path = data_path / f'shape_{datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")}.shp'
    util.save_shapefile(wkt_ogr_geom, epsg, shp_path)
    for pth in tqdm(gdf['tiff_path']):
        print(f'Subsetting: {pth}')
        temp_pth = pth.parent/f'subset_{pth.name}'
        gdal.Translate(destName=str(temp_pth), srcDS=str(pth), projWin=[common_extents[0], common_extents[3], common_extents[2], common_extents[1]])
        pth.unlink()
        temp_pth.rename(pth)

    if wgs84:
        for pth in tqdm(gdf['tiff_path']):
            print(f'Converting {pth} to WGS84')
            gdal.Warp(str(pth), str(pth), dstSRS='EPSG:4326')


def write_cfg(job_name: str, min_coherence: str):
    cfg_folder = os.path.dirname(hyp3_mintpy.__file__) + '/schemas'

    with open(f'{cfg_folder}/config.txt') as cfg:
        lines = cfg.readlines()

    abspath = os.path.abspath(job_name)
    os.makedirs(f"{job_name}/MintPy", exist_ok=True)
    with open(f"{job_name}/MintPy/{job_name}.txt", 'w') as cfg:
        for line in lines:
            newstring = ''
            if 'folder' in line:
                newstring += line.replace('folder', abspath)
            elif 'min_coherence' in line:
                newstring += line.replace('min_coherence', min_coherence)
            else:
                newstring = line
            cfg.write(newstring)


def run_mintpy(job_name):
    subprocess.call(f"smallbaselineApp.py {job_name}/MintPy/{job_name}.txt --work-dir {job_name}/MintPy", shell=True)
    subprocess.call(f"mv {job_name}/MintPy/*.h5 {job_name}/", shell=True)
    subprocess.call(f"mv {job_name}/MintPy/inputs/geometry*.h5 {job_name}/", shell=True)
    subprocess.call(f"mv {job_name}/MintPy/*.txt {job_name}/", shell=True)
    subprocess.call(f"rm -rf {job_name}/MintPy {job_name}/S1_* {job_name}/shape_*", shell=True)
    output_zip = shutil.make_archive(base_name=job_name, format='zip', base_dir=job_name)

    return Path(output_zip)


def process_mintpy(job_name: str, min_coherence: float) -> Path:
    """Create a greeting product.

    Args:
        greeting: Write this greeting to a product file (Default: "Hello world!" )
    """
    download_pairs(job_name)
    set_same_frame(job_name)

    write_cfg(job_name, str(min_coherence))

    product_file = run_mintpy(job_name)
    return product_file

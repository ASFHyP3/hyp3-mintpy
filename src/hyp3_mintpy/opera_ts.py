"""OPERA download."""

import logging
import os
import shutil
import warnings
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path

import earthaccess
import numpy as np
import rioxarray as rxr
import xarray as xr
from fsspec.spec import AbstractFileSystem
from hyp3lib.aws import upload_file_to_s3
from hyp3lib.fetch import write_credentials_to_netrc_file
from pyproj import Transformer

from hyp3_mintpy import opera2mintpy
from hyp3_mintpy.util import nullable_string, upload_file_to_s3_with_publish_access_keys


def open_remote_disp(fs: AbstractFileSystem, url: str, y_slice: slice, x_slice: slice) -> xr.Dataset:
    """Stream displacement OPERA product.

    Args:
        fs: Filesystem from earthaccess.
        url: URL for netcdf.
        y_slice: Limits in the y-axis in UTM coordinates.
        x_slice: Limits in the x-axis in UTM coordinates.

    Returns:
        xarray dataset
    """
    file_object = fs.open(url)
    ds = xr.open_dataset(file_object, group='/', engine='h5netcdf')[
        ['short_wavelength_displacement', 'displacement', 'temporal_coherence', 'spatial_ref']
    ].sel(y=y_slice, x=x_slice)

    return ds


def get_pass_dir(fs: AbstractFileSystem, url: str) -> str:
    """Stream displacement OPERA product.

    Args:
        fs: Filesystem from earthaccess.
        url: URL for netcdf.

    Returns:
        Pass direction
    """
    file_object = fs.open(url)
    direction = xr.open_dataset(file_object, group='/identification', engine='h5netcdf')[
        ['orbit_pass_direction', 'track_number']
    ]

    return direction


def get_perp_baseline(fs: AbstractFileSystem, url: str, y_slice: slice, x_slice: slice) -> xr.Dataset:
    """Stream displacement OPERA product.

    Args:
        fs: Filesystem from earthaccess.
        url: URL for netcdf.
        y_slice: Limits in the y-axis in UTM coordinates.
        x_slice: Limits in the x-axis in UTM coordinates.

    Returns:
        Perpendicular baseline
    """
    file_object = fs.open(url)
    bperp = xr.open_dataset(file_object, group='/corrections', engine='h5netcdf')[
        ['perpendicular_baseline']
    ].sel(y=y_slice, x=x_slice)

    return bperp


def download_los(aoi: list[float], name: str, frame: str | None = None) -> tuple[list[str], list[float]]:
    """Downloads and crop OPERA displacement line of sight.

    Args:
        aoi: Bounding box with area of interest.
        name: Name of the product.
        frame: OPERA frame.

    Returns:
        Frame IDs and AOI in UTM coordinates.
    """
    if frame is None:
        results_static = earthaccess.search_data(short_name='OPERA_L3_DISP-S1-STATIC_V1', bounding_box=tuple(aoi))
    else:
        results_static = earthaccess.search_data(short_name='OPERA_L3_DISP-S1-STATIC_V1', bounding_box=tuple(aoi), granule_name=f'*{frame}*')
    _ = earthaccess.login()
    urls = []
    for result in results_static:
        urls += [
            u.replace('datapool.asf.alaska.edu', 'cumulus.asf.earthdatacloud.nasa.gov')
            for u in result.data_links(access='external')
            if '_line_of_sight_' in u or '_dem' in u or '_layover_shadow_mask' in u
        ]
    urls = [u for u in urls if '_line_of_sight_enu' in u or '_dem' in u or '_layover_shadow_mask' in u]

    tif_dir = Path(f'./OPERA_{name}')
    tif_dir.mkdir(parents=True, exist_ok=True)
    earthaccess.download(urls, local_path=f'./OPERA_{name}')
    tifs = list(tif_dir.glob('*.tif'))
    for tif in tifs:
        ds = rxr.open_rasterio(tif)
        ds_crs = ds.rio.crs  # type: ignore

        transformer = Transformer.from_crs('EPSG:4326', ds_crs, always_xy=True)

        min_x, min_y = transformer.transform(aoi[0], aoi[1])
        max_x, max_y = transformer.transform(aoi[2], aoi[3])

        y_slice = slice(max_y, min_y)
        x_slice = slice(min_x, max_x)
        ds_clip = ds.sel(y=y_slice, x=x_slice)

        ds_clip.rio.to_raster(str(tif).replace('.tif', '_clip.tif'))
        tif.unlink()

    frames = list({str(tif.name).split('_')[3] for tif in tifs})

    return frames, [min_x, max_x, min_y, max_y]


def get_dates(sdate: str) -> tuple[datetime, datetime]:
    """Get start and end date from a netcdf file path.

    Args:
        sdate: Path to netcdf.

    Returns:
        Start and end dates.
    """
    return datetime.strptime(sdate.split('/')[-1].split('_')[6],'%Y%m%dT%H%M%SZ'), datetime.strptime(sdate.split('/')[-1].split('_')[7],'%Y%m%dT%H%M%SZ')


def download_ts(aoi: list[float], name: str, start_time: datetime | None = None, end_time: datetime | None = None, frame: str | None = None) -> Path:
    """Downloads OPERA displacement time series for an AOI.

    Args:
        aoi: Bounding box with area of interest.
        name: Name of the HyP3 project.
        start_time: Start time for the time series.
        end_time: End time for the time series.
        frame: OPERA frame.

    Returns:
        Path for the output zip file.
    """
    frames, bbox = download_los(aoi, name, frame = None)
    min_x, max_x, min_y, max_y = bbox
    print(aoi)
    print(start_time, end_time)
    if frame is None:
        results = earthaccess.search_data(short_name='OPERA_L3_DISP-S1_V1', bounding_box=tuple(aoi))
    else:
        frames = [f'F{frame}']
        results = earthaccess.search_data(short_name='OPERA_L3_DISP-S1_V1', bounding_box=tuple(aoi), granule_name=f'*{frame}*')

    urls = []
    for result in results:
        urls += [
            u.replace('datapool.asf.alaska.edu', 'cumulus.asf.earthdatacloud.nasa.gov')
            for u in result.data_links(access='external')
            if '.nc' in u
        ]

    fs = earthaccess.get_fsspec_https_session()

    y_slice = slice(max_y, min_y)
    x_slice = slice(min_x, max_x)
    print(min_x, max_x, min_y, max_y)
    print(urls)

    for framet in frames:
        if start_time is not None and end_time is not None:
            furls = sorted({u for u in urls if framet in u and get_dates(u)[1]<datetime.strptime(end_time,'%Y-%m-%d') and get_dates(u)[0]>datetime.strptime(start_time,'%Y-%m-%d')})
        else:
            furls = sorted({u for u in urls if framet in u})
        print(furls)
        pass_dir = get_pass_dir(fs, furls[0])

        references = sorted({furl.split('/')[-1].split('_')[6] for furl in furls})
        secondaries = sorted({furl.split('/')[-1].split('_')[7] for furl in furls})

        ds_frame_list = []
        dates = sorted(set(references + secondaries))
        next_frame = False
        for i, reference in enumerate(references):
            print('Frame', framet, reference)
            furls_ref = sorted({u for u in furls if reference==u.split('/')[-1].split('_')[6]})

            da_list = [open_remote_disp(fs, u, y_slice, x_slice) for u in furls_ref]
            bp_list = [get_perp_baseline(fs, u, y_slice, x_slice) for u in furls_ref]
            ds_ref = xr.concat(da_list, dim='time')
            bp_ref = xr.concat(bp_list, dim='time')
            ds_ref['perpendicular_baseline'] = bp_ref['perpendicular_baseline']
            if ds_ref.sizes['time'] == 0 or ds_ref.sizes['x'] == 0 or ds_ref.sizes['y'] == 0:
                next_frame = True
                break
            if i==0:
                shape = ds_ref['displacement'].shape
                cum_offset_disp = np.zeros((shape[1], shape[2]))
                cum_offset_short = np.zeros((shape[1], shape[2]))
                cum_bp = np.zeros((shape[1], shape[2]))

            if ds_ref.sizes['time'] > 0 and ds_ref.sizes['x'] > 0 and ds_ref.sizes['y'] > 0:
                ds_ref = ds_ref.compute()
                ds_ref['displacement'] += cum_offset_disp
                ds_ref['short_wavelength_displacement'] += cum_offset_short
                ds_ref['perpendicular_baseline'] += cum_bp
            cum_offset_disp += ds_ref['displacement'].values[-1,:,:]
            cum_offset_short += ds_ref['short_wavelength_displacement'].values[-1,:,:]
            cum_bp += bp_ref['perpendicular_baseline'].values[-1,:,:]
            ds_frame_list.append(ds_ref)
        if next_frame:
            continue
        ds_frame = xr.concat(ds_frame_list, dim='time')
        ds_frame.attrs['orbit_pass_direction'] = pass_dir['orbit_pass_direction'].item().decode("utf-8")
        ds_frame.attrs['track_number'] = pass_dir['track_number'].item()
        ds_frame.attrs['dates'] = dates
        ds_frame.to_netcdf(f'OPERA_{name}/OPERA_{framet}.h5', engine='h5netcdf')
        opera2mintpy.disp_to_mintpy(ds_frame, framet)

    output_zip = shutil.make_archive(base_name=f'OPERA_{name}', format='zip', base_dir=f'OPERA_{name}')

    return Path(output_zip)


def main() -> None:
    """HyP3 entrypoint for hyp3_mintpy."""
    parser = ArgumentParser()
    parser.add_argument('--bucket', help='AWS S3 bucket HyP3 for upload the final product(s)')
    parser.add_argument('--bucket-prefix', default='', help='Add a bucket prefix to product(s)')
    parser.add_argument(
        '--aoi',
        type=str.split,
        nargs='+',
        help='AOI in lat/lon: [min_lon, min_lat, max_lon, max_lat].',
    )
    parser.add_argument('--name', help='Name of the AOI')
    parser.add_argument('--frame', default=None, help='End date for timeseries')
    parser.add_argument('--start-time', default=None, help='Start date for timeseries')
    parser.add_argument('--end-time', default=None, help='End date for timeseries')
    parser.add_argument(
        '--publish-bucket',
        type=nullable_string,
        default=None,
        help='Additionally, publish products to this bucket. Necessary credentials must be provided '
        'via the `PUBLISH_ACCESS_KEY_ID` and `PUBLISH_SECRET_ACCESS_KEY` environment variables.',
    )
    parser.add_argument(
        '--publish-prefix',
        type=nullable_string,
        default=None,
        help='Prefix for the bucket where the products will be published',
    )

    args = parser.parse_args()

    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO
    )

    username = os.getenv('EARTHDATA_USERNAME')
    password = os.getenv('EARTHDATA_PASSWORD')
    if username and password:
        write_credentials_to_netrc_file(username, password, append=False)

    if not (Path.home() / '.netrc').exists():
        warnings.warn(
            'Earthdata credentials must be present as environment variables, or in your netrc.',
            UserWarning,
        )

    args.aoi = [float(item) for sublist in args.aoi for item in sublist]
    print(args.aoi)

    product_file = download_ts(args.aoi, args.name, args.start_time, args.end_time, args.frame)

    if args.bucket:
        upload_file_to_s3(product_file, args.bucket, args.bucket_prefix)

    if args.publish_bucket:
        if args.publish_prefix:
            prefix = args.publish_prefix
            s3_name = str(product_file)
        else:
            prefix = 'opera_ts/'
            s3_name = str(product_file)
        upload_file_to_s3_with_publish_access_keys(product_file, args.publish_bucket, prefix, s3_name)

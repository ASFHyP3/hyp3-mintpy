"""OPERA download."""

import logging
import os
import shutil
import warnings
from argparse import ArgumentParser
from pathlib import Path

import earthaccess
import rioxarray as rxr
import xarray as xr
from fsspec.spec import AbstractFileSystem
from hyp3lib.aws import upload_file_to_s3
from hyp3lib.fetch import write_credentials_to_netrc_file
from pyproj import Transformer

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
        ['short_wavelength_displacement', 'displacement', 'spatial_ref']
    ].sel(y=y_slice, x=x_slice)

    return ds


def download_los(aoi: list[float], name: str) -> tuple[list[str], list[float]]:
    """Downloads and crop OPERA displacement line of sight.

    Args:
        aoi: Bounding box with area of interest.
        name: Name of the product.

    Returns:
        Frame IDs and AOI in UTM coordinates.
    """
    results_static = earthaccess.search_data(short_name='OPERA_L3_DISP-S1-STATIC_V1', bounding_box=tuple(aoi))
    _ = earthaccess.login()
    urls = []
    for result in results_static:
        urls += [
            u.replace('datapool.asf.alaska.edu', 'cumulus.asf.earthdatacloud.nasa.gov')
            for u in result.data_links(access='external')
            if '_line_of_sight_' in u
        ]
    urls = [u for u in urls if '_line_of_sight_' in u]

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

        ds_clip = ds.rio.clip_box(minx=min_x, miny=min_y, maxx=max_x, maxy=max_y)  # type: ignore

        ds_clip.rio.to_raster(str(tif).replace('.tif', '_clip.tif'))
        tif.unlink()

    frames = list({str(tif.name).split('_')[3] for tif in tifs})

    return frames, [min_x, max_x, min_y, max_y]


def download_ts(aoi: list[float], name: str) -> Path:
    """Downloads OPERA displacement time series for an AOI.

    Args:
        aoi: Bounding box with area of interest.
        name: Name of the HyP3 project.

    Returns:
        Path for the output zip file.
    """
    frames, bbox = download_los(aoi, name)
    min_x, max_x, min_y, max_y = bbox

    results = earthaccess.search_data(short_name='OPERA_L3_DISP-S1_V1', bounding_box=tuple(aoi))
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

    for frame in frames:
        furls = sorted({u for u in urls if frame in u})
        print('Frame', frame)
        if len(furls) > 0:
            da_list = [open_remote_disp(fs, u, y_slice, x_slice) for u in furls]
            ds = xr.concat(da_list, dim='time')
            if ds.sizes['time'] > 0 and ds.sizes['x'] > 0 and ds.sizes['y'] > 0:
                ds = ds.compute()
                ds.to_netcdf(f'OPERA_{name}/OPERA_{frame}.h5', engine='h5netcdf')

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

    product_file = download_ts(args.aoi, args.name)

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

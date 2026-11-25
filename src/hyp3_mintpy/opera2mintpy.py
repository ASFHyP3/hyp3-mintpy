"""Module to export results as MintPy results (modified from opera-utils)."""

from pathlib import Path
from typing import Any

import h5py
import numpy as np
import xarray as xr


DEFAULT_CHUNKS = (1, 512, 512)


def _decimal_year(t: np.ndarray) -> np.ndarray:
    """Convert np.datetime64 array to decimal years."""
    dt = t.astype('datetime64[D]').astype(int)
    yr_start = np.datetime64('1970-01-01', 'D').astype(int)
    days = dt - yr_start
    year = 1970 + days // 365.25
    frac = (days % 365.25) / 365.25
    return year + frac


def _mintpy_metadata(stack: xr.Dataset) -> dict[str, str]:
    """Minimal attribute set expected by MintPy."""
    disp = stack['displacement']
    stack_proj = stack.rio.reproject('EPSG:4326')
    xs = stack_proj.coords['x']
    ys = stack_proj.coords['y']
    meta = {
        'FILE_TYPE': 'timeseries',
        'UNIT': 'm',
        'LENGTH': str(disp.shape[1]),
        'WIDTH': str(disp.shape[2]),
        'X_FIRST': str(xs.values[0]),
        'Y_FIRST': str(ys.values[0]),
        'X_STEP': str(xs.values[1] - xs.values[0]),
        'Y_STEP': str(ys.values[1] - ys.values[0]),
        'X_UNIT': 'degrees',
        'Y_UNIT': 'degrees',
        'EPSG': '4326',
        'PLATFORM': 'Sentinel-1',
        'ORBIT_DIRECTION': _get_orbit_direction(stack),
        # Reasonable constants
        'HEIGHT': '750000.0',
        'WAVELENGTH': '0.05546576',  # m (C-band S-1)
    }
    return meta


def _get_orbit_direction(stack: xr.Dataset) -> str:
    """Get the orbit direction from the product."""
    dset = 'orbit_pass_direction'
    return stack.attrs[dset]


def _write_hdf5(
    out_f: Path,
    datasets: dict[str, tuple[Any, tuple[Any, ...], Any]],
    attrs: dict[str, str],
) -> None:
    """Writer that mimics MintPy's `writefile.layout_hdf5()`."""
    with h5py.File(out_f, 'w') as f:
        for k, v in attrs.items():
            f.attrs[k] = v
        # Copy all datasets from the input file
        compression: str | None = 'gzip'
        for name, (dtype, shape, data) in datasets.items():
            requested_chunks = DEFAULT_CHUNKS[-len(shape) :]  # Pick only last 2
            if len(shape) < 2:
                chunks = None
                compression = None
            else:
                chunks = tuple(min(c, s) for c, s in zip(requested_chunks, shape))
            dset = f.create_dataset(name, shape, dtype=dtype, chunks=chunks, compression=compression)
            if data is not None:
                dset[...] = data


def create_static_layers(
    los_enu_path: Path | str,
    meta: dict[str, Any],
    layover_shadow_mask_path: Path | str | None = None,
    dem_path: Path | str | None = None,
    frame: str = '',
    path: str = '',
) -> None:
    """Create static layer products from reformatted DISP-S1 data.

    Parameters
    ----------
    los_enu_path : Path | str
        Path to the LOS-ENU geometry file.
    meta : dict[str, Any]
        Metadata to be written to the static layer files.
    dem_path : Path | str, optional
        Path to the DEM file.
    layover_shadow_mask_path : Path | str | None, optional
        Path to the layover/shadow mask file.
    outdir : Path, optional
        Output directory for the static layer files.
        Default is "mintpy".

    """
    ny, nx = int(meta['LENGTH']), int(meta['WIDTH'])
    geometry_meta = meta | {'FILE_TYPE': 'geometry'}

    los_enu_data = xr.open_dataarray(los_enu_path).data
    east, north, up = los_enu_data
    # Incidence angle is the angle between the line-of-sight (LOS) vector and
    # the normal to the ellipsoid at the target
    incidence_angle = np.degrees(np.arccos(up))

    # Calculate azimuth angle from East and North components
    # See calc_azimuth_from_east_north_obs for reference
    azimuth_angle = -1 * np.rad2deg(np.arctan2(east, north)) % 360

    if dem_path is not None:
        dem = xr.open_dataarray(dem_path).data
    else:
        dem = np.zeros((ny, nx), dtype=np.float32)
    if layover_shadow_mask_path is not None:
        shadow_mask = xr.open_dataarray(layover_shadow_mask_path).data
    else:
        shadow_mask = np.zeros((ny, nx), dtype=np.int8)
    geometry_dsets = {
        'height': (np.float32, (ny, nx), dem.data),  # DEM heights
        'incidenceAngle': (np.float32, (ny, nx), incidence_angle),
        'azimuthAngle': (np.float32, (ny, nx), azimuth_angle),
        'shadowMask': (np.int8, (ny, nx), shadow_mask),
    }

    _write_hdf5(Path(f'geometryGeo_{frame}_path{path}.h5'), geometry_dsets, geometry_meta)
    print('Created geometryGeo.h5.')


def disp_to_mintpy(
    ds: xr.Dataset,
    frame: str,
) -> None:
    """Convert a reformatted DISP-S1 NetCDF file to MintPy inputs.

    Parameters
    ----------
    reformatted_nc_path : Path
        Path to the reformatted DISP-S1 NetCDF file.
        Result from `opera-utils disp-s1-reformat`
    sample_disp_nc : Path
        Path to one of the sample DISP-S1 NetCDF files.
    geometry_dir : Path | str, optional
        Path to a directory containing the LOS ENU, DEM, and layover/shadow mask files.
        Alternative to specifying the individual files separately.
    los_enu_path : Path | str, optional
        Path to the line-of-sight (LOS) 3-band east, north, up file.
    dem_path : Path | str, optional
        Path to the DEM file.
    layover_shadow_mask_path : Path | str, optional
        Path to the layover/shadow mask file.
    outdir : Path, optional
        Output directory for the MintPy inputs.
        Default is "mintpy".
    virtual : bool, optional
        If True, uses the virtual dataset (VDS) feature of HDF5
        to avoid copying data from the NetCDF's to the mintpy files.
        See https://docs.h5py.org/en/stable/vds.html
        VDS can be quicker to run and use less disk space, but it may
        cause problems when reading the files from other directories
        or moving the `timeseries.h5` file.
        Default is False
    reliability_threshold : float, optional
        Pixel must be valid in >= `reliability_threshold` of epochs to be kept.
        Default is 0.90


    """
    path = int(ds.attrs['track_number'])
    spath = f'{path:03d}'
    crs_wkt = ds['spatial_ref'].attrs.get('crs_wkt')
    ds = ds.rio.write_crs(crs_wkt)
    ds = ds.set_coords('spatial_ref')
    # ds = ds.rio.reproject("EPSG:4326")
    disp = ds['displacement'].transpose('time', 'y', 'x').data.astype(np.float32)
    times = ds.attrs['dates']
    bperp = ds['perpendicular_baseline'].data.astype(np.float32)
    zero_bperp = np.zeros((bperp.shape[1], bperp.shape[2]))

    # Prepend zero-reference epoch if reference_time precedes the first time
    disp = np.concatenate([np.zeros((1, *disp.shape[1:]), dtype=np.float32), disp])
    bperp = np.concatenate([zero_bperp[np.newaxis, :, :], bperp])

    dates = np.array([t.split('T')[0] for t in times], dtype='S8')

    ny, nx = disp.shape[1:]

    ts_meta = _mintpy_metadata(ds)
    ts_dsets = {
        'date': (dates.dtype, dates.shape, dates),
        'bperp': (np.float32, bperp.shape, bperp),
        'timeseries': (np.float32, disp.shape, disp),
    }
    _write_hdf5(Path(f'timeseries_{frame}_path{spath}.h5'), ts_dsets, ts_meta)
    print('Done with timeseries.h5 (copied data)')

    # 3. avgSpatialCoh.h5  (use average_temporal_coherence layer)
    avg_coh = ds['temporal_coherence'].mean('time').data.astype(np.float32)

    # TODO: fix these to work in blocks!
    coh_meta = ts_meta | {'FILE_TYPE': 'mask', 'UNIT': '1'}
    coh_dsets = {'avgSpatialCoh': (np.float32, (ny, nx), avg_coh)}
    _write_hdf5(Path(f'avgSpatialCoh_{frame}_path{spath}.h5'), coh_dsets, coh_meta)
    print('Done with avgSpatialCoh.h5')

    # geometryGeo.h5
    # Download UTM DEM/LOS ENU/Layover shadow mask in `opera_utils.disp._download.py`
    los_enu_path = next(Path.cwd().glob(f'*{frame}*line_of_sight_enu_clip.tif'))
    dem_path = next(Path.cwd().glob(f'*{frame}*dem_clip.tif'))
    layover_shadow_mask_path = next(Path.cwd().glob(f'*{frame}*layover_shadow_mask_clip.tif'))

    if los_enu_path:
        create_static_layers(
            los_enu_path,
            ts_meta,
            dem_path=dem_path,
            layover_shadow_mask_path=layover_shadow_mask_path,
            frame=frame,
            path=spath,
        )
    else:
        print('No LOS-ENU path provided, skipping geometryGeo.h5')

    print('\nAll done')

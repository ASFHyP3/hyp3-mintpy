"""module for gps projection."""

import io
from datetime import datetime, timezone

import h5py
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import utm


def change_reference_vel(h5file: str, ref_coords: tuple[float, float]) -> None:
    """Change the reference pixel on the timeseries.

    Args:
        h5file: H5 file with the timeseries.
        ref_coords: reference pixel in lon/lat coordinates.
    """
    h5f = h5py.File(h5file, 'r+')
    velocity = h5f['velocity'][:]
    ul = (float(h5f.attrs['X_FIRST']), float(h5f.attrs['Y_FIRST']))
    steps = (float(h5f.attrs['X_STEP']), float(h5f.attrs['Y_STEP']))
    lons = np.linspace(ul[0], ul[0] + steps[0] * velocity.shape[1], velocity.shape[1])
    lats = np.linspace(ul[1] + steps[1] * velocity.shape[0], ul[1], velocity.shape[0])[::-1]
    j = np.argmin(np.abs(lons - ref_coords[0]))
    i = np.argmin(np.abs(lats - ref_coords[1]))
    velocity[velocity == 0] = np.nan

    velocity -= velocity[i, j]
    h5f['velocity'][:] = velocity
    h5f.attrs['REF_DATE'] = h5f.attrs['START_DATE']
    h5f.attrs['REF_LAT'] = lats[i]
    h5f.attrs['REF_LON'] = lons[j]
    h5f.attrs['REF_X'] = j
    h5f.attrs['REF_Y'] = i
    h5f.close()


def change_reference_ts(h5file: str, ref_coords: tuple[float, float]) -> None:
    """Change the reference pixel on the timeseries.

    Args:
        h5file: H5 file with the timeseries.
        ref_coords: reference pixel in lon/lat coordinates.
    """
    h5f = h5py.File(h5file, 'r+')
    dates = [date.decode('utf-8') for date in h5f['date'][:]]
    timeseries = h5f['timeseries'][:]
    ul = (float(h5f.attrs['X_FIRST']), float(h5f.attrs['Y_FIRST']))
    steps = (float(h5f.attrs['X_STEP']), float(h5f.attrs['Y_STEP']))
    lons = np.linspace(ul[0], ul[0] + steps[0] * timeseries.shape[2], timeseries.shape[2])
    lats = np.linspace(ul[1] + steps[1] * timeseries.shape[1], ul[1], timeseries.shape[1])[::-1]
    j = np.argmin(np.abs(lons - ref_coords[0]))
    i = np.argmin(np.abs(lats - ref_coords[1]))
    timeseries[timeseries == 0] = np.nan
    for t in range(timeseries.shape[0]):
        timeseries[t, :, :] -= timeseries[t, i, j]
    timeseries[np.isnan(timeseries)] = 0
    timeseries -= timeseries[0, :, :]
    h5f['timeseries'][:] = timeseries
    h5f.attrs['REF_DATE'] = dates[0]
    h5f.attrs['REF_LAT'] = lats[i]
    h5f.attrs['REF_LON'] = lons[j]
    h5f.attrs['REF_X'] = j
    h5f.attrs['REF_Y'] = i
    h5f.close()


def get_pixel_ts(h5file: str, coords: tuple[float, float]) -> np.ndarray:
    """Get the time series for a pixel.

    Args:
        h5file: H5 file with the timeseries.
        coords: coordinates of the pixel.

    Returns:
        ts: Numpy array with the deformation time series
    """
    h5f = h5py.File(h5file, 'r+')
    ul = (float(h5f.attrs['X_FIRST']), float(h5f.attrs['Y_FIRST']))
    steps = (float(h5f.attrs['X_STEP']), float(h5f.attrs['Y_STEP']))
    timeseries = h5f['timeseries'][:]
    h5f.close()

    lons = np.linspace(ul[0], ul[0] + steps[0] * timeseries.shape[2], timeseries.shape[2])
    lats = np.linspace(ul[1] + steps[1] * timeseries.shape[1], ul[1], timeseries.shape[1])[::-1]
    col = np.argmin(np.abs(lons - coords[0]))
    row = np.argmin(np.abs(lats - coords[1]))

    return timeseries[:, row, col]


def get_vel(df: pd.DataFrame) -> dict:
    """Get the velocity per year for GPS time series.

    Args:
        df: H5 file with the timeseries.
        coords: coordinates of the pixel.

    Returns:
        vel: Dictionary with the velocities per year on each component
    """
    vel = {}

    x = df['yyyy.yyyy']
    y = df['__east(m)']
    m_east, c_east = np.polyfit(x, y, 1)

    y = df['_north(m)']
    m_north, c_north = np.polyfit(x, y, 1)

    y = df['____up(m)']
    m_up, c_up = np.polyfit(x, y, 1)

    vel['east'] = {}
    vel['north'] = {}
    vel['up'] = {}

    vel['east']['m'] = m_east
    vel['east']['c'] = c_east

    vel['north']['m'] = m_north
    vel['north']['c'] = c_north

    vel['up']['m'] = m_up
    vel['up']['c'] = c_up

    return vel


def download_station_file(station_id: str, start: datetime | None = None, end: datetime | None = None) -> pd.DataFrame:
    """Download GPS time series file.

    Args:
        station_id: ID for the GPS station.
        start: Filter with start date.
        end: Filter with end date.

    Returns:
        df: Pandas dataframe with the time series.
    """
    url = f'https://geodesy.unr.edu/gps_timeseries/IGS20/tenv3/IGS20/{station_id}.tenv3'

    response = requests.get(url)
    df = pd.read_csv(
        io.StringIO(response.text),
        sep=r'\s+',
    )

    df['YYMMMDD'] = pd.to_datetime(df['YYMMMDD'], format='%y%b%d')
    if start is not None and end is not None:
        df = df[(df['YYMMMDD'] <= end) & (df['YYMMMDD'] >= start)]
        df['__east(m)'] = df['__east(m)'] - df['__east(m)'].iloc[0]
        df['_north(m)'] = df['_north(m)'] - df['_north(m)'].iloc[0]
        df['____up(m)'] = df['____up(m)'] - df['____up(m)'].iloc[0]

    return df


def find_stations(timeseries: str, coherence: str, geometry: str) -> tuple:
    """Find GPS stations within an AOI.

    Args:
        timeseries: Time series MintPy file.
        coherence: avgCoherenceGeo coherence file.
        geometry: geometryGeo from MintPy with the azimuth and incidence angles.

    Returns:
        stations: Names of the stations.
        lons_gps: Longitude coordinates of the stations.
        lats_gps: Latitude coordinates of the stations.
        az_gps: Azimuth angle of the corresponding pixel.
        inc_gps: Incidence angle of the corresponding pixel.
    """
    url = 'https://geodesy.unr.edu/NGLStationPages/DataHoldings.txt'
    response = requests.get(url)
    response.raise_for_status()

    df = pd.read_fwf(io.StringIO(response.text))
    df.loc[df['Long(deg)'] > 180, 'Long(deg)'] = df[df['Long(deg)'] > 180]['Long(deg)'] - 360
    df['Dtbeg'] = pd.to_datetime(df['Dtbeg'], format='%Y-%m-%d')
    df['Dtend'] = pd.to_datetime(df['Dtend'], format='%Y-%m-%d')

    # get the InSAR stack's corner coordinates
    with h5py.File(geometry, 'r') as f:
        lons = [float(f.attrs[f'LON_REF{i}']) for i in range(1, 5)]
        lats = [float(f.attrs[f'LAT_REF{i}']) for i in range(1, 5)]
        azimuth = f['azimuthAngle'][:]
        incidence = f['incidenceAngle'][:]
        df = df[(df['Long(deg)'] >= min(lons)) & (df['Long(deg)'] <= max(lons))]
        df = df[(df['Lat(deg)'] >= min(lats)) & (df['Lat(deg)'] <= max(lats))]

    with h5py.File(timeseries, 'r') as f:
        ts_start = datetime.strptime(f.attrs['START_DATE'], '%Y%m%d').replace(tzinfo=timezone.utc)
        ts_end = datetime.strptime(f.attrs['END_DATE'], '%Y%m%d').replace(tzinfo=timezone.utc)
        df = df[(df['Dtbeg'] <= ts_end) & (df['Dtend'] >= ts_start)]

    stations = []
    lons_gps = []
    lats_gps = []
    az_gps = []
    inc_gps = []

    with h5py.File(coherence, 'r') as f:
        coh = f['coherence'][:]
        lons = [float(f.attrs[f'LON_REF{i}']) for i in range(1, 5)]
        lats = [float(f.attrs[f'LAT_REF{i}']) for i in range(1, 5)]
        lons_in = np.linspace(min(lons), max(lons), coh.shape[1])
        lats_in = np.linspace(min(lats), max(lats), coh.shape[0])[::-1]
        LONS, LATS = np.meshgrid(lons_in, lats_in)
        XX, YY, z, n = utm.from_latlon(LATS, LONS)
        for i in range(len(list(df['Long(deg)']))):
            x, y, z, n = utm.from_latlon(
                list(df['Lat(deg)'])[i], list(df['Long(deg)'])[i], force_zone_number=z, force_zone_letter=n
            )
            dist = np.sqrt((XX[coh > 0.3] - x) ** 2 + (YY[coh > 0.3] - y) ** 2)
            if dist[np.nanargmin(dist)] < 500:
                stations.append(list(df['Sta'])[i])
                lons_gps.append(LONS[coh > 0.3][np.nanargmin(dist)])
                lats_gps.append(LATS[coh > 0.3][np.nanargmin(dist)])
                az_gps.append(azimuth[coh > 0.3][np.nanargmin(dist)])
                inc_gps.append(incidence[coh > 0.3][np.nanargmin(dist)])

    return stations, lons_gps, lats_gps, az_gps, inc_gps


def find_reference_station(coherence: str, stations: list, lons_gps: list, lats_gps: list) -> tuple:
    """Reference time series file with respect to the farthest GPS station.

    Args:
        coherence: coherence file.
        stations: Names of the stations.
        lons_gps: Longitude coordinates of the stations.
        lats_gps: Latitude coordinates of the stations.

    Returns:
        ref_sta: Name of the reference station.
        ref_lon: Longitude coordinate for the reference station.
        ref_lat: Latitude coordinate for the reference station.
    """
    with h5py.File(coherence, 'r') as f:
        lons = [float(f.attrs[f'LON_REF{i}']) for i in range(1, 5)]
        lats = [float(f.attrs[f'LAT_REF{i}']) for i in range(1, 5)]
        lon_cen = np.mean([min(lons), max(lons)])
        lat_cen = np.mean([min(lats), max(lats)])
        dist_cen = np.sqrt((np.array(lons_gps) - lon_cen) ** 2 + (np.array(lats_gps) - lat_cen) ** 2)
        ref_lon = np.array(lons_gps)[np.argmax(dist_cen)]
        ref_lat = np.array(lats_gps)[np.argmax(dist_cen)]
        ref_sta = np.array(stations)[np.argmax(dist_cen)]
    return ref_sta, ref_lon, ref_lat


def reference_timeseries(timeseries: str, coherence: str, stations: list, lons_gps: list, lats_gps: list) -> tuple:
    """Reference time series file with respect to the farthest GPS station.

    Args:
        timeseries: Time series h5 file.
        coherence: coherence file.
        stations: Names of the stations.
        lons_gps: Longitude coordinates of the stations.
        lats_gps: Latitude coordinates of the stations.

    Returns:
        ref_sta: Name of the reference station.
        ref_lon: Longitude coordinate for the reference station.
        ref_lat: Latitude coordinate for the reference station.
    """
    ref_sta, ref_lon, ref_lat = find_reference_station(coherence, stations, lons_gps, lats_gps)
    change_reference_ts(timeseries, ref_coords=[ref_lon, ref_lat])

    return ref_sta, ref_lon, ref_lat


def plot_comparison(timeseries: str, coherence: str, geometry: str) -> None:
    """Plot comparison of time series between the GPS stations projected into the line of sight and the InSAR time series.

    Args:
        timeseries: Time series file path.
        coherence: Coherence file path.
        geometry: Geometry file path.
    """
    stations, lons_gps, lats_gps, az_gps, inc_gps = find_stations(timeseries, coherence, geometry)
    if len(stations) == 0:
        print('No GPS stations found')
        return
    ref_sta, ref_lon, ref_lat = reference_timeseries(timeseries, coherence, stations, lons_gps, lats_gps)
    with h5py.File(timeseries, 'r') as f:
        ts_start = datetime.strptime(f.attrs['START_DATE'], '%Y%m%d').replace(tzinfo=timezone.utc)
        ts_end = datetime.strptime(f.attrs['END_DATE'], '%Y%m%d').replace(tzinfo=timezone.utc)
        dates = f['date'][:]
        dates = [datetime.strptime(date.decode('utf-8'), '%Y%m%d').replace(tzinfo=timezone.utc) for date in dates]

    df_ref = download_station_file(ref_sta, start=ts_start, end=ts_end)
    vel_ref = get_vel(df_ref)
    for i, sta in enumerate(stations):
        if sta == ref_sta:
            pass
        df_sta = download_station_file(sta, start=ts_start, end=ts_end)
        df_sta['__east(m)'] = df_sta['__east(m)'] - vel_ref['east']['m'] * df_sta['yyyy.yyyy']
        df_sta['_north(m)'] = df_sta['_north(m)'] - vel_ref['north']['m'] * df_sta['yyyy.yyyy']
        df_sta['____up(m)'] = df_sta['____up(m)'] - vel_ref['up']['m'] * df_sta['yyyy.yyyy']

        df_sta['__east(m)'] = df_sta['__east(m)'] - df_sta['__east(m)'].iloc[0]
        df_sta['_north(m)'] = df_sta['_north(m)'] - df_sta['_north(m)'].iloc[0]
        df_sta['____up(m)'] = df_sta['____up(m)'] - df_sta['____up(m)'].iloc[0]
        ts_ref = f'{timeseries.split(".h5")[0]}_ref.h5'

        los_gps = -(
            df_sta['__east(m)'] * np.sin(np.radians(inc_gps[i])) * np.cos(np.radians(90 - az_gps[i]))
            - df_sta['_north(m)'] * np.sin(np.radians(inc_gps[i])) * np.sin(np.radians(90 - az_gps[i]))
            - df_sta['____up(m)'] * np.cos(np.radians(inc_gps[i]))
        )
        los_insar = get_pixel_ts(ts_ref, coords=[lons_gps[i], lats_gps[i]])
        plt.figure()
        plt.title(sta)
        plt.scatter(df_sta['YYMMMDD'], los_gps, label='GPS')
        plt.scatter(dates, los_insar + (np.mean(los_gps) - np.mean(los_insar)), label='InSAR')
        plt.legend()
        plt.savefig(f'{sta}.png')

    return ref_sta, ref_lon, ref_lat

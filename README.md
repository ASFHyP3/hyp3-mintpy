# HyP3 MintPy

HyP3 plugin for MintPy processing.

## Usage
The `hyp3_mintpy` command line tool can be run using the following structure:
```bash
python -m hyp3_mintpy \
  --job-name Okmok_44 \
  --min-coherence 0.1 \
  --start-date 2019-01-01 \
  --end-date 2021-01-01
```
Where:

* `--job-name` is the multiburst project name name in HyP3
* `--min-coherence` is the minimum coherence for the timeseries inversion
* `--start-date` start date for the timeseries (will discard products before this date)
* `--end-date` end date for the timeseries (will discard products after this date)

> [!IMPORTANT]
> Earthdata credentials are necessary to access HyP3 data. See the Credentials section for more information.

### Credentials

Generally, credentials are provided via environment variables, but some may be provided by command-line arguments or via a `.netrc` file. 

For Earthdata login, you can provide credentials by exporting environment variables:
```
export EARTHDATA_USERNAME=your-edl-username
export EARTHDATA_PASSWORD=your-edl-password
```
or via your [`~/.netrc` file](https://everything.curl.dev/usingcurl/netrc) which should contain lines like these two:
```
machine urs.earthdata.nasa.gov login your-edl-username password your-edl-password
```

## Developer Setup
1. Ensure that conda is installed on your system (we recommend using [mambaforge](https://github.com/conda-forge/miniforge#mambaforge) to reduce setup times).
2. Download a local version of the `hyp3-mintpy` repository (`git clone https://github.com/ASFHyP3/hyp3-mintpy.git`)
3. In the base directory for this project call `mamba env create -f environment.yml` to create your Python environment, then activate it (`mamba activate hyp3-mintpy`)
4. Finally, install a development version of the package (`python -m pip install -e .`)

To run all commands in sequence use:
```bash
git clone https://github.com/ASFHyP3/hyp3-mintpy.git
cd hyp3-mintpy
mamba env create -f environment.yml
mamba activate hyp3-mintpy
python -m pip install -e .
```

## Contributing
Contributions to the HyP3 mintpy plugin are welcome! If you would like to contribute, please submit a pull request on the GitHub repository.

## Contact Us
Want to talk about HyP3 mintpy? We would love to hear from you!

Found a bug? Want to request a feature?
[open an issue](https://github.com/ASFHyP3/hyp3-gather-landsat/issues/new)

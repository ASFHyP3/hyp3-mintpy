"""mintpy processing for HyP3."""

import logging
import os
import warnings
from argparse import ArgumentParser
from pathlib import Path

from hyp3lib.aws import upload_file_to_s3
from hyp3lib.fetch import write_credentials_to_netrc_file

from hyp3_mintpy.process import process_mintpy


def main() -> None:
    """HyP3 entrypoint for hyp3_mintpy."""
    parser = ArgumentParser()
    parser.add_argument('--bucket', help='AWS S3 bucket HyP3 for upload the final product(s)')
    parser.add_argument('--bucket-prefix', default='', help='Add a bucket prefix to product(s)')

    # TODO: Your arguments here
    parser.add_argument('--job-name', help='Write this greeting to a product file', required = True)
    parser.add_argument('--min-coherence', default=0.01, type=float, help='Write this greeting to a product file', required = False)

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

    product_file = process_mintpy(
        job_name=args.job_name,
        min_coherence=args.min_coherence
    )

    if args.bucket:
        upload_file_to_s3(product_file, args.bucket, args.bucket_prefix)


if __name__ == '__main__':
    main()

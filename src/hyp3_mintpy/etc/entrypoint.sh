#!/bin/bash --login
set -e
conda activate hyp3-mintpy
exec python -um hyp3_mintpy "$@"

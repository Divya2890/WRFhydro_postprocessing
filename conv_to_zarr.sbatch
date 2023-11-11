#!/bin/bash
#SBATCH --job-name=convert_to_zarr
#SBATCH --partition=wholenode
#SBATCH --nodes=1
#SBATCH --ntasks=128
#SBATCH --time=01:00:00
#SBATCH --error=/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/log_files/conv_zarr.out
#SBATCH --output=/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/log_files/conv_zarr.err

module --force purge

module load anaconda/2021.05-py38

conda activate dask_demo_project

python3 conv_zarr.py

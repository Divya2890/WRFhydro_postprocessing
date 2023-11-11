#!/bin/bash
#SBATCH --job-name=zarr_to_csv
#SBATCH --partition=wholenode
#SBATCH --nodes=1
#SBATCH --ntasks=128
#SBATCH --time=01:30:00
#SBATCH --error=/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/log_files/csv.err
#SBATCH --output=/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/log_files/csv.out

module --force purge

module load anaconda/2021.05-py38

conda activate dask_demo_project

python3 zarr_csv.py

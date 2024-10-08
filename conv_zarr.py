#!/usr/bin/env python
#imports
import glob
import logging
import fsspec
import ujson
import time
import dask
import dask.delayed
import xarray as xr
import pandas as pd
import dataretrieval.nwis as nwis  # retrive the observed streamflow of USGS gages
import s3fs
from datetime import datetime
import os
import random
import dask.dataframe as dd
from netCDF4 import Dataset
from dask.delayed import delayed
import zarr
from kerchunk.hdf import SingleHdf5ToZarr 
from kerchunk.combine import MultiZarrToZarr
from dask.distributed import Client, LocalCluster
import pathlib

print("imports are all successfull")

# Creating a temporary json folder to store the information in netcdf files as jsons files 

pathlib.Path('/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/jsons/').mkdir(exist_ok=True)
output_wrfhydro="/anvil/scratch/x-cybergis/compute/1688405429GhGyc/Outputs_640cores"

#input year 
year ='2013'

if __name__ == '__main__':
    client = Client()
    client
    chrtout_path = '{0}/CHRTOUT/{1}*.CHRTOUT_DOMAIN1'.format(output_wrfhydro,year)
    chrtout_path=glob.glob(chrtout_path)
    files=[]
    for folder_path in chrtout_path:
        files.append(folder_path)

#function to generate json files from the netcdf files

    def gen_json(u):
        with fsspec.open(u) as inf:
            h5chunks = SingleHdf5ToZarr(inf, u, inline_threshold=300)
            with open(f"jsons/{u.split('/')[-1]}.json", 'wb') as outf:
               outf.write(ujson.dumps(h5chunks.translate()).encode())

    dask.compute(*[dask.delayed(gen_json)(u) for u in files])

    json_list =sorted(glob.glob('/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/jsons/{0}*.json'.format(year)))
    mzz = MultiZarrToZarr(json_list,concat_dims='time',inline_threshold=0)
    mzz.translate('/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/jsons/combined.json')
    fs = fsspec.filesystem(
    "reference", 
    fo="/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/jsons/combined.json", 
    skip_instance_cache=True
)
    m = fs.get_mapper("")
    ds = xr.open_dataset(m, engine='zarr').chunk(chunks={"time":67, "feature_id":10000})

    print("Successfully read the information from the combined json and output it as a xarray dataset")
    print(ds)

    ds.to_zarr("/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/zarr_files/{0}.zarr".format(year),consolidated=True, mode="w",safe_chunks=False)
    print("succesfully converted to zarr files")


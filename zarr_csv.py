#!/usr/bin/env python
# imports

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

#INPUT THAT IS HARDCODED AND IS OUGHT TO BE CHANGED BASED ON USER PREFERENCES IS THE YEAR AND DATA RANGE, YOU WANT THE CSV FILES TO BE GENERATED FOR

year='2012'

# Specify date range of interest to create the csv files for the variables of interest
dates=("2012-01-01 ","2012-12-31")

# path of NWM retrospective (NWM-R2.0)

s3_path = 's3://noaa-nwm-retro-v2-zarr-pds'

# Connect to S3
s3 = s3fs.S3FileSystem(anon=True)
store = s3fs.S3Map(root=s3_path, s3=s3, check=False)

# load the dataset
start_time = time.time()
print("before reading amazon bucket data", start_time)
ds = xr.open_zarr(store=store, consolidated=True)
end_time = time.time()
elapsed_time = end_time - start_time

# Print the loop execution time
print("time taken to load zarr data from amazon bucket: {:.2f} seconds".format(elapsed_time))

print("done loading Zarr dataset")

# path of the WRF-Hydro model results/outputs
output_wrfhydro="/anvil/scratch/x-cybergis/compute/1688405429GhGyc/Outputs_640cores"


#( INCLUDE THIS PART OF THE CODE IF YOU WANT TO GENERATE THE ZARR FILES FROM THE NETCDF FILES )

###########################################
#start_time = time.time()
#json_list =sorted(glob.glob('/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/multizarr/jsons3/2012*.json'))
#mzz = MultiZarrToZarr(json_list,concat_dims='time',inline_threshold=0)
#mzz.translate('/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/multizarr/combined.json')
#fs = fsspec.filesystem(
#    "reference", 
#    fo="/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/multizarr/combined3.json", 
#    skip_instance_cache=True
#)
#m = fs.get_mapper("")
#reach_ds = xr.open_dataset(m, engine='zarr').chunk(chunks={"time":67, "feature_id":10000})

#print("Successfully read the information from the combined json and output it as a xarray dataset")
#print(reach_ds)

#reach_ds.to_zarr("/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/multizarr/2012.zarr",consolidated=True, mode="w",safe_chunks=False)
#print("succesfully converted to zarr file")
#end_time = time.time()
#elapsed_time = end_time - start_time
#print("time taken to read zarr files to a dataset is {0}".format(elapsed_time))
#print("Done reading chrtout files")
############################################

# Creating a xarray data set from reading the existing zarr files for the specific input year

reach_ds = xr.open_dataset("/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/zarr_files/{0}.zarr".format(year)).chunk(chunks={"time":67, "feature_id":15000})

# path of the route link "Rouet_Link.nc"
jobid_cuahsi_subset_domain='16881406501TsMj'
routelink ='/anvil/projects/x-cis220065/x-cybergis/compute/{}/Route_Link.nc'.format(jobid_cuahsi_subset_domain)

# convert rouetlink to dataframe
route_df = xr.open_dataset(routelink).to_dataframe() # convert routelink to dataframe
route_df.gages = route_df.gages.str.decode('utf-8').str.strip()

########################################################################
########################################################################

## Querying USGS gauges and river reaches that exist within our spatial domain (watershed of interest).
usgs_gages = route_df.loc[route_df['gages'] != '']
usgs_ids=usgs_gages.gages  ## USGS gages_ids exit within the watershed of interest

# Initialize an empty dataframe for the purpose of concatenating the dataframes produced during each iteration of the for-loop.
output_df=pd.DataFrame()
start_time1 = time.time()
# Iterate through the existing USGS gages located within the spatial boundary of the watershed of interest.
for gid in usgs_ids:
    try:
        ll=str(list(usgs_gages.loc[usgs_gages.gages == gid].link)) ## corresponding streamlink to USGS ID 
        ll=ll.replace("[","")
        ll=ll.replace("]","")
        timerange = slice(dates[0], dates[1])

        # Retrieving the Simulated streamflow data
        sim_streamflow=reach_ds.sel(feature_id=int(ll), time=timerange).streamflow.persist()
        sim_streamflow_df=sim_streamflow.to_dataframe()["streamflow"]
        sim_streamflow_df=sim_streamflow_df.to_frame()

    
        # Retrieve the streamflow data from NWM-R2.0 (zarr format)
    
        R2_streamflow = ds.sel(feature_id=int(ll),
                     time=timerange).streamflow.persist() 
        R2_streamflow_df=R2_streamflow.to_dataframe()["streamflow"]
        R2_streamflow_df =R2_streamflow_df.to_frame()
        # rename the columns of streamflow records obtained from NWM-R2.0 and simulated streamflow data.
        R2_streamflow_df.rename(columns = {'streamflow':'NWM-R2.0_Streamflow_{}'.format(gid)}, inplace=True)
        sim_streamflow_df.rename(columns = {"streamflow":'Sim_Streamflow_{}'.format(gid)}, inplace=True)
        #concatenating the data frames
        df_concat2 = pd.concat([sim_streamflow_df,R2_streamflow_df], 
                                    axis=1, 
                                    join="outer")
        print("output df",df_concat2)
        output_df=pd.concat([df_concat2, output_df], axis=1, join="outer")
    except Exception as ex:
        print("an exception has occured",ex)
        continue
        
output_df.index.name = "time"
# save the merged dataframe to a CSV file
output_df.to_csv('/anvil/projects/x-cis220065/x-cybergis/compute/CHRT_CSV_Extraction/csv_files/2012.csv', index=True)
end_time1 = time.time()
# Calculate the elapsed time
elapsed_time1 = end_time1 - start_time1

# Print the loop execution time
print("Time take to create csv files for chrtout data: {:.2f} seconds".format(elapsed_time1))


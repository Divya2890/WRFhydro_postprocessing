## 🌊 Problem Statement

Handling the UGSG water data presents significant challenges due to its immense size—approximately 1TB for a single year. This dataset includes detailed information such as latitude, longitude, and various attributes of water streams (e.g., streamflow, surface water, groundwater etc). Reading data for a specific location and time can take up to 2 hours, making it impractical for real-time analysis and application.

## 🧪 Experimentation

To address these issues, several strategies were explored to enhance data accessibility and processing efficiency:

1. **🔍 Parallel Processing**: Utilized Dask and Xarray to enable parallel processing, significantly speeding up data read operations.
2. **📁 File Format Optimization**: Investigated various file formats, including Parquet, NetCDF4, and Zarr, to find the most efficient storage solution for faster data access.
3. **🔢 Chunking Strategies**: Evaluated different chunking strategies across multiple dimensions to optimize data access and processing times.

### 💡 Optimized Solution

1. **🌐 Convert Year-Long NetCDF Files**: Consolidated multiple year-long NetCDF files into a single Zarr file using kerchunk MultiZarr function and  Dask libnraies  through conv_zarr.py script
2. **📊 Generate_csv_files: Utilized the zarr_csv.py script to create a CSV file with columns representing the desired coordinate variables for various station IDs in the specified year


### 🌟 Key Highlights

- **⏱️ Reduced Access Time**: The time required to obtain a coordinate variable value for any time and location has been reduced to 11-15 minutes.
- **📉 Efficient CSV Creation**: Created CSV files of MBs in size for specific coordinates, making preprocessing and data visualizations more manageable for researchers and users in the geospatial domain.

This solution collectively streamline data handling, making it more practical for analysis and application.



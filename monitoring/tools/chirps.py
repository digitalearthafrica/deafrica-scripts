import os
import gzip
import urllib
import rasterio
import xarray as xr
from datacube.utils.cog import write_cog
from datacube.utils.geometry import assign_crs


def download_and_cog_chirps(year, month, s3_dst):
    
    #set up file strings
    filename = f"chirps-v2.0.{year}.{month}.tif"
    out_filename = f"{s3_dst}/chirps-v2.0_{year}.{month}.tif"
    url = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/{filename}"
    
    try:
        # download url
        if not os.path.exists(out_filename):
            response = urllib.request.urlopen(url)
    
    
        #unzip
        with gzip.GzipFile(fileobj=response, mode='rb') as gzip_infile:

            #grab tif data and metadata
            with rasterio.open(gzip_infile, 'r') as src:
                raster=src.read()
                transform=src.transform
                crs=src.crs
                height=raster.shape[1]
                width=raster.shape[2]

            #write to disk as geotiff
            with rasterio.open(out_filename,
                               'w',
                               driver='GTiff',
                               height=height,
                               width=width,
                               count=1,
                               dtype=rasterio.float32,
                               crs=crs,
                               transform=transform
                              ) as dst:
                dst.write(raster)

        #open as xarray and write as cog
        da = assign_crs(xr.open_rasterio(out_filename))
        write_cog(da,
                  fname=out_filename,
                  overwrite=True)
    except:
        pass


@click.command("download-chirps-rainfall")
@click.option(
    "--s3_dst", default='s3://deafrica-input-datasets/chirps_rainfall_monthly/'",
)

def cli(year, month, s3_dst):
    """
    Download CHIRPS Africa monthly tifs, COG, copy to
    S3 bucket.
    
    geotifss are copied from here:
        https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/
    """

    download_and_cog_chirps(year=year,
                            month=month,
                            s3_dst=s3_dst)
    
if __name__ == "__main__":
    cli()
    
# years = [str(i) for i in range(1981, 2022)]
# months = [str(i).zfill(2) for i in range(1,13)]

# for y in years:
#     for m in months:
#         print('working on: '+y+'-'+m,end='\r')
        
#         download_and_cog_chirps(year=y,
#                                 month=m,
#                                 s3_dst='s3://deafrica-input-datasets/chirps_rainfall_monthly/')

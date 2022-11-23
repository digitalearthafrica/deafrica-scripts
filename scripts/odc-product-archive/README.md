# Archive ODC Product/Datasets (in beta)

- This is repository contains scripts for archiving odc datasets. According to the 
[ODC API REFERENCE DOCS](https://datacube-core.readthedocs.io/en/latest/api/indexed-data/dataset-querying.html) ODC datasets can be archived and later restored.

## WARNING
 - ODC datasets can be restored using **dataset ids**, that means, **datasets ids** must stored before the archiving process.
 - The script in this repository stores the archived **dataset ids** in a ***csv  file*** in a format `PRODUCT_NAME_archived.csv`


## Running archive_product script
 - Runs on ODC environment. [To configure ODC environment](https://datacube-core.readthedocs.io/en/latest/installation/setup/ubuntu.html#)
 - Input : existing **PRODUCT-NAME**
 - Output : `PRODUCT_NAME_archived.csv` containing a list of archived **dataset ids**
 - `s3 Bucket` : s3://deafrica-landsat/status-report/archived/

## Environment variables
``` bash
export DB_DATABASE=dbname
export DB_HOSTNAME=localhost
export DB_USERNAME=example
export DB_PASSWORD=secretexample

os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["AWS_DEFAULT_REGION"] = "af-south-1"
```
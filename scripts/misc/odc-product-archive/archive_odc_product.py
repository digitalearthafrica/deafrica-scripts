
import datacube
import pandas as pd
import sys


dc = datacube.Datacube()

# store dataset list
datasets_list = []

# Empty array to store datasets ids
dataset_ids = []

# input product name
PRODUCT_NAME = sys.argv[1]

# search datasets using product name
try:

    datasets_list = dc.find_datasets(product=PRODUCT_NAME)

    # Storing dataset ids
    for dataset_id in datasets_list:
        dataset_ids.append(dataset_id.id)
except:
    print("Product name "+PRODUCT_NAME+ " does not exist")
    sys.exit(1)

# check datasets
if not dataset_ids:
    print("No datasets to archive................................................")
else:
    df = pd.DataFrame(dataset_ids)
    df.to_csv(PRODUCT_NAME + "_archived.csv")
    dc.index.datasets.archive(dataset_ids)

print("Done")
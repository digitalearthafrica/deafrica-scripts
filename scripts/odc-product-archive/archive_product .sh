#!/bin/bash


#check database connection
datacube system check

#for local test
#eval "$(conda shell.bash hook)"
#conda activate odc_env

echo "Product Name"
read PRODUCT_NAME

echo "start archiving datasets"
python3 archive_odc_product.py $PRODUCT_NAME


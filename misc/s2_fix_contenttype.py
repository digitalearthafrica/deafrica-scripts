from odc.aws.inventory import find_latest_manifest, list_inventory
from odc.aws import s3_head_object, s3_client

INVENTORY_BUCKET = "deafrica-sentinel-2-inventory"
PREFIX = "deafrica-sentinel-2/deafrica-sentinel-2-inventory/"

DO_FIX = False

if DO_FIX:
    client = s3_client(region_name="af-south-1")
else:
    client = s3_client(aws_unsigned=True, region_name="af-south-1")

manifest = find_latest_manifest(
    f"s3://{INVENTORY_BUCKET}/{PREFIX}",
    client,
)

inventory = list_inventory(manifest, s3=client)

report_every = 10000
count = 0

json_docs = 0
to_fix = 0

for obj in inventory:
    count += 1
    if count % report_every == 0:
        print(f"Processing {count}")
    if obj.Key.endswith(".json"):
        json_docs += 1
        o_dict = s3_head_object(f"s3://{obj.Bucket}/{obj.Key}", s3=client)
        if o_dict["ContentType"] != "application/json":
            try:
                if DO_FIX:
                    client.copy_object(
                        Bucket=obj.Bucket,
                        Key=obj.Key,
                        CopySource=f"{obj.Bucket}/{obj.Key}",
                        ContentType="application/json",
                        MetadataDirective="REPLACE",
                    )
                to_fix += 1
                print(f"Fixed {to_fix} out of {json_docs}")
            except KeyError as e:
                print(f"Failed to find content type for {obj.Key}", o_dict, e)

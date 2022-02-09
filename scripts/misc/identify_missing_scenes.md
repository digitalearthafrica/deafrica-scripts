# Identifying missing scenes

We needed to identify scenes based on their URI, so that we can find duplicates.
From the duplicate that is not archived, we need to identify the source Landsat
scenes. This parent's ID is required so that it can be reprocessed for WOfS
and FC.

## Merge CSVs

```python
import pandas as pd

csv1 = pd.read_csv("landsat_archived_2022-01-27.csv")
csv2 = pd.read_csv("landsat_archived_2022-02-02.csv")
csv3 = pd.read_csv("landsat_archived_2022-02-09.csv")
csvs = [csv1, csv2, csv3]
all = pd.concat(csvs)

fc_scenes = all[all["product"]=="fc_ls"]
fc_scenes["location"].to_csv("fc.csv", index=False, header=False)
```

## Identify active duplicate's parent

```python
import datacube
dc = datacube.Datacube()
index = dc.index

def get_sr_id(uri):
    for dataset in index.datasets.get_datasets_for_location(uri):
        if not dataset.is_archived:
            break

    dataset_with_parents = index.datasets.get(dataset.id, include_sources=True)
    return str(dataset_with_parents.sources["ard"].id)

fc_scenes = []
# fc.csv is really just a list of URIs
with open("fc.csv", "r") as f:
    fc_scenes = [i.strip() for i in f.readlines()]

with open("sr.txt", "w") as f:
    for uri in fc_scenes:
        id = get_sr_id(uri)
        f.write(id + "\n")
```

## Adding IDs to the queues

[This PR](https://github.com/opendatacube/datacube-alchemist/pull/135) was
implemented to enable adding lots of IDs to the queue from Alchemist.

Key capability is this:

```bash
xargs datacube-alchemist add-ids-to-queue -q deafrica-prod-af-eks-alchemist-ls-fc -c https://raw.githubusercontent.com/digitalearthafrica/config/master/prod/alchemist/fc_ls.alchemist.yaml < sr.txt
```

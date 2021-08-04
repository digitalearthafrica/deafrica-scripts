# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.5.2
#   kernelspec:
#     display_name: ODC
#     language: python
#     name: odc
# ---

# %%
import numpy as np
from tqdm.auto import tqdm
from affine import Affine
import toolz
from datacube import Datacube
from datacube.utils.geometry import GeoBox
from odc.index import ordered_dss, dataset_count

def compute_overlaps(dss):
    geom, *geom_rest = [ds.extent for ds in dss]
    g_and, g_or = geom, geom
    for g in geom_rest:
        g_and = g_and & g
        g_or = g_or | g
    return g_or, g_and

def overlap_info(dss):
    g_or, g_and = compute_overlaps(dss)
    return (g_or.area, g_and.area)

def find_dupes_to_archive(dc, time, keep_threshold = 0.05, freq='m'):
    ds_s2_order = lambda ds: (ds.center_time,
                              ds.metadata.region_code,
                              ds.metadata_doc['label'])

    query = dict(product='s2_l2a', time=time)
    n_total = dataset_count(dc.index, **query)
    dss = ordered_dss(dc, key=ds_s2_order, freq=freq, **query)

    dss = tqdm(dss, total=n_total)
    groups = (group for group in toolz.partitionby(lambda ds: (ds.center_time, ds.metadata.region_code), dss)
              if len(group)>1)

    keep_groups = []
    to_archive = []

    for dss_group in groups:
        a_or, a_and = overlap_info(dss_group)
        # aa is in range [0, 1] with
        #  0 -- 100% overlap across dupes
        #  1 -- 0% overlap across dupes (disjoint footprints)
        aa = (a_or - a_and)/a_or
        if aa > keep_threshold:
            keep_groups.append(dss_group)
        else:
            to_archive.extend(ds.id for ds in dss_group[:-1])

    return to_archive, keep_groups, n_total

# %%
dc = Datacube()
to_archive, keepers, n_total = find_dupes_to_archive(dc,
                                                     time=('2017', '2020'))
print(f"""Processed {n_total:,d} datasets.
  {len(to_archive):,d}/{n_total:,d} ~ {len(to_archive)*100/n_total:.2f}% to archive
  {len(keepers):,d} groups of datasets were retained
""")

with open('to_archive.txt', 'wt') as f:
    f.write('\n'.join(str(uuid) for uuid in to_archive))

# %%
#can be archived like this:
#dc.index.datasets.archive(to_archive)

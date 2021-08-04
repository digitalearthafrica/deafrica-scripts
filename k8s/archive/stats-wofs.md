# Notes from running WOfS Summary

``` bash
pip install --upgrade --extra-index-url="https://packages.dea.ga.gov.au" odc-stats

# Save tasks, then check out the GeoJSON file for sanity
odc-stats save-tasks --frequency annual --grid africa-30 --year 2018 wofs_ls

# Run a single tile out to the local file system.
# Make sure you've saved the yaml below before you run this.
odc-stats run wofs_ls_2018--P1Y.db \
 2018--P1Y/218/092 \
 --config wofs_annual.yaml \
 --resolution=30 \
 --location file:///home/jovyan/wofs_annual/ \
 --threads=2 \
 --memory-limit=14Gi
```

``` yaml
plugin: wofs-summary
product:
  name: wofs_ls_summary
  short_name: wofs_ls_summary
  version: 0.0.1
  region_code_format: "x{x:03d}y{y:03d}"
cog_opts:
  zlevel: 9
  overrides:
    rgba:
      compress: JPEG
      jpeg_quality: 90

```

`env PGPASSWORD=$DB_PASSWORD psql -U $DB_USERNAME -d $DB_DATABASE -h $DB_HOSTNAME -c "select p.name, count(*) from agdc.dataset as d join agdc.dataset_type as p on d.dataset_type_ref = p.id group by p.name order by count(*);"`
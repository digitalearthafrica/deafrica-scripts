#!python3
import json
import os
import re
import uuid
import warnings
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

import boto3
import numpy
import rasterio
import yaml
from botocore import UNSIGNED
from botocore.client import Config
from eodatasets3 import serialise
from eodatasets3.images import GridSpec, MeasurementBundler
from eodatasets3.model import AccessoryDoc, DatasetDoc, ProductDoc
from eodatasets3.properties import Eo3Interface
from eodatasets3.stac import to_stac_item as eo3_to_stac_item
from eodatasets3.validate import (
    Level,
    ValidationExpectations,
    validate_dataset,
)

from deafrica.io import (
    check_directory_exists,
    check_file_exists,
    get_basename,
    get_filesystem,
    get_gdal_vsi_prefix,
    get_parent_dir,
    join_url,
)

# Uncomment and add logging if and where needed
# import logging
# from tasks.common import get_logger
# logger = get_logger('EasiPrepare', level=logging.DEBUG)

# Adapted from
# https://github.com/opendatacube/tutorial-odc-product/blob/master/tasks/eo3assemble/easi_assemble.py: EasiPrepare()

# OUTPUT_NAME = "metadata.odc-metadata.yaml"
OUTPUT_NAME = "metadata.stac-item.json"


class EasiPrepare(Eo3Interface):
    def __init__(
        self,
        dataset_path: str,
        product_yaml: str | dict,
        output_path: str = None,
        input_source_datasets: list[UUID] = None,
    ) -> None:
        """
        Prepare eo3 metadata for a dataset.

        :param dataset_path:
            File system Path() to the dataset or S3 URL prefix (s3://bucket/key) to the dataset.
            A directory of files represents one dataset.
            Files should be readable by GDAL to allow generation of the grid specifications.

        :param product_yaml:
            File system Path  or URL to the corresponding product YAML. The product name is used and the
            measurements must correspond to file(s).

        :param output_path:
            Optional. Specify the output dataset YAML file. Default is to create it next
            to the dataset file(s).

        :param input_source_datasets:
            Optional. Specify the list of the UUIDs for datasets that were used in the calculation of
            this dataset.
        """
        # Internal variables
        self._dataset_scheme = None  # Set by self._set_dataset_path()
        self._dataset_path = None  # Set by self._set_dataset_path()
        self._dataset_bucket = None  # Set by self._set_dataset_path()
        self._dataset_key = None  # Set by self._set_dataset_path()
        self._output_path = None  # Set by self._set_output_path()
        self._measurements = MeasurementBundler()

        # Handle inputs
        self._set_dataset_path(dataset_path)
        self._set_output_path(output_path)
        self._product_yaml = product_yaml

        # Defaults
        self._dataset = (
            DatasetDoc()
        )  # https://github.com/opendatacube/eo-datasets/blob/develop/eodatasets3/model.py
        # id            UUID                        Dataset UUID
        # label         str                         Human-readable identifier for the datasetA dataset label
        # product       ProductDoc                  The product name
        # locations     List[str]                   Location(s) where this dataset is stored
        # crs           str                         CRS string for the dataset (measurement grids), "epsg:*" or WKT
        # geometry      BaseGeometry                Shapely geometry of the valid data coverage
        # grids         Dict[str, GridDoc]          Grid specifications for measurements
        # properties    Eo3Dict                     Raw properties
        # measurements  Dict[str, MeasurementDoc]   Loadable measurements of the dataset
        # accessories   Dict[str, AccessoryDoc]     References to accessory files
        # lineage       Dict[str, List[UUID]]       Links to source dataset uuids

        # Update defaults
        self._dataset.locations = None
        self._dataset.product = ProductDoc()
        self._dataset.product.name = self.get_product_name()
        self._dataset.accessories = {}
        self._set_dataset_lineage(input_source_datasets)

        # Available for user input, else defaults will be used
        self.geometry = None  # BaseGeometry, overrides valid_data polygon
        self.crs = (
            None  # CRS string, provide a CRS if measurements GridSpec.crs is None
        )
        self.valid_data_method = None

    def _parse_path(self, some_path):
        """
        Parse some_path for validity and type, and resolve to its absolute path.
        Test if its a URI
        - If '','file': Resolve path
        - If s3 or gs: Get bucket, key
        - Ih https(s) url: Get path
        """
        scheme, new_path, bucket, key = (None, None, None, None)
        if some_path is not None:
            loc = urlparse(str(some_path))
            if loc.scheme in ("",):
                scheme = "file"
                new_path = str(Path(some_path).resolve())
            if loc.scheme in ("file",):
                scheme = "file"
                new_path = str(Path("/".join(["", loc.netloc, loc.path])).resolve())
            elif loc.scheme in ("s3", "gs", "gcs"):
                scheme = loc.scheme
                new_path = some_path
                bucket = loc.hostname
                key = re.sub("^[/]", "", loc.path)
            elif loc.scheme in ("http", "https"):
                scheme = loc.scheme
                new_path = some_path
                raise RuntimeError('Location type "http/s" is not implemented yet')
        return (scheme, new_path, bucket, key)

    def _set_dataset_path(self, dataset_path):
        """
        Parse the dataset_path for validity and type
        """
        scheme, new_path, bucket, key = self._parse_path(dataset_path)
        self._dataset_scheme = scheme
        self._dataset_path = new_path
        self._dataset_bucket = bucket
        self._dataset_key = key

    def _set_output_path(self, output_path):
        """
        Parse the output_path for validity and type.
        If self._dataset_scheme is 's3' then output_path must be a local path to write to.
        If output_path is a directory then use OUTPUT_NAME in that directory.
        If output_path is None then use OUTPUT_NAME adjacent to _dataset_path.
        """
        scheme, new_path, bucket, key = self._parse_path(output_path)
        meta = None

        if new_path:
            # Enforce file naming rule
            if new_path.endswith(".stac-item.json") or new_path.endswith(
                ".odc-metadata.yaml"
            ):
                meta = new_path
            else:
                raise ValueError(
                    f"Invalid `output_path` file name: '{output_path}'. "
                    f"File name must end with either '.stac-item.json' "
                    "or '.odc-metadata.yaml'"
                )
        else:
            if check_directory_exists(self._dataset_path):
                meta = join_url(self._dataset_path, OUTPUT_NAME)
            elif check_file_exists(self._dataset_path):
                meta = join_url(get_parent_dir(self._dataset_path), OUTPUT_NAME)

        if meta is None:
            # Error, need a writable path
            raise RuntimeError(
                f"Require a valid output_path to write to: {output_path}"
            )
        self._output_path = meta

    def _set_dataset_lineage(self, input_source_datasets: list[UUID]):
        """
        Add the UUIDs of the source datasets to the lineage.
        """
        if input_source_datasets is None:
            self._dataset.lineage = None
        else:
            self._dataset.lineage = {"inputs": input_source_datasets}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Any necessary clean up
        pass

    # Methods to get or set properties of self._dataset
    @property
    def dataset_path(self):
        return self._dataset_path

    @property
    def dataset_scheme(self):
        return self._dataset_scheme

    @property
    def dataset_id(self) -> uuid.UUID:
        return self._dataset.id

    @dataset_id.setter
    def dataset_id(self, val: str):
        if isinstance(val, str):
            val = uuid.UUID(val)
        self._dataset.id = val

    @property
    def label(self) -> str:
        return self._dataset.label

    @label.setter
    def label(self, val: str):
        self._dataset.label = val

    @property
    def product_name(self) -> str:
        return self._dataset.product.name

    @property
    def product_uri(self) -> str:
        return self._dataset.product.href

    @product_uri.setter
    def product_uri(self, val: str):
        self._dataset.product.href = val

    @property
    def properties(self) -> dict:
        return self._dataset.properties

    @property
    def measurements(self) -> dict:
        return {
            name: (grid, path) for grid, name, path in self._measurements.iter_paths()
        }

    @property
    def accessories(self) -> dict:
        return self._dataset.accessories

    # Functions
    def get_product_name(self) -> str:
        """
        Return the product name from the product yaml
        """
        if isinstance(self._product_yaml, str):
            fs = get_filesystem(self._product_yaml)
            with fs.open(self._product_yaml) as f:
                y = yaml.load(f, Loader=yaml.FullLoader)
                return y["name"]
        elif isinstance(self._product_yaml, dict):
            return self._product_yaml["name"]

    def get_product_measurements(self) -> list:
        """
        Return list of (measurement, alias, ..) tuples
        """
        measurements = []  # list of tuples (measurement name, alias, ...)
        if isinstance(self._product_yaml, str):
            fs = get_filesystem(self._product_yaml)
            with fs.open(self._product_yaml) as f:
                y = yaml.load(f, Loader=yaml.FullLoader)
                for m in y["measurements"]:
                    t = [m["name"]]
                    if "aliases" in m:
                        t.extend(m["aliases"])
                    measurements.append(tuple(t))
        elif isinstance(self._product_yaml, dict):
            for m in self._product_yaml["measurements"]:
                t = [m["name"]]
                if "aliases" in m:
                    t.extend(m["aliases"])
                measurements.append(tuple(t))

        return measurements

    def _match_measurement_names_to_band_ids(
        self, mtuples: list, band_ids: dict, supplementary: dict = None
    ) -> dict:
        """
        Return a dict mapping each measurement name (first item in each tuple in `mtuples`) to a file path.

        :param mtuples:     list of tuples where each tuple is the measurement and aliases names from a product definition
        :param band_ids:    dict mapping a unique key (e.g., from a regex) to a file path
        :param supplementary:  dict mapping a measurement or alias name to a unique key in `band_ids`
        """
        band_set = set(band_ids.keys())
        supplementary_set = set()
        if supplementary:
            supplementary_set = set(supplementary.keys())
        measurement2path = {}

        # Match measurement names to band_ids (-> file paths)
        for mtuple in mtuples:
            mtuple_set = set(mtuple)
            common = mtuple_set & band_set
            if len(common) == 1:
                measurement2path[mtuple[0]] = band_ids[common.pop()]
                continue
            # Try supplementary
            common = mtuple_set & supplementary_set
            if len(common) == 1:
                measurement2path[mtuple[0]] = band_ids[supplementary[common.pop()]]
                continue
            raise RuntimeError(
                f"No unique match for measurements {mtuple} in file paths: {self._dataset_path}"
            )

        return measurement2path

    def map_measurements_to_paths(
        self,
        band_regex: str,
        supplementary: dict = None,
    ) -> dict:
        """
        Return dict of {measurement names: path} for matching file paths in self._dataset_path.

        Each measurement in the product yaml must have a corresponding file path.
        A file path can be for the local file system or a URI (S3, HTTPS).

        Loop through the list of product measurements
        - Compare file path band_id (from BAND_REGEX) with each tuple of measurement names and aliases
        - If found, key is first element of tuple (measurement name) and value is file path
        - If not found, try band_id key in SUPPLEMENTARY

        :param band_regex:
        Regular expression string to apply to each file in self._dataset_path.
        Match.group(1) should be the band ID used to select the corresponding measurement

        :param supplementary:
        Dict mapping any band IDs (from band_regex) to measurement names.
        Use where the unique band ID does not directly match a measurement name.
        """

        # Match band_ids to file paths
        p = re.compile(band_regex)
        band_ids = {}

        # TODO: Sort out relative filename vs self._dataset_path for rasterio

        # File system
        if self._dataset_scheme == "file":
            if Path(self._dataset_path).is_dir():
                for filename in Path(self._dataset_path).rglob("*.*"):
                    m = p.search(str(filename))
                    if m:
                        band_ids[m.group(1)] = filename
            else:
                filename = Path(self._dataset_path)
                m = p.search(str(filename.name))
                if m:
                    band_ids[m.group(1)] = str(filename)

        # S3; obtain a list of object keys for the dataset
        if self._dataset_scheme == "s3":
            # client = boto3.client("s3")
            client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
            response = client.list_objects_v2(
                Bucket=self._dataset_bucket,
                Prefix=self._dataset_key,
                # RequestPayer="requester",  # Make a parameter if/when required
            )
            # print(f'Debug: {response}')
            if response["KeyCount"] > 0:
                for item in response["Contents"]:
                    key = item["Key"]
                    m = p.search(key)
                    if m:
                        band_ids[m.group(1)] = f"s3://{self._dataset_bucket}/{key}"
                        continue

        if len(band_ids) == 0:
            raise RuntimeError(f"No matching file paths found for regex: {band_regex}")

        # Matching is done by interesection of sets, where a single common element indicates a successful match
        mtuples = self.get_product_measurements()
        measurement2path = self._match_measurement_names_to_band_ids(
            mtuples, band_ids, supplementary
        )
        return measurement2path

    def note_measurement(
        self,
        measurement_name: str,
        file_path: Path | str,
        layer: str | None = None,
        expand_valid_data: bool = True,
        relative_to_metadata: bool = True,
        grid: GridSpec | None = None,
        array: numpy.ndarray | None = None,
        nodata: float | int | None = None,
    ):
        """
        Reference a measurement from its existing file path.
        (no data is copied, but Geo information is read from it.)

        :param measurement_name:
            Measurement name corresponding to a product measurement
        :param file_path:
            Path to data file for this measurement
        :param layer:
            Layer name in data file for this measurement
        :param expand_valid_data:
            Calculate the union of valid data polygons across all measurements
        :param relative_to_metadata:
            File paths in the dataset doc will be written relative to output metadata path
        :param grid:
            A given GridSpec. Default is to read from the file_path
        :param array:
            A given data array. Default is to read from the file_path
        :param nodata:
            A given nodata value. Default is to read from the file_path
        """
        # Relative path to file
        written_path = str(file_path)
        if relative_to_metadata:
            written_path = self.relative_to_metadata_path(file_path)

        # If we have a polygon already, there's no need to compute valid data.
        if self.geometry:
            expand_valid_data = False

        src = rasterio.open(file_path)
        count = src.count
        driver = src.driver
        # Multi-band file
        if count != 1:
            src.close()
            if driver.lower() == "netcdf":
                with rasterio.open(
                    f"netcdf:{get_gdal_vsi_prefix(file_path)}:{layer}"
                ) as ds:
                    if not grid:
                        grid = GridSpec.from_rio(ds)
                    if not nodata:
                        nodata = ds.nodata
                    if expand_valid_data:
                        if not array:
                            array = ds.read(1)

            else:
                raise NotImplementedError(
                    "TODO: Only multi-band netcdf files currently supported"
                )
        else:
            if not grid:
                grid = GridSpec.from_rio(src)
            if not nodata:
                nodata = src.nodata
            if expand_valid_data:
                if not array:
                    array = src.read(1)
            src.close()

        self._measurements.record_image(
            measurement_name,  # str
            grid,  # GridSpec
            written_path,  # Path, str
            array,  # numpy.ndarray
            layer,  # str [None]. Layer name in a multi-band file?
            nodata=nodata,  # float, int [None: 'nan' if float else 0]
            expand_valid_data=expand_valid_data,  # bool [True: create valid_values mask with nodata]
        )

    def note_accessory_file(
        self, name: str, file_path: Path, relative_to_metadata: bool = True
    ):
        """
        Record a reference to an additional file.

        :param name: identifying name, eg 'metadata:mtl'
        :param file_path: relative path to file.
        :param relative_to_metadata:
            File paths in the dataset doc will be written relative to output metadata path
        """
        # Relative path to file
        written_path = str(file_path)
        if relative_to_metadata:
            written_path = self.relative_to_metadata_path(file_path)

        if name in self.accessories:
            existing = self.accessories[name].path
            if existing is not None and existing != written_path:
                raise ValueError(f"Duplicate accessory name {name!r}")
        self.accessories[name] = AccessoryDoc(path=written_path, name=name)

    def relative_to_metadata_path(self, path: Path) -> str:
        """Return path relative to output metadata path"""
        if self._dataset_scheme == "file":
            p = os.path.relpath(path, self._output_path.parent)  # str
        elif self._dataset_scheme == "s3":
            p = re.sub(f"^{self._dataset_path}[/]?", "", path)
        else:
            raise ValueError(
                f"Unsupported path scheme for relative path: {self._dataset_scheme}: {path}"
            )
        return p

    def map_measurements_to_files(self, *args, **kwargs):
        """Deprecated"""
        return self.map_measurements_to_paths(*args, **kwargs)

    def map_measurements_to_s3_urls(self, *args, **kwargs):
        """Deprecated"""
        return self.map_measurements_to_paths(*args, **kwargs)

    def add_accessory_file(self, *args, **kwargs):
        """Deprecated"""
        # eodatasets3: This was renamed to note_accessory_file() for consistency in our method names.
        self.note_accessory_file(*args, **kwargs)

    def to_dataset_doc(
        self,
        validate_correctness: bool = True,
        sort_measurements: bool = True,
        expect_geometry: bool = True,
    ) -> DatasetDoc:
        """ """
        dataset = self._dataset

        # Measurements
        crs, grid_docs, measurement_docs = self._measurements.as_geo_docs()
        dataset.grids = grid_docs
        dataset.measurements = measurement_docs
        if sort_measurements:
            dataset.measurements = dict(sorted(dataset.measurements.items()))

        # CRS
        # Use the CRS from measurement GridSpecs if its not None
        # Else, use the user-supplied CRS if not None
        # (If we need to override the GridSpec CRS then can hack the doc after?)
        dataset.crs = None
        if crs is not None:
            dataset.crs = self._crs_str(crs)
            if self.crs:
                warnings.warn(
                    f"Using CRS from measurements and ignoring user input ({self.crs}): {crs}"
                )
        elif self.crs is not None:
            dataset.crs = self._crs_str(self.crs)  # From user input

        # Geometry
        if self.geometry:
            valid_data = self._valid_shape(self.geometry)
        else:
            valid_data = self._measurements.consume_and_get_valid_data(
                valid_data_method=self.valid_data_method
            )
        if valid_data.is_empty:
            valid_data = None
            expect_geometry = False
        dataset.geometry = valid_data

        # Validate
        if validate_correctness:
            doc = serialise.to_doc(dataset)
            expect = ValidationExpectations(require_geometry=expect_geometry)
            validation_messages = validate_dataset(doc, expect=expect)

            for m in validation_messages:
                if m.level in (Level.info, Level.warning):
                    warnings.warn(str(m))
                elif m.level == Level.error:
                    # Since only first error is raised do a count of all
                    # errors to flag potential nested errors
                    error_count = len(
                        [m for m in validation_messages if m.level == Level.error]
                    )
                    raise RuntimeError(
                        f"Validation error: {m}, Total errors: {error_count}"
                    )
                else:
                    raise RuntimeError(
                        f"Internal error: Unhandled type of message level: {m.level}"
                    )

        return dataset

    def to_stac_item(
        self,
        validate_correctness: bool = True,
        sort_measurements: bool = True,
        expect_geometry: bool = True,
    ) -> dict:
        dataset = self.to_dataset_doc(
            validate_correctness=validate_correctness,
            sort_measurements=sort_measurements,
            expect_geometry=expect_geometry,
        )
        stac_item = eo3_to_stac_item(
            dataset=dataset, stac_item_destination_url=self._output_path
        )
        return stac_item

    def write_eo3(
        self, validate_correctness: bool = True, sort_measurements: bool = True
    ) -> tuple:
        """
        Validate and write the dataset doc.

        :param validate_correctness:
            Run the eo3-validator on the dataset doc
        :param sort_measurements:
            Order measurements alphabetically (instead of insert-order)

        :returns: The id and final path to the dataset doc file.
        """
        # Or serialise.to_path(output_path, dataset)
        dataset = self.to_dataset_doc(
            validate_correctness=validate_correctness,
            sort_measurements=sort_measurements,
        )
        doc = serialise.to_formatted_doc(dataset)

        output_yaml = self._output_path
        if not get_basename(output_yaml).lower().endswith(".yaml"):
            raise ValueError(
                f"YAML filename doesn't end in *.yaml (?). Received {output_yaml!r}"
            )

        fs = get_filesystem(output_yaml, anon=False)
        with fs.open(output_yaml, "w") as stream:
            yaml = serialise._init_yaml()
            yaml.dump_all([doc], stream)
        return dataset.id, self._output_path

    def write_stac(
        self, validate_correctness: bool = True, sort_measurements: bool = True
    ) -> tuple:
        """
        Validate and write the dataset doc to a stac file.

        :param validate_correctness:
            Run the eo3-validator on the dataset doc
        :param sort_measurements:
            Order measurements alphabetically (instead of insert-order)

        :returns: The id and final path to the dataset doc file.
        """
        stac_item = self.to_stac_item(
            validate_correctness=validate_correctness,
            sort_measurements=sort_measurements,
        )

        output_json = self._output_path
        if not get_basename(output_json).lower().endswith(".json"):
            raise ValueError(
                f"JSON filename doesn't end in *.json (?). Received {output_json!r}"
            )

        fs = get_filesystem(output_json, anon=False)
        with fs.open(output_json, "w") as file:
            json.dump(stac_item, file, indent=2)  # `indent=4` makes it human-readable

        return stac_item["id"], self._output_path

    # Borrowed from https://github.com/opendatacube/eo-datasets/blob/develop/eodatasets3/assemble.py
    def _crs_str(self, crs) -> str:
        return f"epsg:{crs.to_epsg()}" if crs.is_epsg_code else crs.to_wkt()

    # Borrowed from https://github.com/opendatacube/eo-datasets/blob/develop/eodatasets3/images.py
    def _valid_shape(shape: "BaseGeometry") -> "BaseGeometry":  # type: ignore  # noqa: F821
        if shape.is_valid:
            return shape
        return shape.buffer(0)

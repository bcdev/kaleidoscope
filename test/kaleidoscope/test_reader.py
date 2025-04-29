#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides unit-level tests for the source product reader.
"""

import json
import unittest
import warnings
from importlib import resources
from pathlib import Path
from typing import Any

from xarray import DataArray
from xarray import Dataset

from kaleidoscope.interface.constants import VID_LAT
from kaleidoscope.interface.constants import VID_LON
from kaleidoscope.interface.constants import VID_TIM
from kaleidoscope.reader import Reader
from kaleidoscope.util.ncbin import ncgen

warnings.filterwarnings("ignore")


class ReaderTest(unittest.TestCase):
    """
    Tests the source product reader.

    Generates product files from CDL templates and reads them.
    """

    config: dict[str:Any]
    """
    The reader configuration.
    """
    source_files: list[Path]
    """
    The list of source product datasets generated from CDL
    template files.
    """

    def setUp(self):
        """
        Initializes the test.

        Generates netCDF datasets from source product template CDL files.
        """
        package = "kaleidoscope.config"
        name = "config.reader.json"
        with resources.path(package, name) as resource:
            with open(resource) as r:
                self.config = json.load(r)
        self.generate_source_files()

    def generate_source_files(self):
        """Generate dummy source datasets from template CDL files."""
        self.source_files = []
        with resources.path("kaleidoscope", "templates") as resource:
            for f in resource.iterdir():
                if f.name.endswith(".cdl"):
                    self.source_files.append(ncgen(f))

    def tearDown(self):
        """Cleans up the test."""
        for f in self.source_files:
            f.unlink()
        self.source_files.clear()

    def test_read(self):
        """Tests reading the generated source datasets."""
        for source_file in self.source_files:
            source = None
            try:
                reader = self.create_reader(
                    source_file.stem.replace("_", "-")
                )
                source = reader.read(source_file)
                self.assertIsInstance(source, Dataset)

                global_attrs = source.attrs
                self.assertIsInstance(global_attrs, dict)

                time = source[VID_TIM]
                self.assertIsInstance(time, DataArray)
            finally:
                if source is not None:
                    source.close()

    def create_reader(self, product_type):
        chunks = self.config["config.reader.chunks"]
        for k, v in chunks.get(product_type, chunks["_"]).items():
            chunks[k] = v
        return Reader(self.config)


if __name__ == "__main__":
    unittest.main()

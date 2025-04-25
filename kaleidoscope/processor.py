#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the Kaleidoscope processor.
"""

import json
from argparse import Namespace
from importlib import resources
from typing import Any

import yaml
from typing_extensions import override
from xarray import Dataset

from . import __name__
from . import __version__
from .interface.constants import DID_LAT
from .interface.constants import DID_LON
from .interface.processing import Processing
from .interface.reading import Reading
from .interface.writing import Writing
from .logger import get_logger
from .operators.randomizeop import RandomizeOp
from .readerfactory import ReaderFactory
from .writerfactory import WriterFactory


class Processor(Processing):
    """! The Kaleidoscope processor."""

    def __init__(self, config_package: str = "kaleidoscope.config"):
        """
        Creates a new processor instance.

        :param config_package: The name of the processor
        configuration package.
        """
        self._config_package = config_package

    @override
    def get_config_package(self):  # noqa: D102
        return self._config_package

    @override
    def get_default_config(self) -> dict[str:Any]:  # noqa: D102
        package = self.get_config_package()
        name = "config.yml"
        with resources.path(package, name) as resource:
            with open(resource) as r:
                config = yaml.safe_load(r)
                config["processor_name"] = self.get_name()
                config["processor_version"] = self.get_version()
                return config

    @override
    def get_name(self):  # noqa: D102
        return __name__

    @override
    def get_version(self):  # noqa: D102
        return __version__

    @override
    def run(self, args: Namespace):  # noqa: D102
        config = sorted(vars(args).items(), key=lambda item: item[0])
        for name, value in config:
            get_logger().info(f"config: {name} = {value}")

        source: Dataset | None = None
        target: Dataset | None = None
        try:
            reader: Reading = self._create_reader(args)
            writer: Writing = self._create_writer(args)

            # open the source file
            get_logger().debug(f"opening source dataset: {args.source_file}")
            source = reader.read(args.source_file)

            # create the processing graph
            get_logger().info("starting creating processing graph")
            target = self.get_result(args, source)
            get_logger().info("finished creating processing graph")

            # write the target file
            get_logger().info(
                f"starting writing target dataset: {args.target_file}"
            )
            writer.write(target, args.target_file)
            get_logger().info("finished writing target dataset")
        finally:
            get_logger().info("starting closing datasets")
            if target is not None:
                target.close()
            if source is not None:
                source.close()
            get_logger().info("finished closing datasets")

    @override
    def get_result(  # noqa: D102
        self, args: Namespace, *inputs: Dataset
    ) -> Dataset:
        return RandomizeOp(args).run(inputs[0])

    def _create_reader(self, args) -> Reading:
        """This method does not belong to public API."""
        package = self.get_config_package()
        name = "config.reader.json"
        with resources.path(package, name) as resource:
            get_logger().debug(f"reading resource: {resource}")
            with open(resource) as r:
                config = json.load(r)
        chunks = config["config.reader.chunks"]
        chunks[DID_LAT] = chunks.get(args.product_type, chunks["_"])[DID_LAT]
        chunks[DID_LON] = chunks.get(args.product_type, chunks["_"])[DID_LON]
        if args.chunk_size_lat is not None:
            chunks[DID_LAT] = args.chunk_size_lat
        if args.chunk_size_lon is not None:
            chunks[DID_LON] = args.chunk_size_lon
        if args.engine_reader:
            config["config.reader.engine"] = args.engine_reader
        return ReaderFactory.create_reader(config=config)

    def _create_writer(self, args) -> Writing:
        """This method does not belong to public API."""
        package = self.get_config_package()
        name = "config.writer.json"
        with resources.path(package, name) as resource:
            get_logger().debug(f"reading resource: {resource}")
            with open(resource) as r:
                config = json.load(r)
        chunks = config["config.writer.chunks"]
        chunks[DID_LAT] = chunks.get(args.product_type, chunks["_"])[DID_LAT]
        chunks[DID_LON] = chunks.get(args.product_type, chunks["_"])[DID_LON]
        if args.chunk_size_lat is not None:
            chunks[DID_LAT] = args.chunk_size_lat
        if args.chunk_size_lon is not None:
            chunks[DID_LON] = args.chunk_size_lon
        if args.engine_writer:
            config["config.writer.engine"] = args.engine_writer
        return WriterFactory.create_writer(
            config=config, progress=args.progress
        )

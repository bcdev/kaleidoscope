#!/usr/bin/env python
#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the Kaleidoscope resolve main function.
"""

import json
import signal
import sys
import warnings
from argparse import ArgumentDefaultsHelpFormatter
from argparse import ArgumentError
from argparse import ArgumentParser
from argparse import Namespace
from importlib import resources
from pathlib import Path
from typing import Any
from typing import TextIO

import yaml
from xarray import DataArray
from xarray import Dataset
from xarray import decode_cf

from kaleidoscope import __name__
from kaleidoscope import __version__
from kaleidoscope.interface.constants import VID_TIM
from kaleidoscope.interface.processing import Processing
from kaleidoscope.interface.reading import Reading
from kaleidoscope.interface.writing import Writing
from kaleidoscope.logger import get_logger
from kaleidoscope.readerfactory import ReaderFactory
from kaleidoscope.runner import Runner
from kaleidoscope.signalhandler import AbortHandler
from kaleidoscope.signalhandler import KeyboardInterruptHandler
from kaleidoscope.signalhandler import TerminationRequestHandler
from kaleidoscope.writerfactory import WriterFactory

warnings.filterwarnings("ignore")


class _ArgumentParser(ArgumentParser):
    """A command line parser overriding the default error handling."""

    def error(self, message):
        """Handles an error by raising an argument error."""
        raise ArgumentError(argument=None, message=message)


class Parser:
    """A factory to create the command line parser."""

    @staticmethod
    def create() -> ArgumentParser:
        """
        Creates a new command line parser.

        :return: The parser.
        """
        parser = _ArgumentParser(
            prog=f"{__name__}-resolve",
            description="This scientific processor transforms a time series "
            "dataset into separate datasets, each of which corresponds to a "
            "different time step.",
            epilog="Copyright (c) Brockmann Consult GmbH, 2025. "
            "License: MIT",
            exit_on_error=False,
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        Parser._add_arguments(parser)
        Parser._add_options(parser)
        Parser._add_version(parser)
        return parser

    @staticmethod
    def _add_arguments(parser):
        """This method does not belong to public API."""
        parser.add_argument(
            "source_file",
            help="the file path of the source dataset.",
            type=Path,
        )
        parser.add_argument(
            "target_file",
            help="the file path of the target datasets. The pattern "
            "YYYYMMDD is replaced with the date associated with the "
            "time step extracted.",
            type=Path,
        )

    @staticmethod
    def _add_options(parser):
        """This method does not belong to public API."""
        parser.add_argument(
            "--engine-reader",
            help="specify the engine used to read the source product file.",
            choices=["h5netcdf", "netcdf4", "zarr"],
            required=False,
            dest="engine_reader",
        )
        parser.add_argument(
            "--engine-writer",
            help="specify the engine used to write the target product file.",
            choices=["h5netcdf", "netcdf4", "zarr"],
            required=False,
            dest="engine_writer",
        )
        parser.add_argument(
            "--log-level",
            help="specify the log level.",
            choices=["debug", "info", "warning", "error", "off"],
            required=False,
            dest="log_level",
        )
        parser.add_argument(
            "--progress",
            help="enable progress bar display.",
            action="store_true",
            required=False,
            dest="progress",
        )
        parser.add_argument(
            "--stack-traces",
            help="enable Python stack traces.",
            action="store_true",
            required=False,
            dest="stack_traces",
        )

    @staticmethod
    def _add_version(parser):
        """This method does not belong to public API."""
        parser.add_argument(
            "-v",
            "--version",
            action="version",
            version=f"%(prog)s {__version__}",
        )


def date(t: DataArray) -> str:
    """
    Converts a time stamp into a date (YYYYMMDD).

    :param t: The time stamp.
    :return: The date.
    """
    return t.dt.strftime("%Y%m%d").item()


def time(d: Dataset) -> DataArray:
    """
    Extracts the time data from a dataset.

    :param d: The dataset.
    :return: The time data.
    """
    t = decode_cf(
        d,
        drop_variables=[v for v in d.variables if v != VID_TIM],
    )
    return t[VID_TIM]


class Processor(Processing):
    """! The Kaleidoscope resolve processor."""

    def __init__(self, config_package: str = "kaleidoscope.config"):
        """
        Creates a new processor instance.

        :param config_package: The name of the processor
        configuration package.
        """
        self._config_package = config_package

    def get_config_package(self):  # noqa: D102
        return self._config_package

    def get_default_config(self) -> dict[str, Any]:  # noqa: D102
        package = self.get_config_package()
        name = "config.yml"
        with resources.path(package, name) as resource:
            with open(resource) as r:
                config = yaml.safe_load(r)
                config["processor_name"] = self.get_name()
                config["processor_version"] = self.get_version()
                return config

    def get_name(self):  # noqa: D102
        return f"{__name__}-resolve"

    def get_version(self):  # noqa: D102
        return __version__

    def run(self, args: Namespace):  # noqa: D102
        config = sorted(vars(args).items(), key=lambda item: item[0])
        for name, value in config:
            get_logger().info(f"config: {name} = {value}")

        source: Dataset | None = None
        try:
            reader: Reading = self._create_reader(args)
            source: Dataset = reader.read(args.source_file)
            t: DataArray = time(source)
            for i in range(t.size):
                target: Dataset = source.isel(time=slice(i, i + 1))
                try:
                    get_logger().info(
                        f"starting writing time step: {date(t[i])}"
                    )
                    writer: Writing = self._create_writer(args)
                    writer.write(
                        target,
                        f"{args.target_file}".replace("YYYYMMDD", date(t[i])),
                    )
                    get_logger().info(f"finished writing time step")
                finally:
                    if target is not None:
                        target.close()
        finally:
            if source is not None:
                source.close()

    def get_result(  # noqa: D102
        self, args: Namespace, *inputs: Dataset
    ) -> Dataset:
        """Not used."""
        pass

    def _create_reader(self, args) -> Reading:
        """This method does not belong to public API."""
        package = self.get_config_package()
        name = "config.reader.json"
        with resources.path(package, name) as resource:
            get_logger().debug(f"reading resource: {resource}")
            with open(resource) as r:
                config = json.load(r)
        chunks = config["config.reader.chunks"]
        for k, v in chunks["_"].items():
            chunks[k] = v
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
        for k, v in chunks["_"].items():
            chunks[k] = v
        if args.engine_writer:
            config["config.writer.engine"] = args.engine_writer
        return WriterFactory.create_writer(
            config=config, progress=args.progress
        )


def main() -> int:
    """
    The main function.

    Initializes signal handlers, runs the processor and returns
    an exit code.

    :return: The exit code.
    """
    signal.signal(signal.SIGABRT, AbortHandler())
    signal.signal(signal.SIGINT, KeyboardInterruptHandler())
    signal.signal(signal.SIGTERM, TerminationRequestHandler())
    return run()


def run(
    args: list[str] | None = None,
    out: TextIO = sys.stdout,
    err: TextIO = sys.stderr,
):
    """
    Runs the processor and returns an exit code.

    :param args: An optional list of arguments.
    :param out: The stream to use for usual log messages.
    :param err: The stream to use for error log messages.
    :return: The exit code.
    """
    return Runner(Processor("kaleidoscope.config"), Parser.create()).run(
        args, out, err
    )


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python
#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the Kaleidoscope collect main function
to produce uncertainties from a given Monte Carlo ensemble.
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
from xarray import Dataset

from kaleidoscope import __name__
from kaleidoscope import __version__
from kaleidoscope.interface.processing import Processing
from kaleidoscope.interface.reading import Reading
from kaleidoscope.interface.writing import Writing
from kaleidoscope.logger import get_logger
from kaleidoscope.operators.collectop import CollectOp
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
            prog=f"{__name__}-collect",
            description="This scientific processor computes standard "
            "uncertainty from a given Monte Carlo ensemble.",
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
            "source_glob",
            help="the file path glob of the source datasets. The first "
            "entry in the expanded list of file paths shall refer to the "
            "nominal (i.e., not randomized) source dataset. The remaining "
            "entries shall refer to randomized variants of the nominal "
            "source. Only the '*' character shall be used for globbing.",
            type=str,
        )
        parser.add_argument(
            "target_file",
            help="the file path of the target dataset.",
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
            "--mode",
            help="specify the operating mode. In multithreading mode a "
            "multithreading scheduler is used. In synchronous mode a "
            "single-thread scheduler is used.",
            choices=["multithreading", "synchronous"],
            required=False,
            dest="mode",
        )
        parser.add_argument(
            "--workers",
            help="specify the number of workers used in multithreading "
            "mode. If not set, the number of workers is determined by the "
            "system.",
            choices=[1, 2, 3, 4, 5, 6, 7, 8],
            type=int,
            required=False,
            dest="workers",
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


class Processor(Processing):
    """! The Kaleidoscope reduce processor."""

    def __init__(self, config_package: str = "kaleidoscope.config"):
        """
        Creates a new processor instance.

        :param config_package: The name of the processor
        configuration package.
        """
        self._config_package = config_package

    def get_config_package(self):  # noqa: D102
        return self._config_package

    def get_default_config(self) -> dict[str:Any]:  # noqa: D102
        package = self.get_config_package()
        name = "config.yml"
        with resources.path(package, name) as resource:
            with open(resource) as r:
                config = yaml.safe_load(r)
                config["processor_name"] = self.get_name()
                config["processor_version"] = self.get_version()
                return config

    def get_name(self):  # noqa: D102
        return f"{__name__}-collect"

    def get_version(self):  # noqa: D102
        return __version__

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
            get_logger().debug(f"opening source dataset: {args.source_glob}")
            source = reader.read(args.source_glob)

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

    def get_result(  # noqa: D102
        self, args: Namespace, *inputs: Dataset
    ) -> Dataset:
        return CollectOp(args).run(inputs[0])

    def _create_reader(self, args) -> Reading:
        """This method does not belong to public API."""
        package = self.get_config_package()
        name = "config.reader.json"
        with resources.path(package, name) as resource:
            get_logger().debug(f"reading resource: {resource}")
            with open(resource) as r:
                config = json.load(r)
        chunks = config["config.reader.chunks"]
        for k, v in chunks.get(args.source_type, chunks["_"]).items():
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
        for k, v in chunks.get(args.source_type, chunks["_"]).items():
            chunks[k] = v
        if args.engine_writer:
            config["config.writer.engine"] = args.engine_writer
        return WriterFactory.create_writer(
            config=config, progress=args.progress
        )


def main() -> int:
    """
    The main function.

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

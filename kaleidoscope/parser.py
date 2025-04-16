#  Copyright (c) Brockmann Consult GmbH, 2024
#  License: MIT

"""
This module provides the command line parser.
"""

from argparse import ArgumentDefaultsHelpFormatter
from argparse import ArgumentError
from argparse import ArgumentParser
from pathlib import Path
from typing import Literal

from . import __name__
from . import __version__


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
            prog=f"{__name__}",
            description="This scientific processor simulates measurement errors.",
            epilog="Copyright (c) Brockmann Consult GmbH, 2025. License: MIT",
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
            type=Parser.FileType("r"),
        )
        parser.add_argument(
            "target_file",
            help="the file path of the target dataset.",
            type=Parser.FileType("w"),
        )

    @staticmethod
    def _add_options(parser):
        """This method does not belong to public API."""
        parser.add_argument(
            "--chunk-size-lat",
            help="specify the chunk size along the latitudinal dimension "
            "for reading and computing data arrays. A value of `-1` refers "
            "to full latitudinal chunk size and a value of `0` refers "
            "to the chunk size used in the source file.",
            type=Parser.IntType(-1),
            required=False,
            dest="chunk_size_lat",
        )
        parser.add_argument(
            "--chunk-size-lon",
            help="specify the chunk size along the longitudinal dimension "
            "for reading and computing data arrays. A value of `-1` refers "
            "to full longitudinal chunk size and a value of `0` refers "
            "to the chunk size used in the source file.",
            type=Parser.IntType(-1),
            required=False,
            dest="chunk_size_lon",
        )
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
            "--no-progress",
            help="disable progress bar display.",
            action="store_false",
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
        parser.add_argument(
            "--no-stack-traces",
            help="disable Python stack traces.",
            action="store_false",
            required=False,
            dest="stack_traces",
        )
        parser.add_argument(
            "--test",
            help="enable test mode.",
            action="store_true",
            required=False,
            dest="test",
        )
        parser.add_argument(
            "--no-test",
            help="disable test mode.",
            action="store_false",
            required=False,
            dest="test",
        )
        parser.add_argument(
            "--tmpdir",
            help="specify the path to the temporary directory.",
            type=Parser.DirType(),
            required=False,
            dest="tmpdir",
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

    class IntType:
        """Callable to convert an argument into an integer value."""

        def __init__(
            self, min_int: int | None = None, max_int: int | None = None
        ):
            """
            Creates a new instance of this class.

            :param min_int: The lower bound (inclusive) of the value domain.
            :param max_int: The upper bound (inclusive) of the value domain.
            """
            self._min = min_int
            self._max = max_int

        def __call__(self, arg: str) -> int:
            """Converts an argument into an integer value."""
            i = int(arg)
            if self._min is not None and i < self._min:
                raise TypeError("Argument is not a valid integer value")
            if self._max is not None and i > self._max:
                raise TypeError("Argument is not a valid integer value")
            return i

    class DirType:
        """Callable to convert an argument into a directory path."""

        def __call__(self, arg: str) -> Path:
            """Converts an argument into a directory path."""
            path = Path(arg)
            if not path.is_dir():
                raise TypeError("Path does not refer to a directory")
            try:
                path = path.resolve()
            except RuntimeError:
                raise TypeError(f"Directory path {path} cannot be resolved")
            return path

    class FileType:
        """! Callable to convert an argument into a file path."""

        def __init__(self, mode: Literal["r", "w"] = "r"):
            """
            Creates a new instance of this class.

            @param mode The file mode required.
            """
            self._mode = mode

        def __call__(self, arg: str):
            """Converts an argument into a file path."""
            path = Path(arg)
            if self._mode == "r":
                if not path.is_file():
                    raise TypeError(
                        f"Path {path} does not refer to an existing file"
                    )
            if self._mode == "w":
                if path.is_dir() or not path.parent.is_dir():
                    raise TypeError(
                        f"Path {path} does not refer to a writeable file"
                    )
            try:
                path = path.resolve()
            except RuntimeError:
                raise TypeError(f"File path {path} cannot be resolved")
            return path


if __name__ == "__main__":
    Parser.create().parse_args(["--help"])

#!/usr/bin/env python
#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the main function.
"""

import signal
import sys
import warnings
from typing import TextIO

from kaleidoscope.parser import Parser
from kaleidoscope.processor import Processor
from kaleidoscope.runner import Runner
from kaleidoscope.signalhandler import AbortHandler
from kaleidoscope.signalhandler import KeyboardInterruptHandler
from kaleidoscope.signalhandler import TerminationRequestHandler

warnings.filterwarnings("ignore")


def main() -> int:
    """
    The main function (and entry point) of the processor.

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

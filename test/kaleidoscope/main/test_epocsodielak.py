#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""This module provides integration test cases."""

import unittest

from kaleidoscope.interface.exitcodes import ExitCodes
from kaleidoscope.main import epocsodielak


class NoArgumentsTest(unittest.TestCase):
    """Tests a call without command line arguments."""

    def test_run_without_arguments(self):
        """
        Tests missing command line arguments. Termination with
        nonzero exit code is expected.
        """
        exit_code = epocsodielak.run()
        self.assertEqual(ExitCodes.FAILURE_ARGUMENT_ERROR, exit_code)


class HelpTest(unittest.TestCase):
    """Tests the `-h` and `--help` command line options."""

    def test_option_h(self):
        """Tests the `-h` command line option."""

        exit_code = epocsodielak.run(args=["-h"])
        self.assertEqual(ExitCodes.SUCCESS, exit_code)

    def test_option_help(self):
        """Tests the `--help` command line option."""
        exit_code = epocsodielak.run(args=["--help"])
        self.assertEqual(ExitCodes.SUCCESS, exit_code)


class VersionTest(unittest.TestCase):
    """Tests the `-v` and `--version` command line options."""

    def test_option_v(self):
        """Tests the `-v` command line option."""
        exit_code = epocsodielak.run(args=["-v"])
        self.assertEqual(ExitCodes.SUCCESS, exit_code)

    def test_option_version(self):
        """Tests the `--version` command line option."""
        exit_code = epocsodielak.run(args=["--version"])
        self.assertEqual(ExitCodes.SUCCESS, exit_code)


if __name__ == "__main__":
    unittest.main()

#  Copyright (c) Brockmann Consult GmbH, 2024
#  License: MIT

"""
This module provides a reader factory.
"""

from typing import Any

from .interface.reading import Reading


class ReaderFactory:
    """The reader factory."""

    @staticmethod
    def create_reader(config: dict[str:Any] | None = None) -> Reading:
        """
        A static factory method to create a new reader instance.

        :param config: An optional configuration for the file system reader.
        :return: A new reader instance.
        """
        from .reader import Reader

        return Reader(config)

#  Copyright (c) Brockmann Consult GmbH, 2024
#  License: MIT

"""
This module provides a writer factory.
"""

from typing import Any

from .interface.writing import Writing


class WriterFactory:
    """The writer factory."""

    @staticmethod
    def create_writer(
        config: dict[str:Any] | None = None,
        progress: bool = False,
    ) -> Writing:
        """
        A static factory method to create a new writer instance.

        :param config: An optional configuration for the file system writer.
        :param progress: To display a progress bar on the console, if
        supported by the writer.
        :return: A new writer instance.
        """
        from .writer import Writer

        return Writer(config, progress=progress)

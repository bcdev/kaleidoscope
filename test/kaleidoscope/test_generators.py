#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides unit-level tests for random number generators.
"""

import unittest

from numpy.random import PCG64

from kaleidoscope.generators import DefaultGenerator


class DefaultGeneratorTest(unittest.TestCase):

    def test_random(self):
        rng = DefaultGenerator(seed=PCG64(42))
        self.assertEqual(6164909031098000398, rng.random())
        self.assertEqual(62765134502071353, rng.random())
        self.assertEqual(6068961337446000720, rng.random())
        self.assertEqual(3424215743300924766, rng.random())
        self.assertEqual(1906168894638979906, rng.random())
        self.assertEqual(1787925334117997081, rng.random())


if __name__ == "__main__":
    unittest.main()

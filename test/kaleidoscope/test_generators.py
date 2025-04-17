#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides unit-level tests for random number generators.
"""

import unittest

from kaleidoscope.generators import DefaultGenerator


class DefaultGeneratorTest(unittest.TestCase):

    def test_random(self):
        rng = DefaultGenerator(42)
        self.assertEqual(6164909031098000398, rng.next_int64())
        self.assertEqual(62765134502071353, rng.next_int64())
        self.assertEqual(6068961337446000720, rng.next_int64())
        self.assertEqual(3424215743300924766, rng.next_int64())
        self.assertEqual(1906168894638979906, rng.next_int64())
        self.assertEqual(1787925334117997081, rng.next_int64())


if __name__ == "__main__":
    unittest.main()

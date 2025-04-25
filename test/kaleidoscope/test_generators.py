#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides unit-level tests for random number generators.
"""

import unittest

from kaleidoscope.generators import DefaultGenerator


class DefaultGeneratorTest(unittest.TestCase):

    def test_random(self):
        g = DefaultGenerator(42)
        self.assertEqual(6164909031098000398, g.next())
        self.assertEqual(62765134502071353, g.next())
        self.assertEqual(6068961337446000720, g.next())
        self.assertEqual(3424215743300924766, g.next())
        self.assertEqual(1906168894638979906, g.next())
        self.assertEqual(1787925334117997081, g.next())


if __name__ == "__main__":
    unittest.main()

import unittest
import lm_list
import os
import Trace
import e_errors

class TestLMList(unittest.TestCase):

    def setUp(self):
        self.lm = lm_list.LMList()

    def test___init__(self):
        self.assertTrue(isinstance(self.lm,lm_list.LMList))

    def test_restore(self):
        self.lm.restore()

    def test_append_and_remove(self):
        self.lm.append('a')
        self.assertTrue('a' in self.lm.list)
        self.lm.remove('a')
        self.assertFalse('a' in self.lm.list)

if __name__ == "__main__":
    unittest.main()

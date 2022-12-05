import unittest
import safe_dict
from UserDict import UserDict
class TestSafeDict(unittest.TestCase):
    def setUp(self):
        self.zd = safe_dict.SafeDict({})
        self.nzd = safe_dict.SafeDict({'a':'a', 'b':'b'})

    def test___init__(self):
        self.assertTrue(isinstance(self.zd, safe_dict.SafeDict))
        self.assertTrue(isinstance(self.nzd, safe_dict.SafeDict))

    def test___getitem__(self):
        self.assertEqual(self.nzd['a'],'a')
        self.assertEqual(self.nzd['b'],'b')
        self.assertEqual(self.zd['b'], {})

    def test___nonzero__(self):
        self.assertEqual(2, self.nzd.__nonzero__())
        self.assertEqual(0, self.zd.__nonzero__())

if __name__ == "__main__":
    unittest.main()

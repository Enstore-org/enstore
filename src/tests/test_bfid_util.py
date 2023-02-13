import unittest
from bfid_util import BfidGenerator
from bfid_util import is_bfid
from bfid_util import extract_brand
from bfid_util import strip_brand
from bfid_util import bfid2time

BFIDGENERATOR = BfidGenerator("CMS")


class TestBfidUtils(unittest.TestCase):

    def test_is_bfid(self):
        bfid = BFIDGENERATOR.create()
        self.assertTrue(is_bfid(bfid))

    def test_extract_brand(self):
        test_brand = "Acme"
        BFIDGENERATOR.set_brand(test_brand)
        bfid = BFIDGENERATOR.create()
        extr_brand = extract_brand(bfid)
        self.assertEqual(extr_brand, test_brand)
        self.assertTrue(test_brand in bfid)

    def test_strip_brand(self):
        test_brand = "Acme"
        BFIDGENERATOR.set_brand(test_brand)
        bfid = BFIDGENERATOR.create()
        self.assertTrue(test_brand in bfid)
        bfid = strip_brand(bfid)
        self.assertFalse(test_brand in bfid)

    def test_bfid2time(self):
        bfid = BFIDGENERATOR.create()
        rslt = bfid2time(bfid)
        self.assertTrue(isinstance(rslt, int))


class TestBfidGenerator(unittest.TestCase):

    def test_set_and_get_brand(self):
        brand = "Acme"
        BFIDGENERATOR.set_brand(brand)
        self.assertEqual(brand, BFIDGENERATOR.get_brand())

    def test_check(self):
        bfid = BFIDGENERATOR.create()
        tst = BFIDGENERATOR.check(bfid)
        self.assertTrue(tst[0])

    def test_create(self):
        bfid = BFIDGENERATOR.create()
        self.assertTrue(is_bfid(bfid))


if __name__ == "__main__":   # pragma: no cover
    unittest.main()

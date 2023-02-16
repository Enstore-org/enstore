import unittest
import volume_family


class Test_volume_family(unittest.TestCase):

    def setUp(self):
        self.grp = "storage_group"
        self.ff = "file_family"
        self.wrp = "wrapper"
        self.vol1 = "%s.%s.%s" % (self.grp, self.ff, self.wrp)
        self.vol2 = "wierd"

    def test_extract_storage_group(self):
        grp = volume_family.extract_storage_group(self.vol1)
        self.assertEqual(grp, self.grp)

    def test_extract_storage_group2(self):
        grp = volume_family.extract_storage_group(self.vol2)
        self.assertEqual(self.vol2, grp)

    def test_extract_storage_group3(self):
        grp = volume_family.extract_storage_group("")
        self.assertEqual("", grp)

    def test_extract_file_family(self):
        ff = volume_family.extract_file_family(self.vol1)
        self.assertEqual(ff, self.ff)

    def test_extract_file_family_2(self):
        ff = volume_family.extract_file_family(self.vol2)
        self.assertEqual("none", ff)

    def test_extract_wrapper(self):
        wrp = volume_family.extract_wrapper(self.vol1)
        self.assertEqual(wrp, self.wrp)

    def test_extract_wrapper_2(self):
        wrp = volume_family.extract_wrapper(self.vol2)
        self.assertEqual("none", wrp)

    def test_make_volume_family(self):
        vf = volume_family.make_volume_family(self.grp, self.ff, self.wrp)
        self.assertEqual(self.vol1, vf)

    def test_match_volume_families(self):
        vf = volume_family.make_volume_family(self.grp, self.ff, self.wrp)
        self.assertTrue(volume_family.match_volume_families(vf, self.vol1))

    def test_match_volume_families2(self):
        vf = volume_family.make_volume_family(self.grp, self.ff, self.wrp)
        self.assertFalse(volume_family.match_volume_families(vf, self.vol2))


if __name__ == "__main__":   # pragma: no cover
    unittest.main()

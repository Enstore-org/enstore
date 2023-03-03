import unittest
import generic_driver

class TestDriverError(unittest.TestCase):
    def test___init__(self):
        self.assertTrue(isinstance(generic_driver.DriverError("foo"), Exception))   

        
class TestDriver(unittest.TestCase):
    def setUp(self):
        self.gd = generic_driver.Driver()

    def test_fileno(self):
        with self.assertRaises(NotImplementedError):
            self.gd.fileno()

    def test_tell(self):
        with self.assertRaises(NotImplementedError):
            self.gd.tell()

    def test_open(self):
        with self.assertRaises(NotImplementedError):
            self.gd.open( "foo", "bar")

    def test_flush(self):
        with self.assertRaises(NotImplementedError):
            self.gd.flush( "foo")

    def test_close(self):
        with self.assertRaises(NotImplementedError):
            self.gd.close()

    def test_rewind(self):
        with self.assertRaises(NotImplementedError):
            self.gd.rewind()

    def test_seek(self):
        with self.assertRaises(NotImplementedError):
            self.gd.seek( "foo")

    def test_skipfm(self):
        with self.assertRaises(NotImplementedError):
            self.gd.skipfm( "foo")

    def test_get_status(self):
        with self.assertRaises(NotImplementedError):
            self.gd.get_status()

    def test_verify_label(self):
        with self.assertRaises(NotImplementedError):
            self.gd.verify_label("foo", "bar")

    def test_set_mode(self):
        with self.assertRaises(NotImplementedError):
            self.gd.set_mode("foo", "bar")

    def test_rates(self):
        with self.assertRaises(NotImplementedError):
            self.gd.rates()

    def test_get_cleaning_bit(self):
        self.assertEqual(self.gd.get_cleaning_bit(), 0)

if __name__ == "__main__":
    unittest.main()

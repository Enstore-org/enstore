import unittest
import null_driver
import os
import time
import StringIO
import setpath
import generic_driver
import strbuffer
import e_errors
import Trace

class TestNullDriver(unittest.TestCase):

    def setUp(self):
        self.ndw = null_driver.NullDriver()
        self.ndr = null_driver.NullDriver()

    def test___init__(self):
        self.assertTrue(isinstance(self.ndw, generic_driver.Driver))
        self.assertTrue(isinstance(self.ndr, generic_driver.Driver))

    def test_open(self):
        self.ndr.open(mode=0)
        self.ndw.open(mode=1)
        self.assertEqual(self.ndw.device, '/dev/null')
        self.assertEqual(self.ndr.device, '/dev/zero') 
        ndw2 = null_driver.NullDriver()
        with self.assertRaises(ValueError):
            ndw2.open(mode=2)

    def test_seek_tell_rewind(self):
        self.ndr.seek(30)
        self.assertEqual(self.ndr.tell(), (30,30))
        self.ndr.rewind()
        self.assertEqual(self.ndr.tell(), (0,0))
    
    def test_fileno(self):
        self.ndw.open(mode=1)
        self.assertEqual(self.ndw.fileno(), self.ndw.fd)

    def test_flush(self):
        self.assertEqual(self.ndw.flush(), None)

    def test_close(self):
        self.assertEqual(self.ndw.close(), 0)
        self.ndw.open(mode=1)
        self.assertEqual(self.ndw.close(), None)
        self.ndw.fd = -3
        self.assertEqual(self.ndw.close(), -1)

    def test_read(self):
        # test normal read
        self.ndr.open(mode=0)
        s = StringIO.StringIO()
        r = self.ndr.read(s.buf,0,10)
        self.assertEqual(r, 10)
        
        # test read from bad file descriptor
        with self.assertRaises(IOError):
            self.ndw.open(mode=0)
            self.ndw.fd = -1
            self.ndw.read(s.buf,0,10)

        # test read from file open for writing
        with self.assertRaises(ValueError):
            self.ndw.open(mode=1)
            self.ndw.read(s.buf,0,10)  

    def test_write(self):
        # test normal write
        self.ndw.open(mode=1)
        s = StringIO.StringIO()
        w = self.ndw.write(s.buf,0,10)
        self.assertEqual(w, 10)

        # test write with bad file descriptor, returning -1
        self.ndr.open(mode=1)
        self.ndr.fd = -1
        self.assertEqual(self.ndr.write(s.buf,0,10), -1)

        # test write to driver open for reading
        with self.assertRaises(ValueError):
            self.ndr.open(mode=0)
            self.ndr.write(s.buf,0,10)

    def test_write_and_skip_fm(self):
        self.ndw.open(mode=1)
        self.assertEqual(self.ndw.loc, 0)
        self.ndw.writefm()
        self.assertEqual(self.ndw.loc, 1)
        self.ndw.skipfm(10)
        self.assertEqual(self.ndw.loc, 11)

    def test_eject(self):
        self.assertEqual(self.ndw.eject(), None)

    def test_set_mode(self):
        self.assertEqual(self.ndw.set_mode(1), None)
    
    def test_rates(self):
        self.assertEqual(self.ndw.rates(), (0,0))
    
    def test_verify_label(self):
        self.assertEqual(self.ndw.verify_label(), ( e_errors.READ_VOL1_READ_ERR, None))
        self.assertEqual(self.ndr.verify_label(label='foo'), ( e_errors.OK, None))

    def test_tape_transfer_time(self):
        self.assertEqual(self.ndw.tape_transfer_time(), 0)

if __name__ == "__main__":
    unittest.main()

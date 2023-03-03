import unittest
import enstore_plotter_module
import os
import errno
import time
import tempfile

class TestEnstorePlotterModule(unittest.TestCase):

    def setUp(self):
        self.epm = enstore_plotter_module.EnstorePlotterModule("test")

    def test___init__(self):
        self.assertTrue(self.epm.name == "test")
        self.assertTrue(isinstance(self.epm, enstore_plotter_module.EnstorePlotterModule))

    def test_set_and_isActive(self):
        self.assertEqual(self.epm.isActive(), True) 
        self.epm.setActive(False)
        self.assertEqual(self.epm.isActive(), False)

    def test_book(self):
        self.epm.book("frame")

    def test_fill(self):
        self.epm.fill("frame")

    def test_plot(self):
        self.epm.plot()

    def test_install(self):
        self.epm.install()

    def test_add_and_get_parameter(self):
        self.epm.add_parameter("par_name", "par_value")
        self.assertEqual(self.epm.parameters["par_name"], "par_value")
        self.assertEqual(self.epm.get_parameter("par_name"), "par_value")

    def test_move(self):
        src = tempfile.mktemp()
        dst = tempfile.mktemp()
        open(src, 'w').close()
        self.epm.move(src, dst)
        self.assertTrue(os.path.exists(dst))
        self.assertFalse(os.path.exists(src))
        os.remove(dst)

    def test_roundtime(self):
        t1 = 1677875595.160911
        self.assertEqual(enstore_plotter_module.roundtime(t1), 1677875595.0)
        self.assertEqual(enstore_plotter_module.roundtime(t1,'floor'), 1677798000.0)
        self.assertEqual(enstore_plotter_module.roundtime(t1,'ceil'), 1677884399.0)
    
        self.assertEqual(enstore_plotter_module.roundtime(1000000000), 1000000000.0)
        self.assertEqual(enstore_plotter_module.roundtime(1000000001), 1000000001.0)

if __name__ == "__main__":
    unittest.main()

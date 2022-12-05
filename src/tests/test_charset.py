import unittest
import string
import charset

class TestCharSet(unittest.TestCase):

    def setUp(self):
        self.charset = charset.charset
        self.filenamecharset = charset.filenamecharset
        self.hostnamecharset = charset.hostnamecharset

    def test_is_string_in_character_set(self):
        self.assertFalse(charset.is_string_in_character_set('',self.hostnamecharset))
        self.assertTrue(charset.is_string_in_character_set(self.filenamecharset,self.filenamecharset))
        self.assertTrue(charset.is_string_in_character_set(self.hostnamecharset,self.hostnamecharset))
        self.assertTrue(charset.is_string_in_character_set(self.charset,self.charset))
        self.assertFalse(charset.is_string_in_character_set(self.filenamecharset,self.hostnamecharset))
        self.assertTrue(charset.is_string_in_character_set(self.hostnamecharset,self.filenamecharset))
        self.assertFalse(charset.is_string_in_character_set(self.filenamecharset,self.charset))
        self.assertTrue(charset.is_string_in_character_set(self.charset,self.filenamecharset))
        self.assertFalse(charset.is_string_in_character_set(self.hostnamecharset,self.charset))
        self.assertFalse(charset.is_string_in_character_set(self.charset,self.hostnamecharset))

    def test_is_in_charset(self):
        self.assertTrue(charset.is_in_charset(self.charset))
        self.assertFalse(charset.is_in_charset(self.hostnamecharset))
        self.assertFalse(charset.is_in_charset(self.filenamecharset))

    def test_is_in_filenamecharset(self):
        self.assertTrue(charset.is_in_filenamecharset(self.charset))
        self.assertTrue(charset.is_in_filenamecharset(self.hostnamecharset))
        self.assertTrue(charset.is_in_filenamecharset(self.filenamecharset))

    def test_is_in_hostnamecharset(self):
        self.assertFalse(charset.is_in_hostnamecharset(self.charset))
        self.assertTrue(charset.is_in_hostnamecharset(self.hostnamecharset))
        self.assertFalse(charset.is_in_hostnamecharset(self.filenamecharset))

if __name__ == "__main__":
    unittest.main()

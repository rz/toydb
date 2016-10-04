import os
import unittest
import struct

import toydb


TEST_DB_FILENAME = '__test.db'


class FileStorageTest(unittest.TestCase):
    def delete_db_file(self):
        try:
            os.remove(TEST_DB_FILENAME)
        except FileNotFoundError:
            pass

    def bytes_to_integer(self, fmt, bs):
        return struct.unpack(fmt, bs)[0]

    def setUp(self):
        self.delete_db_file()

    def test_init_does_not_delete(self):
        # create and set up a file
        txt = 'testing text'
        with open(TEST_DB_FILENAME, 'w+') as f:
            f.write(txt)

        fs = toydb.FileStorage(TEST_DB_FILENAME)
        with open(TEST_DB_FILENAME, 'r') as f:
            self.assertEqual(f.read(len(txt)), txt)
        fs.close()
        with open(TEST_DB_FILENAME, 'r') as f:
            self.assertEqual(f.read(len(txt)), txt)

    def test_init_creates(self):
        fs = toydb.FileStorage(TEST_DB_FILENAME)
        self.assertTrue(os.path.isfile(TEST_DB_FILENAME))
        fs.close()
        self.assertTrue(os.path.isfile(TEST_DB_FILENAME))

    def test_init_pages(self):
        fs = toydb.FileStorage(TEST_DB_FILENAME)
        with open(TEST_DB_FILENAME, 'rb') as f:
            for _ in range(2):  # there are 2 pages, so do it twice
                bs = f.read(fs.INTEGER_LENGTH)
                page_size = self.bytes_to_integer(fs.INTEGER_FORMAT, bs)
                self.assertEqual(page_size, fs.PAGE_SIZE)
                bs = f.read(fs.PAGE_SIZE - fs.INTEGER_LENGTH)
                self.assertTrue(all(b == 0 for b in bs))
        fs.close()

    def test_init_does_not_overwrite(self):
        # this is a dump of a db file that is correct and has 2 keys: key1 and key2 and 2 values: val1 and val2
        bs = b'\x00\x80\x00\x06\x00\x00\x00\x00\x00\x06key1\x00\x06key2\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x06val1\x00\x06val2\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        with open(TEST_DB_FILENAME, 'bx+') as f:
            f.write(bs)

        fs = toydb.FileStorage(TEST_DB_FILENAME)
        with open(TEST_DB_FILENAME, 'br') as f:
            self.assertEqual(f.read(), bs)
        fs.close()
        with open(TEST_DB_FILENAME, 'br') as f:
            self.assertEqual(f.read(), bs)

    def test_key_read_write(self):
        fs = toydb.FileStorage(TEST_DB_FILENAME)
        ak1 = fs.write_key(b'key1')
        ak2 = fs.write_key(b'key2')
        av1 = fs.write_value(b'value1')
        av2 = fs.write_value(b'value2')

        self.assertEqual(fs.read(ak1), b'key1')
        self.assertEqual(fs.read(ak2), b'key2')
        self.assertEqual(fs.read(av1), b'value1')
        self.assertEqual(fs.read(av2), b'value2')

        fs.close()

    def tearDown(self):
        self.delete_db_file()


if __name__ == '__main__':
    if os.path.isfile(TEST_DB_FILENAME):
        raise RuntimeError('A file named %s exists. The test suite uses this file as a temporary file. Please remove it or change the value of TEST_DB_FILENAME in tests.py')
    unittest.main()

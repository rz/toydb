import os
import unittest
from unittest import mock
import struct

import toydb


TEST_DB_FILENAME = '__test.db'


def delete_db_file():
    try:
        os.remove(TEST_DB_FILENAME)
    except FileNotFoundError:
        pass


class FileStorageTest(unittest.TestCase):
    def bytes_to_integer(self, fmt, bs):
        return struct.unpack(fmt, bs)[0]

    def setUp(self):
        delete_db_file()

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

    @mock.patch('toydb.FileStorage.PAGE_SIZE', 128)
    @mock.patch('toydb.FileStorage.INTEGER_FORMAT', '!H')
    @mock.patch('toydb.FileStorage.INTEGER_LENGTH', 2)
    def test_init_does_not_overwrite(self):
        # this is a dump of a db file that is correct and has 2 keys: key1 and key2 and 2 values: val1 and val2
        # it assumes PAGE_SIZE=128, INTEGER_FORMAT='!H' and INTEGER_LENGTH=2
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
        delete_db_file()


class DBTest(unittest.TestCase):
    def setUp(self):
        delete_db_file()
        self.db = toydb.DB(TEST_DB_FILENAME)

    def test_set_get(self):
        key, val = 'key', 'val'
        self.db.set(key, val)
        self.assertEqual(self.db.get(key), val)

    def test_update(self):
        key, orig_val, final_val = 'key', 'val1', 'val2'
        self.db.set(key, orig_val)
        self.db.set(key, final_val)
        self.assertEqual(self.db.get(key), final_val)

    def test_pop(self):
        key, val = 'key', 'val'
        self.db.set(key, val)
        self.assertEqual(self.db.pop(key), val)
        with self.assertRaises(KeyError):
            self.db.get(key)
        with self.assertRaises(KeyError):
            self.db.pop(key)

    def test_multivalue(self):
        keys = ['f', 'fo', 'foo', 'fool', 'fools', 'fooled']
        for k in keys:
            self.db.set(k, len(k))

        self.db.set('foo', 33)
        self.db.set('f', 11)
        self.db.set('fools', 55)
        self.db.set('foo', 333)
        self.db.set('fools', 555)

        self.db.pop('fools')
        with self.assertRaises(KeyError):
            self.db.get('fools')
        self.db.set('fools', 5555)

        self.assertEqual(self.db.get('f'), 11)
        self.assertEqual(self.db.get('fo'), 2)
        self.assertEqual(self.db.get('foo'), 333)
        self.assertEqual(self.db.get('fool'), 4)
        self.assertEqual(self.db.get('fools'), 5555)
        self.assertEqual(self.db.get('fooled'), 6)

    def tearDown(self):
        self.db.close()
        delete_db_file()



if __name__ == '__main__':
    if os.path.isfile(TEST_DB_FILENAME):
        raise RuntimeError('A file named %s exists. The test suite uses this file. Please remove it or change the value of TEST_DB_FILENAME in tests.py')
    unittest.main()

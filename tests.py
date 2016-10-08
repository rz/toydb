import os
import unittest
from unittest import mock
import struct

import toydb


TEST_DB_BASENAME = '__test.db'
TEST_DB_FILENAMES = [TEST_DB_BASENAME, TEST_DB_BASENAME + '.keys', TEST_DB_BASENAME + '.values']

def delete_db_files():
    for name in TEST_DB_FILENAMES:
        try:
            os.remove(name)
        except FileNotFoundError:
            pass


class FileStorageTest(unittest.TestCase):
    def bytes_to_integer(self, fmt, bs):
        return struct.unpack(fmt, bs)[0]

    def setUp(self):
        delete_db_files()

    def test_init_does_not_delete(self):
        # create and set up a file
        txt = 'testing text'
        with open(TEST_DB_BASENAME, 'w+') as f:
            f.write(txt)

        fs = toydb.FileStorage(TEST_DB_BASENAME)
        with open(TEST_DB_BASENAME, 'r') as f:
            self.assertEqual(f.read(len(txt)), txt)
        fs.close()
        with open(TEST_DB_BASENAME, 'r') as f:
            self.assertEqual(f.read(len(txt)), txt)

    def test_init_creates(self):
        fs = toydb.FileStorage(TEST_DB_BASENAME)
        self.assertTrue(os.path.isfile(TEST_DB_BASENAME))
        fs.close()
        self.assertTrue(os.path.isfile(TEST_DB_BASENAME))

    @mock.patch('toydb.FileStorage.INTEGER_FORMAT', '!H')
    @mock.patch('toydb.FileStorage.INTEGER_LENGTH', 2)
    def test_init_does_not_overwrite(self):
        # this is a dump of a db file that is correct and has 2 keys: key1 and key2 and 2 values: val1 and val2
        # it assumes PAGE_SIZE=128, INTEGER_FORMAT='!H' and INTEGER_LENGTH=2
        bs = b'\x00\x80\x00\x06\x00\x00\x00\x00\x00\x06key1\x00\x06key2\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x06val1\x00\x06val2\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        with open(TEST_DB_BASENAME, 'bx+') as f:
            f.write(bs)

        fs = toydb.FileStorage(TEST_DB_BASENAME)
        with open(TEST_DB_BASENAME, 'br') as f:
            self.assertEqual(f.read(), bs)
        fs.close()
        with open(TEST_DB_BASENAME, 'br') as f:
            self.assertEqual(f.read(), bs)

    def tearDown(self):
        delete_db_files()


class DBTest(unittest.TestCase):
    def setUp(self):
        delete_db_files()
        self.db = toydb.DB(TEST_DB_BASENAME)

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

    def test_obj_key_ovj_value(self):
        key = [0, 1, 2]
        val = {1:10, 2:20}
        self.db.set(key, val)
        retrieved_val = self.db.get(key)
        self.assertEqual(val, retrieved_val)

    def tearDown(self):
        self.db.close()
        delete_db_files()


import sys
if __name__ == '__main__':
    for name in TEST_DB_FILENAMES:
        if os.path.isfile(name):
            sys.exit('A file named %s exists. The test suite uses this file. In order to run the tests please remove it or change the value of TEST_DB_BASENAME in tests.py' % name)
    unittest.main()

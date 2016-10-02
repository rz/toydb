import unittest

import toydb

class ToyDBTest(unittest.TestCase):
    def setUp(self):
        self.db = toydb.DB()

    def test_end_to_end(self):
        with self.assertRaises(KeyError):
            self.db.get('anything')
        with self.assertRaises(KeyError):
            self.db.pop('anything')

        self.db.set('foo', 3)
        self.db.commit()
        self.assertEqual(self.db.get('foo'), 3)
        with self.assertRaises(KeyError):
            self.db.get('somethingelse')
        with self.assertRaises(KeyError):
            self.db.pop('somethingelse')

        self.db.pop('foo')
        self.db.commit()
        with self.assertRaises(KeyError):
            self.db.get('foo')

    def test_logical_insert(self):
        # we'll test the logical layer by doing calls to the db api and
        # testing that we get the expected data in the storage.
        # it'll be handy to have a reference to the raw data of the stoarge
        data = self.db.storage._data

        self.assertEqual(data, [0])

        self.db.set('foo', 3)

        # we shouldn't see anything in the storage until we commit
        self.assertEqual(data, [0])

        self.db.commit()

        # the storage now looks like: [2, 3, <LLNode key:foo value:<ValueRef addr:1 v:3> next:<LLNodeRef addr:0>]
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0], 2)
        self.assertEqual(data[1], 3)
        self.assertTrue(isinstance(data[2], toydb.LLNode))
        self.assertEqual(data[2].key, 'foo')
        self.assertEqual(data[2].value_ref.address, 1)
        self.assertEqual(data[2].next_ref.address, 0)

        self.db.set('fool', 4)
        self.db.commit()
        # since we implement an immutable linked-list and we inserted at the end
        # we should have the first list in the storage, along with a 2nd copy
        # which contains the new node and updated root address:
        # [
        #   0: 5
        #   1: 3
        #   2: <LLNode key:foo value:<ValueRef addr:1 v:3> next:<LLNodeRef addr:0>>
        #   3: 4
        #   4: <LLNode key:fool value:<ValueRef addr:3 v:4> next:<LLNodeRef addr:0>>
        #   5: <LLNode key:foo value:<ValueRef addr:1 v:3> next:<LLNodeRef addr:4>>
        # ]
        self.assertEqual(len(data), 6)
        self.assertEqual(data[0], 5)

        self.assertEqual(data[5].key, 'foo')
        self.assertEqual(data[5].value_ref.address, 1)
        self.assertEqual(data[5].next_ref.address, 4)

        self.assertEqual(data[4].key, 'fool')
        self.assertEqual(data[4].value_ref.address, 3)
        self.assertEqual(data[4].next_ref.address, 0)

        self.assertEqual(data[1], 3)
        self.assertEqual(data[3], 4)

        self.assertEqual(data[2].key, 'foo')
        self.assertEqual(data[2].value_ref.address, 1)
        self.assertEqual(data[2].next_ref.address, 0)

        self.db.set('fools', 5)
        self.db.commit()
        self.assertEqual(len(data), 10)

    def tearDown(self):
        self.db = None


if __name__ == '__main__':
    unittest.main()

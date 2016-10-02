### A simple DB using a single file for storage
# this DB supports 3 commands
# set <key> <value> -> None
# get <key> -> value
# pop <key> -> value, deletes the key
# all the keys must be comparable python types
import struct




### This is the physical layer
# the file format is as follows:
#   binary, uses pickle to serialize/de-serialize
#   2 kinds of "pages" in the file: key data, value data
#   a key data page is as follows:
#     all ints are unsigned long long
#     <1 byte: b'k'>,<page_size: int, the of bytes in the page including the meta data ie empty page has 9 = 1 for the 'k' byte + 8 for the page size>
#     key-data is as follows:
#       <legnth of key data: long int><pickled key data object>
#       a key data object is a dict with keys: key, value address, length of value
#     a value data page is as follows:
#     <1 byte: b'v'>,<page_size, as above>
#       <pickled values>

# empty file: k9v9
# empty DB, initialized with pages of size 2048: k2048<2039 0 bytes>v2048<2039 0 bytes>
# insert key "foo" with value 3: k2048<size of key data for key=foo><pickled key data><0 bytes to fill up the page>v2048<pickled value:3><0 bytes to fill up the page>

class FileStorage(object):
    pass



### This the logical layer
# tracks of where objects are/sould be given the FileStorage specified
class Logical(object):
    def _get():
        pass
    def _insert():
        pass
    def _pop():
        pass
    def _follow():
        pass

    def get(self, key):
        pass
    def set(self, key, value):
        pass
    def pop(self, key):
        pass




### The API to the database ie what a python user would use.
# if the database had a query processor it would interact with this API
class DB(object):
    def __init__(self, dbname):
        self.storage = FileStorage(dbname)
        self.ds = Logical(self.storage)

    def commit(self):
       self.ds.commit()

    def get(self, key):
        return self.ds.get(key)

    def set(self, key, value):
        return self.ds.set(key, value)

    def pop(self, key):
        return self.ds.pop(key)


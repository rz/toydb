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
#   all ints are unsigned long long
#   file has 2 pages of a given size. the first value in each page is the size of the page (always 2048 for now)
#   a key-data page is as follows:
#     <int page_size><key data><key data>...<key data>\x00...\x00
#     key-data is as follows:
#       <legnth of key data: long int><pickled key data object>
#       a key data object is a dict with keys: key, value address
#   a value-data page is as follows:
#     <int page_size><value data><value data>...<value data>\x00...\x00
#       value data is as follows:
#         <length of value data: long int><pickled value data>

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


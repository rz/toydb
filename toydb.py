### A simple DB using a single file for storage
# this DB supports 3 commands
# set <key> <value> -> None
# get <key> -> value
# pop <key> -> value, deletes the key
# all the keys must be comparable python types
import pickle
import os
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
    # to simplify debugging/viewing with xdd, make our pages very small for now
    # PAGE_SIZE = 2048
    PAGE_SIZE = 512

    # to simplify debugging / viewing with xdd, make our ints 2 bytes for now
    # INTEGER_FORMAT = '!Q'
    # INTEGER_LENGTH = 8  # in bytes, this is implied by INTEGER_FORMAT, see https://docs.python.org/3.1/library/struct.html#format-characters
    INTEGER_FORMAT = '!H'
    INTEGER_LENGTH = 2  # in bytes, this is implied by INTEGER_FORMAT, see https://docs.python.org/3.1/library/struct.html#format-characters

    # initialization methods
    def __init__(self, dbname):
        try:
            f = open(dbname, 'bx+')  # x mode raises exception when the file exists
        except FileExistsError:
            f = open(dbname, 'r+b')
        self._f = f
        self._initialize_pages()

    def _initialize_pages(self):
        end_address = self._seek_end()
        needed_size = 2*self.PAGE_SIZE
        if end_address < needed_size:
            self._write(b'\x00' * (needed_size - end_address))
        if end_address == 0:
            # the file was originally empty
            self._seek(0)
            self._write_integer(self.PAGE_SIZE)
            self._seek(self.PAGE_SIZE)
            self._write_integer(self.PAGE_SIZE)

    # methods that interact with the associated file ie self._f
    def _tell(self):
        "Wrapper around File.tell() for consistency."""
        return self._f.tell()

    def _seek(self, offset, whence=0):
        """Wrapper around File.seek() for consistency."""
        return self._f.seek(offset, whence)

    def _seek_start(self):
        """Moves the stream position to the beginning of the file and returns that address."""
        return self._f.seek(0, os.SEEK_SET)

    def _seek_end(self):
        """Moves the stream position to the end of file and returns that address."""
        return self._f.seek(0, os.SEEK_END)

    def _read(self, n):
        """Wrapper around File.read() for consistency."""
        return self._f.read(n)

    def _write(self, bs):
        """Wrapper around File.write() for consistency."""
        addr = self._f.write(bs)
        self._f.flush()
        return addr

    # utility methods
    def _write_integer(self, n):
        """Write the given integer to the file at the current stream position."""
        bs = struct.pack(self.INTEGER_FORMAT, n)  # convert the integer to bytes per our integer format
        self._write(bs)

    def _read_integer(self):
        """Read an integer from the file at the current stream position."""
        bs = self._read(self.INTEGER_LENGTH)
        return struct.unpack(self.INTEGER_FORMAT, bs)[0]

    def _read_integer_and_rewind(self):
        """
        Reads an integer from the file at the current stream position and
        leaves the current stream position unchanged.
        """
        n = self._read_integer()
        self._seek(-self.INTEGER_LENGTH, os.SEEK_CUR)
        return n

    def _write_formatted(self, data):
        """
        Writes an int with the length of the data followed by the data at the
        current stream position.
        """
        addr = self._tell()
        length = len(data) + self.INTEGER_LENGTH
        self._write_integer(length)
        self._write(data)
        return addr

    def _seek_formatted_data_end(self, start_at):
        """
        Moves the current stream position to the end of the data after start_at.

        Assumes that start_at is the starting position of formatted data ie that
        reading that address yields an integer with the length of the data that
        follows.
        """
        self._seek(start_at)
        length = self._read_integer_and_rewind()
        while length > 0:
            self._seek(length, os.SEEK_CUR)
            length = self._read_integer_and_rewind()

    # external api
    def write_key(self, key_data):
        """
        Appends the key-data to the key page.

        key_data should be an iterable of bytes.
        """
        # find the end of the existing key-data:
        self._seek_start()
        self._seek_formatted_data_end(0 + self.INTEGER_LENGTH)  # key data is the first page and the first thing in the page is an integer for the page size
        return self._write_formatted(key_data)

    def write_value(self, value_data):
        """
        Appends the value-data to the key page.

        value_data should be an iterable of bytes.
        """
        self._seek_start()
        self._seek_formatted_data_end(self.PAGE_SIZE + self.INTEGER_LENGTH) # value-data is the 2nd page and the first thing in the page is an integer for the page size
        return self._write_formatted(value_data)

    def read(self, address):
        self._seek(address)
        data_length = self._read_integer()
        data = self._read(data_length - self.INTEGER_LENGTH)
        return data

    def read_keys(self):
        ret = []
        self._seek(0 + self.INTEGER_LENGTH)  # beginning of keys in the key page
        length = self._read_integer_and_rewind()
        while length > 0:
            address = self._tell()
            data = self.read(address)
            ret.append(data)
            length = self._read_integer_and_rewind()
        return ret

    def close(self):
        self._f.close()

    @property
    def is_closed(self):
        return self._f.closed

    @property
    def is_open(self):
        return not self.is_closed





### This the logical layer
# tracks of where objects are/sould be given the FileStorage specified
#
# we'll only append that means that to update a key we'll simply insert another
# copy and ditto for values.
# to denote a key that has been deleted, we'll use a special address reference: None
# this way deleting is just inserting a key with None for a value reference.
class Logical(object):
    def __init__(self, storage):
        self._storage = storage

    def _read_keys(self):
        return [pickle.loads(key_data) for key_data in self._storage.read_keys()]

    def _get(self, key):
        # see _insert() for key_data format, it is a tuple (key, value_address)
        # note that we do updates by simply inserting another copy of the key, so, to retrieve the key
        # we have to look at the last one
        keys = self._read_keys()
        value_data = None
        for k, value_address in keys:
            if k == key:
                if value_address is None:
                    value_data = None
                else:
                    value_data = self._storage.read(value_address)
        if value_data is None:
            raise KeyError
        return pickle.loads(value_data)

    def _insert(self, key, value, for_deletion=False):
        # we always insert the key
        if not for_deletion:
            value_data = pickle.dumps(value)
            value_address = self._storage.write_value(value_data)
        else:
            value_address = None
        key_tuple = (key, value_address)
        key_data = pickle.dumps(key_tuple)
        key_address = self._storage.write_key(key_data)

    def _pop(self, key):
        # see _insert() for key_data format, it is a tuple (key, value_address)
        keys = self._read_keys()
        value_data = None
        for k, value_address in keys:
            if k == key:
                if value_address is None:
                    value_data = None
                else:
                    value_data = self._storage.read(value_address)
        if value_data is None:
            raise KeyError
        # this is the actual deletion
        self._insert(key, None, for_deletion=True)
        return pickle.loads(value_data)

    def get(self, key):
        return self._get(key)

    def set(self, key, value):
        return self._insert(key, value)

    def pop(self, key):
        return self._pop(key)




### The API to the database ie what a python user would use.
# if the database had a query processor it would interact with this API
class DB(object):
    def __init__(self, dbname):
        self.storage = FileStorage(dbname)
        self.ds = Logical(self.storage)

    def get(self, key):
        return self.ds.get(key)

    def set(self, key, value):
        return self.ds.set(key, value)

    def pop(self, key):
        return self.ds.pop(key)

    def close(self):
        return self.storage.close()


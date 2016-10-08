### A simple DB using a single file for storage
# this DB supports 3 commands
# set <key> <value> -> None
# get <key> -> value
# pop <key> -> value, deletes the key
# all the keys must be comparable python types
import ast
import pickle
import os
import struct
import sys




### This is the physical layer
# the file format is as follows:
#   binary
#   all ints are unsigned long long
#   the data format in the file is as follows <int lenght of data><binary data>
#   the external API of the storage hides the details of the format, reads the data at an address without the caller having to concern itself with its length
#   the external API only has an append method for writing, soft enforcing of write-only
class FileStorage(object):
    # to simplify debugging / viewing with xxd, make our ints 2 bytes for now
    # INTEGER_FORMAT = '!Q'
    # INTEGER_LENGTH = 8  # in bytes, this is implied by INTEGER_FORMAT, see https://docs.python.org/3.1/library/struct.html#format-characters
    INTEGER_FORMAT = '!H'
    INTEGER_LENGTH = 2  # in bytes, this is implied by INTEGER_FORMAT, see https://docs.python.org/3.1/library/struct.html#format-characters

    # initialization methods
    def __init__(self, filename):
        try:
            f = open(filename, 'bx+')  # x mode raises exception when the file exists
        except FileExistsError:
            f = open(filename, 'r+b')
        self._f = f

        # initialize the file to ensure there are zero bytes at the end
        if self._is_empty():
            self._zero_end(128)
        else:
            self._seek(-1, os.SEEK_END)
            last_char = self._read(1)
            if last_char != b'\x00':
                self._zero_end(128)


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

    def _is_empty(self):
        end_address = self._seek_end()
        return end_address == 0

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

    def _zero_end(self, n=1):
        """Writes zero bytes at the end of the file."""
        self._seek_end()
        self._write(b'\x00'* self.INTEGER_LENGTH * n)

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

    def _seek_formatted_data_end(self, start_at=0):
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
    def append(self, data):
        self._seek_formatted_data_end()
        self._last_address = self._write_formatted(data)
        self._zero_end()
        return self._last_address

    def read(self, address):
        """
        Reads the data at the given address. Returns None if the address is past the end of the data on file.
        """
        self._seek(address)
        data_length = self._read_integer()
        if data_length == 0:
            return None
        data = self._read(data_length - self.INTEGER_LENGTH)
        return data

    def next_address(self, address):
        """
        Returns the address of the first piece of data after address.
        """
        self._seek_start()
        length = self._read_integer_and_rewind()
        read_address = 0
        while read_address <= address and length > 0:
            length = self._read_integer_and_rewind()
            self._seek(length, os.SEEK_CUR)
            read_address += length
        if read_address == address:
            return None
        return read_address

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
# we'll keep 2 files / storage objects, one for keys and another for values.
# we'll use pickle to serialize/de-serialize data to/from binary
class Logical(object):
    def __init__(self, dbname):
        self._key_storage = FileStorage(dbname + '.keys')
        self._value_storage = FileStorage(dbname + '.values')

    def _read_keys(self):
        keys = []
        address = 0
        while address is not None:
            key_data = self._key_storage.read(address)
            if key_data is not None:
                key = pickle.loads(key_data)
                keys.append(key)
            address = self._key_storage.next_address(address)
        return keys

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
                    value_data = self._value_storage.read(value_address)
        if value_data is None:
            raise KeyError
        return pickle.loads(value_data)

    def _insert(self, key, value, for_deletion=False):
        # we always insert the key
        if not for_deletion:
            value_data = pickle.dumps(value)
            value_address = self._value_storage.append(value_data)
        else:
            value_address = None
        key_tuple = (key, value_address)
        key_data = pickle.dumps(key_tuple)
        key_address = self._key_storage.append(key_data)

    def _pop(self, key):
        # see _insert() for key_data format, it is a tuple (key, value_address)
        keys = self._read_keys()
        value_data = None
        for k, value_address in keys:
            if k == key:
                if value_address is None:
                    value_data = None
                else:
                    value_data = self._value_storage.read(value_address)
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

    def close_storage(self):
        self._key_storage.close()
        self._value_storage.close()



### The API to the database ie what a python user would use.
# the query processor interacts with this API
class DB(object):
    def __init__(self, dbname):
        self.ds = Logical(dbname)

    def get(self, key):
        return self.ds.get(key)

    def set(self, key, value):
        return self.ds.set(key, value)

    def pop(self, key):
        return self.ds.pop(key)

    def close(self):
        return self.ds.close_storage()




### The QueryProcessor
# this layer validates user input, makes API calls to the database
# for now, it returns output strings
# our query language has 3 comands:
#   set key=value -> returns nothing
#   get key -> returns the value associated with key
#   pop key -> deletes the key and returns the value formerly associated with key
#   keys and values can be any native python object
#   the user-input key and values string will be evaluated with literal_eval() see: https://docs.python.org/3/library/ast.html#ast.literal_eval
#   and if that fails they'll be treated as strings
class QueryProcessor(object):
    def __init__(self, db):
        self._db = db

    def _validate_cmd(self, s):
        return s in ['set', 'get', 'pop']

    def _to_python(self, s):
        try:
            pyval = ast.literal_eval(s)
        except (SyntaxError, ValueError):
            pyval = str(s)
        return pyval

    def _format(self, v):
        return '<%s>: %s' % (type(v).__name__, v)

    def _handle_set(self, set_args):
        if set_args.count('=') != 1:
            return 'Invalid set syntax.'
        key_str, value_str = set_args.split('=')
        key = self._to_python(key_str)
        val = self._to_python(value_str)
        self._db.set(key, val)
        return '  set %s: %s' % (key, val)

    def _handle_get(self, get_args):
        key = self._to_python(get_args)
        try:
            val = self._db.get(key)
        except KeyError:
            return 'Not found: %s' % get_args
        return self._format(val)

    def _handle_pop(self, pop_args):
        key = self._to_python(pop_args)
        try:
            val = self._db.pop(key)
        except KeyError:
            return 'Not found: %s' % pop_args
        return self._format(val)

    def execute(self, user_input):
        """
        Accepts a string as provided by the user and returns the output that should be displayed.

        The return value is a string with the result of the query or an error message.
        """
        cmd, *pieces = user_input.split()
        if not self._validate_cmd(cmd):
            return 'Invalid query. %s is not a toydb command.' % cmd
        cmd_args = ''.join(pieces)
        if cmd == 'set':
            return self._handle_set(cmd_args)
        elif cmd == 'get':
            return self._handle_get(cmd_args)
        elif cmd == 'pop':
            return self._handle_pop(cmd_args)





### The database client.
def print_usage():
    print('Usage: python toydb <path to the database file>.')
    print('The file will be created if it does not exist.')


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) != 1:
        print_usage()
        sys.exit()
    dbname = args[0]
    db = DB(dbname)
    qp = QueryProcessor(db)
    print('Use Ctrl-D to exit.')
    while True:
        try:
            user_input = input('[toydb]=> ')
            output = qp.execute(user_input)
            print(output)
        except (EOFError, KeyboardInterrupt):
            sys.exit()


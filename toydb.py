### A simple in-memory DB using a linked list.
# this DB supports three commands:
# set <key> <value> -> None
# get <key> -> value
# pop <key> -> value, deletes key
# all the keys must be comparable python-types




### This is the physical storage layer
# this storage works in-memory only
# it is a way to simplify what needs to be understood before implementing a file storage
# the rules are:
# the list can contain the following:
#     a value the user wants to store, as a native python value
#     an integer to serve as an address (ie it gets interpreted by our code as an index into the list)
#     an LLNode (see below for its definition)
# there is no deleting stuff
# the first element of the list is always the index of the "root" of the data structure used
# ie the latest address at which the data starts
class MemoryStorage(object):
    def __init__(self):
        self._data = [0]

    @property
    def is_open(self):
        return True

    def open(self):
        pass

    def close(self):
        pass

    def write(self, data):
        self._data.append(data)
        object_address = len(self._data) - 1
        return object_address

    def read(self, address):
        return self._data[address]

    def commit_root_address(self, address):
        self._data[0] = address

    def get_root_address(self):
        return self._data[0]

    def _peak(self):
        elements_s = '\n'.join('  %s: %s' % (i, elem) for i, elem in enumerate(self._data))
        return '[\n%s\n]' % elements_s





### This is the logical layer
# it is an implementation of a linked list that stores its data in a
# MemoryStorage object as specified above.
# a few things here:
#   - a utility class to handle referencing values in storage
#   - a related utility class to handle referencing nodes of the linked list
#   - a concrete implementation of the logical layer using a linked list
class ValueRef(object):
    def __init__(self, value=None, address=0):
        self.value = value
        self.address = address

    def __repr__(self):
        return '<ValueRef addr:%s v:%s>' % (self.address, self.value)

    def prepare_to_store(self, storage):
        pass

    def get(self, storage):
        if self.address and self.value is None:
            self.value = storage.read(self.address)
        return self.value

    def store(self, storage):
        if self.value is not None and not self.address:
            self.prepare_to_store(storage)
            self.address = storage.write(self.value)


class LLNodeRef(ValueRef):
    def __repr__(self):
        return '<LLNodeRef addr:%s>' % self.address

    def prepare_to_store(self, storage):
        if self.value:
            self.value.store_refs(storage)


class LLNode(object):
    def __init__(self, key, value_ref, next_ref):
        self.key = key
        self.value_ref = value_ref
        self.next_ref = next_ref

    def __repr__(self):
        mem_addr = hex(id(self))
        return '<LLNode %s key:%s value:%s next:%s>' % (mem_addr, self.key, self.value_ref, self.next_ref)

    def store_refs(self, storage):
        self.value_ref.store(storage)
        self.next_ref.store(storage)


class LinkedList(object):
    def __init__(self, storage):
        self._storage = storage
        self._refresh_root_ref()

    def commit(self):
        self._root_ref.store(self._storage)
        self._storage.commit_root_address(self._root_ref.address)

    def get(self, key):
        return self._get(self._follow(self._root_ref), key)

    def set(self, key, value):
        self._root_ref = self._insert(self._follow(self._root_ref), key, ValueRef(value))

    def pop(self, key):
        self._root_ref = self._pop(self._follow(self._root_ref), key)

    def _refresh_root_ref(self):
        self._root_ref = ValueRef(address=self._storage.get_root_address())

    def _follow(self, ref):
        return ref.get(self._storage)

    def _get(self, node, key):
        while node is not None:
            if key == node.key:
                return self._follow(node.value_ref)
            else:
                node = self._follow(node.next_ref)
        raise KeyError

    def _insert(self, node, key, value_ref):
        if node is None:
            # inserting on an empty list
            new_node = LLNode(key, value_ref, next_ref=LLNodeRef())
            return LLNodeRef(value=new_node)

        if key == node.key:
            new_newdata_node = LLNode(key, value_ref, next_ref=node.next_ref) # notice that this has the new value and points to the existing next node
            return LLNodeRef(new_newdata_node)
        else:
            if node.next_ref.value is None: # we are at the end
                # make the new node ie the one with the new data
                new_newdata_node = LLNode(key, value_ref, next_ref=LLNodeRef())
                # make a copy of the current node
                new_node = LLNode(node.key, node.value_ref, next_ref=LLNodeRef(new_newdata_node))
                return LLNodeRef(new_node)
            else:
                # make a copy of the present node
                next_node = self._follow(node.next_ref)
                # recurse
                new_node = LLNode(node.key, node.value_ref, next_ref=self._insert(next_node, key, value_ref))
                return LLNodeRef(new_node)

    def _pop(self, node, key):
        if node is None:
            raise KeyError

        next_node = self._follow(node.next_ref)
        if next_node is None:
            if node.key == key:
                # there is only 1 node and it is the one we need to get rid of
                return LLNodeRef()
            else:
                # we are at the end and didn't find the key
                raise KeyError

        if key == next_node.key:
            # the node we need to delete is next, so first get the node that this node needs to point to after the deletion
            new_node = LLNode(node.key, node.value_ref, next_ref=next_node.next_ref)
            return LLNodeRef(new_node)
        else:
            next_node = self._follow(node.next_ref)
            # make a copy of the present node and recurse
            new_node = LLNode(node.key, node.value_ref, next_ref=self._pop(next_node, key))
            return LLNodeRef(new_node)

    def _peak(self):
        s = ''
        node = self._follow(self._root_ref)
        while node is not None:
            s += '[k:%s v:%s]-->' % (node.key, node.value_ref.value)
            node = self._follow(node.next_ref)
        return s





### The API to the database ie what a python user would use.
# if the database had a query processor it would interact with this API
class DB(object):
    def __init__(self, dbname):
        self.storage = MemoryStorage()
        self.ds = LinkedList(self.storage)

    def commit(self):
        self.ds.commit()

    def get(self, key):
        return self.ds.get(key)

    def set(self, key, value):
        return self.ds.set(key, value)

    def pop(self, key):
        return self.ds.pop(key)



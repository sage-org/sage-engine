# import logging

from typing import List, Tuple, Optional, Union

from sage.query_engine.types import Mappings


class Node():
    """
    :description: Class for each individual node of the red black tree
    """

    def __init__(self, key, value=None):
        self.key = key
        self.value = value
        self.parent = None
        self.left = None
        self.right = None
        self.color = 1

    def __repr__(self):
        return "<key = "+str(self.key)+", value = "+str(self.value)+">"


class OrderedDict():
    """
    :description: Class for the ordered dictionary
    :data_structure: The ordered dict implemented as a red black tree
    :restriction: All keys must be of the same data type
    """

    def __init__(self, default=None):
        self.TNULL = Node(0, 0)  # TNULL refers to the NIL node
        self.TNULL.color = 0
        self.TNULL.left = None
        self.TNULL.right = None
        self.root = self.TNULL
        self.length = 0
        self.curr = None  # needed for iteration
        self.stack = []  # needed for iteration
        self.default = default  # the default value to return if key isn"t present

    # update the value for a key, or add a new key-value pair
    def __setitem__(self, key, value):
        self.insert(key, value)

    # returns the value corresponding to a key (if key is present)
    def __getitem__(self, key):
        # if nil, return key error, else return value
        node = self.search(self.get_root(), key)
        if node == self.TNULL:
            if self.default is not None:
                return self.default
            message = "Key Error! "+str(key)+" does not exist in the dictionary"
            raise Exception(message)
        else:
            return node.value

    # checks if dict contains the given key (the in operator)
    def __contains__(self, key):
        return False if self.search(self.root, key) == self.TNULL else True

    # initialise iteration
    def __iter__(self):
        self.curr = self.root
        self.stack = []
        return self

    # returns the next element on every call
    def __next__(self):
        while self.curr != self.TNULL or self.stack:
            if self.curr != self.TNULL:
                self.stack.append(self.curr)
                self.curr = self.curr.left
            elif self.stack:
                self.curr = self.stack.pop()
                temp = self.curr
                self.curr = self.curr.right
                return temp
        else:
            raise StopIteration

    # iterates the dict in reverse order of keys
    def reverse_iterate(self):
        self.curr = self.root
        self.stack = []
        while self.curr != self.TNULL or self.stack:
            if self.curr != self.TNULL:
                self.stack.append(self.curr)
                self.curr = self.curr.right
            elif self.stack:
                self.curr = self.stack.pop()
                yield self.curr
                self.curr = self.curr.left

    # returns list of all key values in dict in formatted manner
    def __repr__(self):
        res = ["{"]
        for x in self:
            res.append(" "+str(x.key)+": "+str(x.value)+",")
        if len(res) == 1:
            return "{ }"
        res[-1] = res[-1][:len(res[-1])-1]+" }"
        return "".join(res)
        return res[:len(res)-1]+" }" if len(res) > 1 else "{ }"

    # returns length of dict
    def __len__(self):
        return self.length

    # Search the tree
    def search(self, node, key):
        if node == self.TNULL or key == node.key:
            return node

        if key < node.key:
            return self.search(node.left, key)
        return self.search(node.right, key)

    # returns the next greater key present in dict corresponding to a given value
    def upper_bound(self, n):
        """
        :param name: n - key for which upper bound is to be searched
        :param type: any
        :return: key value pair
        """
        for x in self:
            if x.key > n:
                self.stack = []
                return x
        return None

    # delete all elements of bst
    def clear(self):
        """
        :parameters: None
        :function: clears the dictionary
        :returns: None
        """
        for node in self:
            del node
        self.root = self.TNULL

    # returns the next smaller key present in dict corresponding to a given value
    def lower_bound(self, n):
        """
        :param name: n - key for which lower bound is to be searched
        :param type: any
        :return: key value pair
        """
        for x in self.reverse_iterate():
            if x.key < n:
                self.stack = []
                return x
        return None

    # Balancing the tree after deletion
    def delete_fix(self, x):
        """
        :function: Rebalance tree after delete operation
        """
        while x != self.root and x.color == 0:
            if x == x.parent.left:
                s = x.parent.right
                if s.color == 1:
                    s.color = 0
                    x.parent.color = 1
                    self.left_rotate(x.parent)
                    s = x.parent.right

                if s.left.color == 0 and s.right.color == 0:
                    s.color = 1
                    x = x.parent
                else:
                    if s.right.color == 0:
                        s.left.color = 0
                        s.color = 1
                        self.right_rotate(s)
                        s = x.parent.right

                    s.color = x.parent.color
                    x.parent.color = 0
                    s.right.color = 0
                    self.left_rotate(x.parent)
                    x = self.root
            else:
                s = x.parent.left
                if s.color == 1:
                    s.color = 0
                    x.parent.color = 1
                    self.right_rotate(x.parent)
                    s = x.parent.left

                if s.left.color == 0 and s.right.color == 0:
                    s.color = 1
                    x = x.parent
                else:
                    if s.left.color == 0:
                        s.right.color = 0
                        s.color = 1
                        self.left_rotate(s)
                        s = x.parent.left

                    s.color = x.parent.color
                    x.parent.color = 0
                    s.left.color = 0
                    self.right_rotate(x.parent)
                    x = self.root
        x.color = 0

    def __rb_transplant(self, u, v):
        if u.parent is None:
            self.root = v
        elif u == u.parent.left:
            u.parent.left = v
        else:
            u.parent.right = v
        v.parent = u.parent

    # Node deletion
    def delete_node_helper(self, node, key):
        """
        :function: Deletes a node
        """
        z = self.TNULL
        while node != self.TNULL:
            if node.key == key:
                z = node
                break
            if node.key < key:
                node = node.right
            else:
                node = node.left

        if z == self.TNULL:
            message = "Key Error! Key "+str(key)+" does not exist in dictionary."
            raise Exception(message)
            return
        y = z
        self.length -= 1
        y_original_color = y.color
        if z.left == self.TNULL:
            x = z.right
            self.__rb_transplant(z, z.right)
        elif (z.right == self.TNULL):
            x = z.left
            self.__rb_transplant(z, z.left)
        else:
            y = self.minimum(z.right)
            y_original_color = y.color
            x = y.right
            if y.parent == z:
                x.parent = y
            else:
                self.__rb_transplant(y, y.right)
                y.right = z.right
                y.right.parent = y

            self.__rb_transplant(z, y)
            y.left = z.left
            y.left.parent = y
            y.color = z.color
        if y_original_color == 0:
            self.delete_fix(x)

    # Balance the tree after insertion

    def fix_insert(self, k):
        """
        :function: Rebalances tree after insert operation
        """
        while k.parent.color == 1:
            if k.parent == k.parent.parent.right:
                u = k.parent.parent.left
                if u.color == 1:
                    u.color = 0
                    k.parent.color = 0
                    k.parent.parent.color = 1
                    k = k.parent.parent
                else:
                    if k == k.parent.left:
                        k = k.parent
                        self.right_rotate(k)
                    k.parent.color = 0
                    k.parent.parent.color = 1
                    self.left_rotate(k.parent.parent)
            else:
                u = k.parent.parent.right

                if u.color == 1:
                    u.color = 0
                    k.parent.color = 0
                    k.parent.parent.color = 1
                    k = k.parent.parent
                else:
                    if k == k.parent.right:
                        k = k.parent
                        self.left_rotate(k)
                    k.parent.color = 0
                    k.parent.parent.color = 1
                    self.right_rotate(k.parent.parent)
            if k == self.root:
                break
        self.root.color = 0

    # returns the smallest key of dict
    def smallest(self):
        """
        :function: returns the smallest key of dicionary
        """
        node = self.root
        while node.left and node.left != self.TNULL:
            node = node.left
        if node and node == self.TNULL:
            raise Exception("Dictionary empty")
            return
        return node

    # returns biggest key of dict
    def biggest(self):
        """
        :function: returns the biggest key of dicionary
        """
        node = self.root
        while node.right and node.right != self.TNULL:
            node = node.right
        if node and node == self.TNULL:
            raise Exception("Dictionary empty")
            return
        return node

    def minimum(self, node):
        while node.left != self.TNULL:
            node = node.left
        return node

    def maximum(self, node):
        while node.right != self.TNULL:
            node = node.right
        return node

    def successor(self, x):
        if x.right != self.TNULL:
            return self.minimum(x.right)

        y = x.parent
        while y != self.TNULL and x == y.right:
            x = y
            y = y.parent
        return y

    def predecessor(self,  x):
        if (x.left != self.TNULL):
            return self.maximum(x.left)

        y = x.parent
        while y != self.TNULL and x == y.left:
            x = y
            y = y.parent

        return y

    def left_rotate(self, x):
        y = x.right
        x.right = y.left
        if y.left != self.TNULL:
            y.left.parent = x

        y.parent = x.parent
        if x.parent is None:
            self.root = y
        elif x == x.parent.left:
            x.parent.left = y
        else:
            x.parent.right = y
        y.left = x
        x.parent = y

    def right_rotate(self, x):
        y = x.left
        x.left = y.right
        if y.right != self.TNULL:
            y.right.parent = x

        y.parent = x.parent
        if x.parent is None:
            self.root = y
        elif x == x.parent.right:
            x.parent.right = y
        else:
            x.parent.left = y
        y.right = x
        x.parent = y

    # insertion of key value pair
    def insert(self, key, value):
        """
        :param names: key, value
        :param types: any
        :function: Inserts new element
        :returns: None
        """
        if self.length == 0:
            if type(key) in [list, dict, OrderedDict]:
                raise Exception("Key data type can't be list or another dictionary.")
                return
        elif not isinstance(self.root.key, type(key)):
            raise Exception("Data types of all keys must match.")
            return
        node = Node(key, value)
        node.parent = None
        # node.key = key
        # node.value=value
        node.left = self.TNULL
        node.right = self.TNULL
        # node.color = 1

        y = None
        x = self.root
        self.length += 1
        while x != self.TNULL:
            y = x
            if node.key == x.key:
                x.value = value
                return
            elif node.key < x.key:
                x = x.left
            else:
                x = x.right
        node.parent = y
        if y is None:
            self.root = node
        elif node.key < y.key:
            y.left = node
        else:
            y.right = node

        if node.parent is None:
            node.color = 0
            return

        if node.parent.parent is None:
            return

        self.fix_insert(node)

    # returns root of bst
    def get_root(self):
        return self.root

    # deletes a key-value pair from dict
    def pop(self, key):
        """
        :param name: key
        :param type: any
        :returns: None
        """
        self.delete_node_helper(self.root, key)

    # returns the kth smallest key (along with value)
    def ksmallUtil(self, root, k, ksmall):
        """
        :param names: root, k, ksmall
        :param types: Node, int, list of size 1
        :returns: None
        """
        if root == self.TNULL or ksmall[0] or k[0] < 0:
            return
        self.ksmallUtil(root.left, k, ksmall)
        k[0] -= 1
        if k[0] == 0:
            ksmall[0] = root
        self.ksmallUtil(root.right, k, ksmall)

    def KSmallest(self, k):
        """
        :param name: k
        :param type: int
        :function: Returns kth smallest key
        :returns: key value pair
        """
        if self.length < k or k < 1:
            raise Exception("k should be between 1 and size of dict")
            return
        ksmall = [None]
        self.ksmallUtil(self.root, [k], ksmall)
        return ksmall[0]

    # returns the kth largest key (along with value)
    def klargeUtil(self, root, k, klarge):
        """
        :param names: root, k, klarge
        :param types: Node, int, list of size 1
        :returns: None
        """
        if root == self.TNULL or klarge[0] or k[0] < 0:
            return
        self.klargeUtil(root.right, k, klarge)
        k[0] -= 1
        if k[0] == 0:
            klarge[0] = root
        self.klargeUtil(root.left, k, klarge)

    def KLargest(self, k):
        """
        :param name: k
        :param type: int
        :function: Returns kth largest key
        :returns: key value pair
        """
        if self.length < k or k < 1:
            raise Exception("k should be between 1 and size of dict")
            return
        klarge = [None]
        self.klargeUtil(self.root, [k], klarge)
        return klarge[0]


class TOPKStruct():
    """
    This class implements a data structure that allows to maitain a
    multiple-keys order between the solutions mappings.

    Parameters
    ----------
    keys: List[Tuple[str, str]]
        The keys that are used to define the order. A key is a 2-tuple
        (key, order) where:
            - key: str - is a variable in the solutions mappings.
            - order: str ("ASC" or "DESC") - defines if solutions mappings need
              to be ordered from the smallest (resp. largest) to the largest
              (resp. smallest) solution for the given key.
    limit: int
        The limit defines the capacity of the TOPKStruct.

    Example
    -------
    >>> topk = TOPKStruct([("?v1", "ASC"), ("?v2", "DESC")], limit=3)
    >>> topk.insert({"?v1": 1, "?v2": 1, "?v3": "A"})
    >>> topk.insert({"?v1": 1, "?v2": 2, "?v3": "B"})
    >>> topk.insert({"?v1": 2, "?v2": 1, "?v3": "C"})
    >>> topk.insert({"?v1": 3, "?v2": 1, "?v3": "D"})
    >>> topk.flatten()
        [
            {"?v1": 1, "?v2": 2, "?v3": "B"},
            {"?v1": 1, "?v2": 1, "?v3": "A"},
            {"?v1": 2, "?v2": 1, "?v3": "C"}
        ]
    """

    def __init__(self, keys: List[Tuple[str, str]], limit: int = 100) -> None:
        self._keys = keys
        self._limit = limit
        self._topk = OrderedDict()
        self._size = 0

    def __len__(self) -> int:
        return self._size

    def __flatten__(
        self, node: Union[OrderedDict, Mappings], key_index: int = 0
    ) -> List[Mappings]:
        """
        Returns an ordered list of the solutions mappings that are descendants
        of the current node.

        Parameters
        ----------
        node: Union[OrderedDict, Mappings]
            A node in the TOPKStruct.
        key_index: int - (default = 0)
            The index of the key that defines the order for the children of the
            current node.

        Returns
        -------
        List[Mappings]
            An ordered list of the solutions mappings that are descendants of
            the current node.
        """
        if isinstance(node, OrderedDict):
            nodes = []
            if self._keys[key_index][1] == "DESC":
                for item in node.reverse_iterate():
                    nodes.extend(self.__flatten__(item.value, key_index + 1))
            else:
                for item in node:
                    nodes.extend(self.__flatten__(item.value, key_index + 1))
            return nodes
        return node

    def flatten(self) -> List[Mappings]:
        """
        Returns an ordered list of the solutions mappings stored in the
        TOPKStruct.

        Returns
        -------
        List[Mappings]
            An ordered list of the solutions mappings stored in the TOPKStruct.
        """
        return self.__flatten__(self._topk)

    def lower_bound(self) -> Mappings:
        """
        Returns the k-th solution mappings in the TOPKStruct (according to the
        defined order).

        Returns
        -------
        Mappings
            The k-th solution mappings in the TOPKStruct.
        """
        node = self._topk
        key_index = 0
        while isinstance(node, OrderedDict):
            if self._keys[key_index][1] == "DESC":
                node = node.smallest().value
            else:
                node = node.biggest().value
            key_index += 1
        return node[0]

    def upper_bound(self) -> Mappings:
        """
        Returns the 1-th solution mappings in the TOPKStruct (according to the
        defined order).

        Returns
        -------
        Mappings
            The 1-th solution mappings in the TOPKStruct.
        """
        node = self._topk
        key_index = 0
        while isinstance(node, OrderedDict):
            if self._keys[key_index][1] == "DESC":
                node = node.biggest().value
            else:
                node = node.smallest().value
            key_index += 1
        return node[0]

    def threshold(self) -> Mappings:
        """
        Returns the threshold for a solution mappings to enter in the TOP-K. If
        there is less than k solution mappings in the TOPKStruct, the threshold
        is `None`, otherwise it is the k-th solution mappings.

        Returns
        -------
        Mappings
            The smallest solution mappings in the TOPKStruct.
        """
        if self._size < self._limit:
            return None
        return self.lower_bound()

    def can_insert(self, mappings: Mappings) -> bool:
        """
        Checks if a solution mappings can be inserted in the TOPKStruct.

        A solution mappings can be inserted if:
            - there is enough space left in the TOPKStruct.
            - the solution mappings is greater than the threshold.

        Parameters
        ----------
        mappings: Mappings
            The mappings to insert.

        Returns
        -------
        bool
            True if the solution mappings can be inserted, False otherwise.
        """
        threshold = self.threshold()
        if threshold is None:
            return True
        for key, order in self._keys:
            if order == "DESC":
                if mappings[key] > threshold[key]:
                    return True
                elif mappings[key] < threshold[key]:
                    return False
            elif order == "ASC":
                if mappings[key] < threshold[key]:
                    return True
                elif mappings[key] > threshold[key]:
                    return False
        return False

    def __insert__(
        self, node: OrderedDict, mappings: Mappings, key_index: int = 0
    ) -> None:
        """
        Inserts a solution mappings as a child of the current node.

        Parameters
        ----------
        node: OrderedDict
            The node on which we want to insert the soluton mappings.
        mappings: Mappings
            The solution mappings to insert.
        key_index: int - (default = 0)
            The index of the key that defines the order for the children of the
            current node.
        """
        key = self._keys[key_index][0]
        # logging.debug(f" __insert__ : {key} - {node}")
        if key_index == len(self._keys) - 1:
            if mappings[key] not in node:
                node[mappings[key]] = []
            node[mappings[key]].append(mappings)
        else:
            if mappings[key] not in node:
                node[mappings[key]] = OrderedDict()
            self.__insert__(node[mappings[key]], mappings, key_index + 1)
        # logging.debug(f" __insert__ : {key} - {node}")

    def insert(self, mappings: Mappings) -> bool:
        """
        Inserts a solution mappings in the TOPKStruct.

        A solution mappings can be inserted if:
            - there is enough space left in the TOPKStruct.
            - the solution mappings is greater than the k-th element in the
              TOPKStruct.

        Parameters
        ----------
        mappings: Mappings
            The solution mappings to insert.

        Returns
        -------
        bool
            True if the solution mappings has been inserted, False otherwise.
        """
        # logging.debug(f" insert : {self._size}/{self._limit} - {mappings}")
        if self.can_insert(mappings):
            self.__insert__(self._topk, mappings)
            self._size += 1
            if self._size > self._limit:
                self.delete(self.lower_bound())
            return True
        return False

    def __delete__(
        self, node: OrderedDict, mappings: Mappings, key_index: int = 0
    ) -> None:
        """
        Deletes a solution mappings from the current node.

        Parameters
        ----------
        node: OrderedDict
            The node from which we want to delete the soluton mappings.
        mappings: Mappings
            The solution mappings to delete.
        key_index: int - (default = 0)
            The index of the key that defines the order for the children of the
            current node.
        """
        key = self._keys[key_index][0]
        # logging.debug(f" __delete__ : {key} - {node}")
        if key_index == len(self._keys) - 1:
            node[mappings[key]].remove(mappings)
        else:
            self.__delete__(node[mappings[key]], mappings, key_index + 1)
        if len(node[mappings[key]]) == 0:
            node.pop(mappings[key])
        # logging.debug(f" __delete__ : {key} - {node}")

    def delete(self, mappings: Mappings) -> None:
        """
        Deletes a solution mappings in the TOPKStruct.

        Parameters
        ----------
        mappings: Mappings
            The solution mappings to delete.
        """
        # logging.debug(f" delete : {self._size}/{self._limit} - {mappings}")
        self.__delete__(self._topk, mappings)
        self._size -= 1

    def pop(self) -> Mappings:
        """
        Pop the 1-th solution mappings from the TOPKStruct.

        Returns
        -------
        Mappings
            The 1-th solution mappings in the TOPKStruct.
        """
        if self._size == 0:
            raise Exception("Dictionary empty")
        mappings = self.upper_bound()
        self.delete(mappings)
        return mappings


class PartialTOPKStruct(TOPKStruct):
    """
    This class is just a TOPKStruct with the possibility to set a threshold for
    the insert. A solution mappings can be inserted in the TOPKStruct only if it
    is greater than or equal to the threshold (according to the defined order).

    Parameters
    ----------
    keys: List[Tuple[str, str]]
        The keys that are used to define the order. A key is a 2-tuple
        (key, order) where:
            - key: str - is a variable in the solutions mappings.
            - order: str ("ASC" or "DESC") - defines if solutions mappings need
              to be ordered from the smallest (resp. largest) to the largest
              (resp. smallest) solution for the given key.
    limit: int
        The limit defines the capacity of the TOPKStruct.
    threshold: Mappings
        A threshold such that the k elements must be greater than or equal to it.

    Example
    -------
    >>> topk = TOPKStruct(
        [("?v1", "ASC"), ("?v2", "DESC")],
        limit=10,
        threshold={"?v1": 2, "?v2": 1})
    >>> topk.insert({"?v1": 1, "?v2": 1, "?v3": "A"})
    >>> topk.insert({"?v1": 1, "?v2": 2, "?v3": "B"})
    >>> topk.insert({"?v1": 2, "?v2": 1, "?v3": "C"})
    >>> topk.insert({"?v1": 3, "?v2": 1, "?v3": "D"})
    >>> topk.flatten()
        [
            {"?v1": 1, "?v2": 2, "?v3": "B"},
            {"?v1": 1, "?v2": 1, "?v3": "A"},
            {"?v1": 2, "?v2": 1, "?v3": "C"}
        ]
    """

    def __init__(
        self, keys: List[Tuple[str, str]], limit: int = 100,
        threshold: Optional[Mappings] = None
    ) -> None:
        super(PartialTOPKStruct, self).__init__(keys, limit=limit)
        self._threshold = threshold

    def threshold(self) -> Mappings:
        """
        Returns the smallest solution mappings in the PartialTOPKStruct
        (according to the defined order). If there is less than k solution
        mappings in the PartialTOPKStruct, the smallest solution mappings is
        the given threshold, otherwise it is the k-th solution mappings.

        Returns
        -------
        Mappings
            The smallest solution mappings in the PartialTOPKStruct.
        """
        if self._size < self._limit:
            return self._threshold
        return self.lower_bound()


if __name__ == "__main__":
    keys = [("__o1", "ASC"), ("__o2", "DESC"), ("__o3", "ASC")]
    topk = TOPKStruct(keys, limit=3)

    print(topk.insert({"__o1": 1, "__o2": 20, "__o3": 100, "?x": "A"}))
    print(topk.insert({"__o1": 1, "__o2": 20, "__o3": 200, "?x": "A"}))
    print(topk.insert({"__o1": 1, "__o2": 40, "__o3": 200, "?x": "A"}))
    print(topk.insert({"__o1": 2, "__o2": 40, "__o3": 400, "?x": "B"}))
    print(topk.insert({"__o1": 2, "__o2": 30, "__o3": 100, "?x": "A"}))
    print(topk.flatten())
    print(len(topk))

    print(f"lower bound: {topk.lower_bound()}")

    print(f"upper bound: {topk.upper_bound()}")
    print(topk.pop())
    print(f"upper bound: {topk.upper_bound()}")
    print(topk.flatten())
    print(len(topk))

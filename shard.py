# partitioning algorithm
# implements modular hashing

# procedure
# 1. assign nodes an index
# 2. hash "data" by key and mod by total number of nodes


import xxhash as hasher
import random, string
from database import Database # want to be able to use Database methods

# create a shard given database class
class Partition(Database):
    """docstring for consistent_hash class"""
    def __init__(self, IP, view):
        Database.__init__(self)
        self.virtual_range = 10
        self.IP = IP
        self.Physical_Nodes = view
        self.VIEW = []
        self.LABELS = {}
        self.initial_view(view)

    # construct initial view given ip/port pairs
    # use concept of virtual nodes
    def initial_view(self, view):

        node_list = []

        # insert ip addresses into dict
        for ip_port in view:
            for v_num in range(self.virtual_range):

                node = ip_port + str(v_num)

                node_list.append(node)
                self.LABELS[node] = ip_port

        # we want to store ring values in sorted list
        self.VIEW = node_list.sort()

    # perform a key operation, ie find the correct node given key
    def key_op(self, key):
        
        key_hash = hasher.xxh32(key).digest()

        node_val = key_hash % self.key_count
        ip_port = self.LABELS[node_val]

        return ip_port

    # return list of nodes to query which can be done in app
    def key_count(self):
        return self.Physical_Nodes

    # respond to view change request, perform a reshard
    # this can only be done if all nodes have been given new view
    def view_change(self, new_view):

        new_physical_view = self.update_view(new_view) # add or remove nodes

        self.reshard(new_physical_view) # reshard

    # perform a reshard, re-distribute minimun number of keys
    def reshard(self, new_physical_view):
        pass
        
    def update_view(self, new_view):
        # are we adding a new node to the current view
        if len(new_view) > len(self.Physical_Nodes):
            new_nodes = []

            for ip_port in new_view:
                if ip_port not in self.Physical_Nodes:
                    new_nodes.append(ip_port)

            # do something with new_nodes

        # if we are removing node from the current view
        elif len(new_view) < len(self.Physical_Nodes):
            pass

        # must be replacing a node
        else:
            pass



        

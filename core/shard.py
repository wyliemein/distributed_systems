# partitioning algorithm
# implements consistent hashing with virtual nodes

# procedure
# 1. hash nodes by IP address
# 2. hash "data" by key

# each data packet corresponds to a node on a unit circle of integers
# ownership of data is defined as:
#	the predecessor on the circle
#	the data between two nodes on the circle

import xxhash as hasher
import random, string
from bisect import bisect_right
from database import Database # want to be able to use Database methods

# create a shard given database class
class Partition(Database):
	"""docstring for consistent_hash class"""
	def __init__(self, address, view):
		Database.__init__(self)
		self.virtual_range = 10 # parameter for number of virtual nodes
		self.prime = 691        # parameter for hash mod value
		self.ADDRESS = address
		self.Physical_Nodes = view
		self.VIEW = []
		self.LABELS = {}       
		self.initial_view(view) 
		#self.distribution()
		print(self.VIEW)

	# hash frunction is a composit of xxhash modded by prime
	def hash(self, key):
		hash_val = hasher.xxh32(key).intdigest()

		# may be expensive but will produce better distribution
		return (hash_val % self.prime) 

	# construct initial view given ip/port pairs
	# use concept of virtual nodes
	# must handle collisions
	def initial_view(self, view):

		# insert ip addresses into dict
		for ip_port in view:
			
			self.add_node(ip_port)

		# we want to store ring values in sorted list
		self.VIEW.sort()

	# given node address, hash and create virtual nodes
	# after this method is called, self.VIEW should be sorted
	def add_node(self, ADDRESS):

		for v_num in range(self.virtual_range):

			virtural_node = ADDRESS + str(v_num)
			ring_num = self.hash(virtural_node) # unique value on "ring"

			# if ring_num is already in unsorted list, skip this iteration
			if ring_num in self.VIEW:
				print("hash collision detected")
				continue

			self.VIEW.append(ring_num)
			self.LABELS[ring_num] = ADDRESS

	# perform a key operation, ie find the correct node given key
	def key_op(self, key):
		
		ring_val = self.hash(key)

		node_ring_val = self.find_node(ring_val) 
		ip_port = self.LABELS[node_ring_val]

		return ip_port

	# return list of nodes to query which can be done in app
	def key_count(self):
		return self.Physical_Nodes

	# respond to view change request, perform a reshard
	# this can only be done if all nodes have been given new view
	def view_change(self, new_view):

		# are we adding a new node to the current view
		if len(new_view) > len(self.Physical_Nodes):
			new_nodes = []

			for ip_port in new_view:
				if ip_port not in self.Physical_Nodes:
					self.Physical_Nodes.append(ip_port)

					# add node here

		# if we are removing node from the current view
		elif len(new_view) < len(self.Physical_Nodes):
			pass

		# must be replacing a node 
		else:
			pass

	# perform a reshard, re-distribute minimun number of keys
	def reshard(self, new_physical_view):
		pass

	# perform binary search on VIEW given ring value
	# we need to be careful about wrap around case. If ring_val is max_int, wrap to 0
	def find_node(self, ring_val):

		if abs(ring_val) == (self.prime - 1):
			print("must wrap around") 
			node_num = self.VIEW[0]
			return node_num

		print(ring_val)
		node_num = bisect_right(self.VIEW, ring_val)
		if node_num:
			return self.VIEW[node_num]
		raise ValueError

	# find the distribution of keys to physical nodes
	def distribution(self):
		print("analyzing the distribution of nodes and keys")

		for v in self.LABELS:
			print("virtural_node:", v, " -- physical_node:", self.LABELS[v])




		
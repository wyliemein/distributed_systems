# partitioning algorithm
# implements consistent hashing 

# procedure
# 1. hash nodes then apply modulo number of nodes in system
# 2. hash "data" then apply modulo number of nodes in system

# each data packet corresponds to a node on a unit circle of integers
# ownership of data is defined as:
#	the predecessor on the circle
#	the data between two nodes on the circle

import xxhash
from database import Database # want to be able to use Database methods

# create a shard given database class
class Partition(Database):
	"""docstring for consistent_hash class"""
	def __init__(self):
		super.__init__(self)
		self.peers = []
		self.IP = None
		self.view = None

	# hash server to ring
	# use concept of virtual node, each node is mapped to multiple places in the ring
	# this produces further even distribution
	def map_server(self):
		pass

	# hash data to ring
	def map_data(self):
		pass

	# respond to view change request, perform a reshard
	def view_change(self):
		pass



		
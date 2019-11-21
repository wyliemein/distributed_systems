'''
partitioning algorithm implementing consistent hashing with virtual nodes

procedure
1. hash nodes by IP address
2. hash 'data' by key

each data packet corresponds to a node on a unit circle of integers
ownership of data is defined as:
	the predecessor on the circle
	the data between two nodes on the circle
'''

import xxhash as hasher
from bisect import bisect_right, bisect_left
from storage_host import Database 
from datetime import datetime
import json


class Node(Database):
	'''docstring for node class'''
	def __init__(self, router, address, view, replication_factor):
		Database.__init__(self)

		self.transfers = []
		self.virtual_range = 10 * replication_factor         # parameter for number of virtual nodes
		self.ring_edge = 691 if len(view) < 100 else 4127    # parameter for hash mod value
		self.num_shards = len(view) // replication_factorp
		self.shard_interval = ring_edge // self.num_shards
		self.ADDRESS = address
		self.physical_nodes = view
		self.VIEW = []
		self.virtual_translation = {}       
		self.router = router
		self.initial_view(view)

	def __repr__(self):
		return {'ADDRESS':self.ADDRESS, 'VIEW':self.physical_nodes, 'KEYS':len(self.keystore), 'VIRTUAL_NODES':self.virtual_translation}

	'''
	give a state report 
	this includes node data and distribution of keys to nodes
	'''
	def state_report(self):
		state = self.__repr__()

		state['TRANSFERS'] = {}
		string = 'node'
		itr = 1
		for event in self.transfers:
			key = string + str(itr)
			itr += 1
			state['TRANSFERS'][key] = event

		return state

	'''
	hash frunction is a composit of xxhash modded by prime
	'''
	def hash(self, key):
		hash_val = hasher.xxh32(key).intdigest()

		# may be expensive but will produce better distribution
		return (hash_val % self.ring_edge) 

	'''
	construct initial view given ip/port pairs
	use concept of virtual nodes
	must handle collisions
	'''
	def initial_view(self, view):

		# insert ip addresses into dict
		for ip_port in view:
			
			self.add_node(ip_port)

		# we want to store ring values in sorted list
		self.VIEW.sort()

	'''
	determine what shard this node is in
	defined as: see where ring index / virtual range lands
	'''
	def shard_memebership(self, ring_val):
		node_num = self.VIEW.index(ring_val)
		shard = node_num // self.shard_interval
		return shard

	'''
	given node address, hash and create virtual nodes
	after this method is called, self.VIEW should be sorted
	'''
	def add_node(self, ADDRESS):

		if ADDRESS not in self.physical_nodes:
			self.physical_nodes.append(ADDRESS)

		new_nodes = []
		for v_num in range(self.virtual_range):

			virtural_node = ADDRESS + str(v_num)
			ring_num = self.hash(virtural_node) # unique value on 'ring'

			# if ring_num is already in unsorted list, skip this iteration
			if ring_num in self.VIEW:
				print('System: Hash collision detected')
				continue

			self.VIEW.append(ring_num)
			self.virtual_translation[ring_num] = ADDRESS
			new_nodes.append(ring_num)

		return new_nodes

	'''
	Perform a key operation, ie find the correct node given key
	First hash the key then perform binary search to find the correct node
	to store the key. 
	'''
	def find_match(self, key):
		
		ring_val = self.hash(key)

		# get the virtual ring number
		node_ring_val = self.find_node('predecessor', ring_val)

		# convert to physical node ip
		ip_port = self.virtual_translation[node_ring_val]

		return ip_port

	'''
	insert a new key, send it to all nodes in shard
	this lays ontop of storage_host.insertKey
	'''
	def insert_key(self, key, value):
		pass

	'''
	get a key and make sure its the latest version
	'''
	def read_key(self, key):
		pass

	'''
	delete a key from all nodes in shard
	'''
	def delete_key(self, key):
		pass

	'''
	perform binary search on VIEW given ring value
	we need to be careful about wrap around case. If ring_val >= max_ring_val, return 0
	'''
	def find_node(self, direction, ring_val):

		if direction == 'predecessor':
			node_num = bisect_left(self.VIEW, ring_val)
			if node_num:
				return self.VIEW[node_num-1]
			return self.VIEW[-1]

		elif direction == 'successor':
			node_num = bisect_right(self.VIEW, ring_val)
			if node_num != len(self.VIEW):
				return self.VIEW[node_num]
			return self.VIEW[0]

	'''
	return all physical nodes
	'''
	def all_nodes(self):
		return self.physical_nodes

	'''
	respond to view change request, perform a reshard
	this can only be done if all nodes have been given new view
	'''
	def view_change(self, new_view):

		add_nodes = list(set(new_view) - set(self.physical_nodes))
		remove_nodes = list(set(self.physical_nodes) - set(new_view))

		# add nodes to ring
		for shard in add_nodes:
			add = True
			self.reshard(add, shard)

		# remove nodes from ring
		for shard in remove_nodes:

			add = False
			self.reshard(add, shard)

		return self.ADDRESS, self.numberOfKeys()

	'''
	Perform a reshard, re-distribute minimun number of keys
	Transfer all keys between new node and successor that should
	belong to new node
	'''
	def reshard(self, adding, node):
		# we are adding
		if adding:

			# hash new node and create virtual nodes
			new_virtual_nodes = self.add_node(node)

			for v in new_virtual_nodes:

				# find next node on ring and previous node on ring
				successor = self.find_node('successor', v)
				predecessor = self.find_node('predecessor', v)

				if self.virtual_translation[predecessor] == self.ADDRESS:
					# this instance contains keys that need to be re-distributed
					address = self.transfer(predecessor, v, successor) # we now have the keys to swap
		
		# we are removing nodes 
		else:
			pass

	'''
	this instance must be the predecessor of the new v-node
	now need to transfer keys from predecessor to new v-node
	'''
	def transfer(self, predecessor, new_node, successor):

		for key in list(self.keystore):
			ring_val = self.hash(key)

			# look for ring ring value between predecessor and successor
			correct_predecessor = (ring_val > predecessor) or (predecessor == self.VIEW[-1])

			if correct_predecessor and (ring_val < successor):
				
				# transfer key-val to new node 
				# once transfered, delete from predecessor
				address = self.virtual_translation[new_node]
				path = '/kv-store/internal/keys/' + key
				data = json.dumps({'value':self.keystore[key]})
				forward = False

				content, status = self.router.PUT(address, path, data, forward)

				# we were successful in transfering the key
				if status == 201:

					msg = str(self.keystore[key]) + " transfered: from " + self.ADDRESS + " to " + address + "\n"

					self.transfers.append(msg)

					del self.keystore[key]

				else:
					# error occured
					raise Exception ("non 201 status_code in transfer", res)

		return new_node






		
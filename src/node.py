'''
Partitioning algorithm implementing consistent hashing, virtual nodes
and shard membership.
'''

import xxhash as hasher
from bisect import bisect_right, bisect_left
from storage_host import KV_store
from datetime import datetime
import json


class Node(KV_store):
	'''docstring for node class'''
	def __init__(self, router, address, view, replication_factor):
		KV_store.__init__(self)
		self.history = [("Initialized", datetime.now())]
		self.transfers = []

		self.ADDRESS = address
		self.ring_edge = 691 if len(view) < 100 else 4127    # parameter for hash mod value
		self.repl_factor = replication_factor
		self.num_shards = len(view) // replication_factor
		self.virtual_range = 2        
		self.shard_interval = self.ring_edge // self.virtual_range
		self.nodes = view
		self.V_SHARDS = [] # store all virtual shards
		self.P_SHARDS = [[] for i in range(0, self.num_shards)] # map physical shards to nodes
		self.virtual_translation = {}
   
		self.router = router
		self.initial_sharding(view)

	def __repr__(self):
		return {'ADDRESS':self.ADDRESS, 'V_SHARDS':self.V_SHARDS, 'P_SHARDS':self.P_SHARDS, 'KEYS':len(self.keystore)}

	'''
	give a state report 
	this includes node data and distribution of keys to nodes
	'''
	def state_report(self):
		state = self.__repr__()

		state['HISTORY'] = {}
		string = 'node'
		itr = 1
		for event in self.history:
			key = string + str(itr)
			itr += 1
			state['HISTORY'][key] = event

		return state

	'''
	Below are all key operations, these functions are used as a wraper for 
	the key-value store to evaluate causal objects before actually writing
	'''
	def insert_key(self, key, value):
		pass

	def read_key(self, key):
		pass

	def delete_key(self, key):
		pass

	'''
	parse the causal object to determine ordering
	'''
	def parse_causal_object(self, response):
		pass

	'''
	define a gossip protocol to ensure eventual consistency among nodes in shard
	'''
	def anti_entropy(self):
		pass

	'''
	hash frunction is a composit of xxhash modded by prime
	'''
	def hash(self, key):
		hash_val = hasher.xxh32(key).intdigest()

		# may be expensive but will produce better distribution
		return (hash_val % self.ring_edge) 

	'''
	construct virtual shards and map them to physical shards
	once mapping is done, sort
	'''
	def initial_sharding(self, view):
		for p_shard in range(self.num_shards): 
			for v_shard in range(self.virtual_range):

				virtural_shard = str(p_shard) + str(v_shard)
				ring_num = self.hash(virtural_shard) # unique value on 'ring'

				# if ring_num is already in unsorted list, skip this iteration
				if ring_num in self.V_SHARDS:
					print('System: Hash collision detected')
					continue

				self.V_SHARDS.append(ring_num)
				self.virtual_translation[ring_num] = p_shard

		self.V_SHARDS.sort()
		self.init_node_assignment(view)

	'''
	We want to evenly add replica nodes to shards, ie each shard gets
	repl_factor number of shards
	'''
	def init_node_assignment(self, view):
		nodes = {}
		for ip_port in view:
			node_num = self.hash(ip_port)
			nodes[node_num] = ip_port

		nodes = OrderedDict(sorted(nodes.items()))
		vals = list(nodes.values())

		node_iter = 0
		shard_num = 0
		while node_iter < len(nodes):
			if node_iter % self.repl_factor == 0 and node_iter != 0:
				shard_num += 1

			self.P_SHARDS[shard_num].append(vals[node_iter])
			node_iter += 1

	'''
	Add a single node to shards, must decide which shard to add to.
	If len(nodes) + 1 // r > shard_num, we need to add a shard perform a re-shard
	'''
	def add_node(self, node):
		if ((len(self.nodes) + 1) // self.repl_factor) > self.num_shards:
			# must perform a shard shuffling 
			pass

		else:
			ring_val = self.hash(node)
			shard_ID = self.find_match(ring_val)
			self.P_SHARDS[shard_ID].append(node)

	'''
	remove single node from a shard, determine if we need to re-shuffle shards
	'''
	def remove_node(self, node):
		pass

	'''
	Perform a key operation, ie find the correct shard given key.
	First hash the key then perform binary search to find the correct shard
	to store the key. 
	'''
	def find_match(self, key):
		
		ring_val = self.hash(key)

		# get the virtual shard number
		v_shard = self.find_shard('predecessor', ring_val)

		# convert to physical shard
		shard_ID = self.virtual_translation[v_shard]

		return shard_ID

	'''
	perform binary search on list of virtual shards given ring value
	we need to be careful about wrap around case. If ring_val >= max_ring_val, return 0
	'''
	def find_shard(self, direction, ring_val):

		if direction == 'predecessor':
			v_shard = bisect_left(self.V_SHARDS, ring_val)
			if v_shard:
				return self.V_SHARDS[v_shard-1]
			return self.V_SHARDS[-1]

		elif direction == 'successor':
			v_shard = bisect_right(self.V_SHARDS, ring_val)
			if v_shard != len(self.V_SHARDS):
				return self.V_SHARDS[v_shard]
			return self.V_SHARDS[0]

	'''
	return all physical shards
	'''
	def all_shards(self):
		return self.P_SHARDS

	'''
	respond to view change request, perform a reshard
	this can only be done if all nodes have been given new view
	'''
	def view_change(self, new_view):
		pass

	'''
	Perform a reshard, re-distribute minimun number of keys
	Transfer all keys between new node and successor that should
	belong to new node
	'''
	def reshard(self, type, node):
		pass

	'''
	transfer keys from this node to other nodes, must be an atomic operation
	'''
	def transfer(self, predecessor, new_node, successor):
		pass






		
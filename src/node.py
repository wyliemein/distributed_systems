'''
Partitioning algorithm implementing consistent hashing, virtual nodes
and shard membership.
'''

import xxhash as hasher
from bisect import bisect_right, bisect_left
from datetime import datetime
import json
from collections import OrderedDict
from storage_host import KV_store
from vectorclock import VectorClock


class Node(KV_store, VectorClock):
	'''docstring for node class'''
	def __init__(self, router, address, view, replication_factor):
		KV_store.__init__(self)
		self.history = [('Initialized', datetime.now())]

		self.ADDRESS = address
		self.ring_edge = 691 if len(view) < 100 else 4127    # parameter for hash mod value
		self.repl_factor = replication_factor
		self.num_shards = 0
		self.virtual_range = 10        
		self.shard_interval = self.ring_edge // self.virtual_range
		self.nodes = []
		self.shard_ID = -1
		self.V_SHARDS = [] # store all virtual shards
		self.P_SHARDS = [[] for i in range(0, self.num_shards)] # map physical shards to nodes
		self.virtual_translation = {} # map virtual shards to physical shards
   
		self.router = router
		self.view_change(view, replication_factor)

	def __repr__(self):
		return {'ADDRESS':self.ADDRESS, 'V_SHARDS':self.V_SHARDS, 'P_SHARDS':self.P_SHARDS, 'KEYS':len(self.keystore)}

	def __str__(self):
		return 'ADDRESS: '+self.ADDRESS+'\nREPL_F: '+str(self.repl_factor)+'\nNODES: '+(', '.join(map(str, self.nodes))) + '\nP_SHARDS: ' + (', '.join(map(str, self.P_SHARDS)))

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
	return all physical shards
	'''
	def all_shards(self):
		return self.P_SHARDS

	'''
	get all nodes in this shard
	'''
	def shard_replicas(self, shard_ID):
		return self.P_SHARDS[shard_ID]

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
	hash frunction is a composit of xxhash modded by prime
	'''
	def hash(self, key, Type):
		hash_val = hasher.xxh32(key).intdigest()

		# may be expensive but will produce better distribution
		return (hash_val % self.ring_edge)


	'''
	evenly distribute nodes into num_shard buckets
	'''
	def even_distribution(self, repl_factor, nodes):

		nodes.sort()
		num_shards = (len(nodes) // repl_factor)
		replicas = (len(nodes) // num_shards)
		overflow = (len(nodes) % num_shards)

		shards = [[] for i in range(0, num_shards)]
		shard_dict = {}

		node_iter = 0
		for shard in range(num_shards):
			extra = (1 if shard < overflow else 0)
			interval = replicas + extra

			shards[shard] = nodes[node_iter:(node_iter+interval)]
			node_iter += interval

			for node in shards[shard]:
				shard_dict[node] = shard

		return shard_dict

	'''
	Perform a key operation, ie. find the correct shard given key.
	First hash the key then perform binary search to find the correct shard
	to store the key. 
	'''
	def find_match(self, key):
		
		ring_val = self.hash(key, 'consistent')
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
	respond to view change request, perform a reshard
	this can only be done if all nodes have been given new view
	2 cases:
		1. len(nodes) + 1 // r > or < shard_num: we need to add or 
			remove a shard to maintain repl_factor
		2. add and/or remove nodes
	'''
	def view_change(self, view, repl_factor):
		new_num_shards = len(view) // repl_factor
		if new_num_shards == 1:
			new_num_shards = 2

		buckets = self.even_distribution(repl_factor, view)
		#print('buckets', buckets)

		# add nodes and shards
		for node in view:
			my_shard = buckets[node]

			# add a new node
			if node not in self.nodes:
				self.add_node(node, my_shard, new_num_shards)

			# move node to new shard
			else:
				if my_shard >= len(self.P_SHARDS):
					self.add_shard()
				if node not in self.P_SHARDS[my_shard]:
					self.move_node(node, my_shard)

		old_nodes = list(set(self.nodes) - set(view))

		# remove nodes from view
		for node in old_nodes:
			self.remove_node(node)

		# remove empty shards
		for shard_ID in range(0, len(self.P_SHARDS)):
			if len(self.P_SHARDS[shard_ID]) == 0:
				self.remove_shard(shard_ID)

	'''
	Add a single node to shards and get keys from shard replicas
	'''
	def add_node(self, node, shard_ID, num_shards):

		# do we need to add another shard before adding nodes
		while num_shards > self.num_shards:
			self.add_shard()

		# update internal data structures
		self.nodes.append(node)
		self.nodes.sort()
		self.P_SHARDS[shard_ID].append(node)

		# determine if the node's shard is this shard
		if self.shard_ID == shard_ID:
			print('<adding node to:', shard_ID)
			self.shard_keys()

	'''
	move node from old shard to new shard and perform atomic key transfer
	'''
	def move_node(self, node, shard_ID):
		
		old_shard_ID = self.nodes.index(node) // self.num_shards
		if node not in self.P_SHARDS[old_shard_ID]:
			if old_shard_ID > 0 and node in self.P_SHARDS[old_shard_ID-1]:
				old_shard_ID += -1
			else:
				old_shard_ID += 1
		
		# do we need to add another shard before adding nodes
		while shard_ID > len(self.P_SHARDS):
			self.add_shard()

		self.atomic_key_transfer(old_shard_ID, shard_ID, node)
		self.P_SHARDS[shard_ID].append(node)
		self.P_SHARDS[old_shard_ID].pop(self.P_SHARDS[old_shard_ID].index(node))

	'''
	remove single node from a shard and send final state to shard replicas
	'''
	def remove_node(self, node):
		shard_ID = (self.nodes.index(node)-1) // self.num_shards
		if shard_ID > 0 and shard_ID < len(self.P_SHARDS) and node not in self.P_SHARDS[shard_ID]:
			if shard_ID > 0 and node in self.P_SHARDS[shard_ID-1]:
				shard_ID += -1
			else:
				shard_ID += 1
			#print('error finding node')

		if node == self.ADDRESS:
			print('<send my final state to my replicas before removing') 
			success = self.final_state_transfer(node)

			if success:
				self.nodes.pop(self.nodes.index(node))
			else:
				raise Exception('<final_state_transfer failed>')
		else:
			self.nodes.pop(self.nodes.index(node))
			self.P_SHARDS[shard_ID].pop(self.P_SHARDS[shard_ID].index(node))

	'''
	add shard to view
	'''
	def add_shard(self):

		new_shards = []
		p_shard = self.num_shards
		if p_shard >= len(self.P_SHARDS):
			self.P_SHARDS.append([])

		for v_shard in range(self.virtual_range):

			virtural_shard = str(p_shard) + str(v_shard)
			ring_num = self.hash(virtural_shard, 'consistent') # unique value on 'ring'

			# if ring_num is already in unsorted list, skip this iteration
			if ring_num in self.V_SHARDS:
				#print('<System: Hash collision detected>')
				continue

			self.V_SHARDS.append(ring_num)
			self.virtual_translation[ring_num] = p_shard
		self.num_shards += 1
		self.V_SHARDS.sort()

		return new_shards

	'''
	remove from all internal data structures if there are no nodes in shard
	'''
	def remove_shard(self, shard_ID):
		self.P_SHARDS.pop(shard_ID)

	'''
	transfer keys from one shard to another
	send keys from origin shard replicas to new shard replicas
	'''
	def keys_transfer(self):
		for v in v_shards:

			successor = self.find_shard('successor', v)
			predecessor = self.find_shard('predecessor', v)

			# if this node is the precessor a new v_shard, transfer keys over
			if self.virtual_translation[predecessor] == self.shard_ID:
				pass

	'''
	get all keys for a given shard
	'''
	def shard_keys(self):
		pass

	'''
	perform an atomic key transfer
	concurrent operation: get new keys, send old keys, delete old keys
	'''
	def atomic_key_transfer(self, old_shard_ID, new_shard_ID, node):
		return True

	'''
	send final state of node before removing a node
	'''
	def final_state_transfer(self, node):
		return True

	'''
	handle node failures, check if node should be removed or not
	'''
	def handle_unresponsive_node(self, node):
		pass





















		
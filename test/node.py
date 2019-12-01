'''
Partitioning algorithm implementing consistent hashing, virtual nodes
and shard membership.
'''

import xxhash as hasher
from bisect import bisect_right, bisect_left
from storage_host import KV_store
from datetime import datetime
import json
from collections import OrderedDict


class Node(KV_store):
	'''docstring for node class'''
	def __init__(self, router, address, view, replication_factor):
		KV_store.__init__(self)
		self.history = [('Initialized', datetime.now())]

		self.ADDRESS = address
		self.ring_edge = 691 if len(view) < 100 else 4127    # parameter for hash mod value
		self.repl_factor = replication_factor
		self.num_shards = len(view) // replication_factor
		self.virtual_range = 10        
		self.shard_interval = self.ring_edge // self.virtual_range
		self.nodes = view
		self.shard_ID = -1
		self.V_SHARDS = [] # store all virtual shards
		self.P_SHARDS = [[] for i in range(0, self.num_shards)] # map physical shards to nodes
		self.virtual_translation = {} # map virtual shards to physical shards
   
		self.router = router
		self.initial_sharding()

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
	return all physical shards
	'''
	def all_shards(self):
		return self.P_SHARDS

	'''
	get all nodes in this shard
	'''
	def shard_replicas(self):
		return self.P_SHARDS[self.shard_ID]

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
	def hash(self, key):
		hash_val = hasher.xxh32(key).intdigest()

		# may be expensive but will produce better distribution
		return (hash_val % self.ring_edge) 

	'''
	construct virtual shards and map them to physical shards
	once mapping is done, sort
	'''
	def initial_sharding(self):
		for p_shard in range(0, self.num_shards): 
			
			self.add_shard(p_shard)

		self.V_SHARDS.sort()
		self.init_node_assignment(self.nodes)

	'''
	add shard to view
	'''
	def add_shard(self, p_shard):

		new_shards = []
		for v_shard in range(self.virtual_range):

			virtural_shard = str(p_shard) + str(v_shard)
			ring_num = self.hash(virtural_shard) # unique value on 'ring'

			# if ring_num is already in unsorted list, skip this iteration
			if ring_num in self.V_SHARDS:
				print('<System: Hash collision detected>')
				continue

			self.V_SHARDS.append(ring_num)
			self.virtual_translation[ring_num] = p_shard

		return new_shards

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
			if vals[node_iter] == self.ADDRESS:
				self.shard_ID = shard_num

			node_iter += 1

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
	respond to view change request, perform a reshard
	this can only be done if all nodes have been given new view
	2 cases:
		1. len(nodes) + 1 // r > shard_num: we need to add a shard to maintain repl_factor
		2. add node to correct shard
	'''
	def view_change(self, new_view):
		print('shards:', self.P_SHARDS)

		add_nodes = list(set(new_view) - set(self.nodes))
		remove_nodes = list(set(self.nodes) - set(new_view))

		# determine if we need to add or remove a shard in order to maintain repl_factor
		if ((len(self.nodes)+len(add_nodes)) // self.repl_factor) > self.num_shards:
				self.update_shards('add', ((len(self.nodes)+len(add_nodes)) // self.repl_factor))

		elif ((len(self.nodes)-len(remove_nodes)) // self.repl_factor) < self.num_shards:
				self.update_shards('remove', ((len(self.nodes)-len(remove_nodes)) // self.repl_factor))

		# add nodes to shard
		for node in add_nodes:
			self.add_node(node)

		# remove nodes from shard
		for node in remove_nodes:
			self.remove_node(node)

		print('shards:', self.P_SHARDS)

		return self.ADDRESS, self.numberOfKeys()

	'''
	add or remove a shard to the system in order to maintain repl_factor
	must move some nodes to new shard as well as keys
	'''
	def update_shards(self, action, num_shards):
		if action == 'add':
			for new_p_shard in range(self.num_shards+1, num_shards+1):

				# transfer nodes to new shard
				#self.transfer_nodes(new_p_shard)
				self.add_shard(new_p_shard)
				print("<adding shard: ID", new_p_shard, ">")
				

		else:
			for old_shard in range(num_shards, self.num_shards):
				pass

	'''
	transfer nodes to new shard: take one node from each shard
	'''
	def transfer_nodes(self, new_shard_ID):
		if new_shard_ID >= len(self.P_SHARDS):
			self.P_SHARDS.append([])

		for shard in self.P_SHARDS:
			if len(self.P_SHARDS[new_shard_ID-1]) >= self.repl_factor:
				break
			node = shard.pop()
			self.P_SHARDS[new_shard_ID-1].append(node)
			print('<moving', node, 'to new shard:', new_shard_ID-1, '>')

		# recursive call if we need more than one node from each shard
		if len(self.P_SHARDS[new_shard_ID-1]) < self.repl_factor:
			self.transfer_nodes(new_shard_ID)

	'''
	transfer keys from one shard to another
	'''
	def transfer_keys(self):
		for v in v_shards:

			successor = self.find_shard('successor', v)
			predecessor = self.find_shard('predecessor', v)

			# if this node is the precessor a new v_shard, transfer keys over
			if self.virtual_translation[predecessor] == self.shard_ID:
				pass

	'''
	Add a single node to shards, must decide which shard to add to.
	'''
	def add_node(self, node):
		self.nodes.append(node)

		shard_ID = self.find_match(node)
		self.P_SHARDS[shard_ID].append(node)
		
		# if the new node is being added to my shard, transfer my keys
		if self.shard_ID == shard_ID:
			print('<adding node: get keys from shard replicas>')
			print('<get keys form shard', shard_ID)

	'''
	remove single node from a shard
	'''
	def remove_node(self, node):
		self.nodes.pop(self.nodes.index(node))
		if node == self.ADDRESS:
			print('<send my final state to my replicas before removing') 

	'''
	transfer keys from this node to other nodes, must be an atomic operation
	'''
	def shard_broadcast(self, destination, message):
		pass



















		
'''
Partitioning algorithm implementing consistent hashing, virtual nodes
and shard membership.
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
		self.history = [("Initialized", datetime.now())]
		self.transfers = []
		self.ADDRESS = address

		self.ring_edge = 691 if len(view) < 100 else 4127    # parameter for hash mod value
		self.repl_factor = replication_factor
		self.num_shards = len(view) // replication_factor
		self.virtual_range = 10 * self.num_shards         
		self.shard_interval = self.ring_edge // self.virtual_range
		self.nodes = view

		self.V_SHARDS = [] # store all virtual shards
		self.P_SHARDS = [[] for i in range(num_shards)] # map physical shards to nodes

		self.shard_ID = -1      
		self.router = router
		self.initial_view(view)

	def __repr__(self):
		return {'ADDRESS':self.ADDRESS, 'VIEW':self.nodes, 'KEYS':len(self.keystore), 'VIRTUAL_SHARDS':self.P_SHARDS}

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
	construct virtual shards and map them to physical shards
	once mapping is done, sort
	'''
	def initial_sharding(self):
		for v_num in range(self.virtual_range):

			virtural_shard = ADDRESS + str(v_num)
			ring_num = self.hash(virtural_shard) # unique value on 'ring'

			# if ring_num is already in unsorted list, skip this iteration
			if ring_num in self.VIEW:
				print('System: Hash collision detected')
				continue

			self.V_SHARDS.append(ring_num)

		# we want to keep these in sorted order for ring ownership
		self.V_SHARDS.sort()

		self.init_shard_population()

	'''
	We want to evenly add replica nodes to shards, ie each shard gets
	repl_factor number of shards
	'''
	def init_shard_population(self):
		nodes = []
		for ip_port in self.nodes:
			node_num = self.hash(ip_port)
			nodes.append(node_num)

		nodes.sort()

		g_iter = 0
		shard_num = 0
		while g_iter < len(nodes):
			if g_iter % self.repl_factor == 0:
				shard_num += 1

			self.P_SHARDS[shard_num].append(nodes[g_iter])

	'''
	Add a single node to shards, must decide which shard to add to.
	If len(nodes) + 1 // r > shard_num, we need to add a shard perform a re-shard
	'''
	def add_node(self, node):
 		if ((len(self.nodes) + 1) // self.repl_factor) > self.num_shards:
 			# must perform a shard shuffling 

		else:
			ring_val = self.hash(node)
			shard_ID = self.shard_ID(ring_val)
			self.P_SHARDS[shard_ID].append(node)

	'''
	determine what physical shard this node is in
	defined as: see where ring index / virtual range lands
	'''
	def shard_ID(self, ring_val):
		node_num = self.V_SHARDS.index(ring_val)
		shard = node_num // self.shard_interval
		return shard

	'''
	Perform a key operation, ie find the correct node given key
	First hash the key then perform binary search to find the correct node
	to store the key. 
	'''
	def find_match(self, key):
		
		ring_val = self.hash(key)

		# get the virtual ring number
		v_shard = self.find_shard('predecessor', ring_val)

		# convert to physical node ip
		shard_ID = self.P_SHARDS[node_ring_val]

		return shard_ID

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
	return all physical nodes
	'''
	def all_shards(self):
		return self.P_SHARDS

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

				if self.P_SHARDS[predecessor] == self.ADDRESS:
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
				address = self.P_SHARDS[new_node]
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






		
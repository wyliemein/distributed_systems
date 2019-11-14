# partitioning algorithm
# implements consistent hashing with virtual nodes

# procedure
# 1. hash nodes by IP address
# 2. hash 'data' by key

# each data packet corresponds to a node on a unit circle of integers
# ownership of data is defined as:
#	the predecessor on the circle
#	the data between two nodes on the circle

import xxhash as hasher
import random, string
from bisect import bisect_right, bisect_left
from storage_host import Database 
from datetime import datetime
import json

''' 
create a shard which stores addess of nodes in a hash ring
inherits from database.py
'''
class Partition(Database):
	'''docstring for consistent_hash class'''
	def __init__(self, router, address, view):
		Database.__init__(self, address)
		self.virtual_range = 10 # parameter for number of virtual nodes
		self.prime = 691        # parameter for hash mod value
		self.ADDRESS = address
		self.Physical_Nodes = view
		self.VIEW = []
		self.LABELS = {}      
		self.initial_view(view) 
		self.distribution = {ip:0 for ip in self.Physical_Nodes} 
		self.router = router

	def __repr__(self):
		return {'ADDRESS':self.ADDRESS, 'VIEW':self.Physical_Nodes, 'VIRTUAL_NODES':self.LABELS}

	def __str__(self):
		return 'ADDRESS = '+self.ADDRESS+'\nVIEW = '+(', '.join(map(str, self.Physical_Nodes))) + '\nHASHED = ' + (', '.join(map(str, self.VIEW)))

	'''
	give a state report 
	this includes node data and distribution of keys to nodes
	'''
	def state_report(self):
		state = self.__repr__()

		string = 'node'
		itr = 1
		for node in self.distribution:
			key = string + str(itr)
			itr += 1
			state[key] = self.distribution[node]

		return state

	'''
	hash frunction is a composit of xxhash modded by prime
	'''
	def hash(self, key):
		hash_val = hasher.xxh32(key).intdigest()

		# may be expensive but will produce better distribution
		return (hash_val % self.prime) 

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
	given node address, hash and create virtual nodes
	after this method is called, self.VIEW should be sorted
	'''
	def add_node(self, ADDRESS):

		if ADDRESS not in self.Physical_Nodes:
			self.Physical_Nodes.append(ADDRESS)
			
		new_nodes = []
		for v_num in range(self.virtual_range):

			virtural_node = ADDRESS + str(v_num)
			ring_num = self.hash(virtural_node) # unique value on 'ring'

			# if ring_num is already in unsorted list, skip this iteration
			if ring_num in self.VIEW:
				print('system: Hash collision detected')
				continue

			self.VIEW.append(ring_num)
			self.LABELS[ring_num] = ADDRESS
			new_nodes.append(ring_num)

		return new_nodes

	'''
	Perform a key operation, ie find the correct node given key
	First hash the key then perform binary search to find the correct node
	to store the key
	'''
	def find_match(self, key):
		
		ring_val = self.hash(key)

		# get the virtual ring number
		node_ring_val = self.find_node('predecessor', ring_val)

		# convert to physical node ip
		ip_port = self.LABELS[node_ring_val]

		# for optimization purposes, store how many keys have been mapped to each ip
		self.distribution[ip_port] += 1

		return ip_port

	'''
	perform binary search on VIEW given ring value
	we need to be careful about wrap around case. If ring_val >= max_ring_val, return 0
	'''
	def find_node(self, direction, ring_val):

		if abs(ring_val) == (self.prime - 1):
			print('must wrap around') 
			node_num = self.VIEW[0]
			return node_num

		if direction == 'predecessor':
			node_num = bisect_right(self.VIEW, ring_val)
			if node_num != len(self.VIEW):
				return self.VIEW[node_num]
			return False

		elif direction == 'successor':
			node_num = bisect_left(self.VIEW, ring_val)
			if node_num:
				return self.VIEW[node_num-1]
			return False

	def all_nodes(self):
		return self.Physical_Nodes

	'''
	respond to view change request, perform a reshard
	this can only be done if all nodes have been given new view
	'''
	def view_change(self, new_view):

		add_nodes = list(set(new_view) - set(self.Physical_Nodes))
		remove_nodes = list(set(self.Physical_Nodes) - set(new_view))

		# add nodes to ring
		for shard in add_nodes:
			add = True
			self.reshard(add, shard)

		# remove nodes from ring
		for shard in remove_nodes:

			add = False
			self.reshard(add, shard)

	'''
	Perform a reshard, re-distribute minimun number of keys
	Transfer all keys between new node and successor that should
	belong to new node
	'''
	def reshard(self, adding, node):
		status_code = 0

		if adding:

			# hash new node and create virlual nodes
			new_virtual_nodes = self.add_node(node)

			for v in new_virtual_nodes:

				successor = self.find_node('successor', v)
				predecessor = self.find_node('predecessor', v)

				if successor == False:
					successor == (self.prime - 1) # if no successor, scan till end of ring

				if predecessor and self.LABELS[predecessor] == self.ADDRESS:
					# this instance contains keys that need to be re-distributed
					ack = self.transfer(predecessor, v, successor) # we now have the keys to swap

					if ack == False:
						status_code += 1
		else:
			pass

		return status_code

	'''
	this instance must be the predecessor of the new v-node
	now need to transfer keys from predecessor to new v-node
	'''
	def transfer(self, predecessor, new_node, successor):
		
		status = 0

		for key in self.keystore:
			ring_val = self.hash(key)

			if ring_val > predecessor and ring_val < successor:
				
				# transfer key-val to new node 
				# once transfered, delete from predecessor
				address = self.LABELS[new_node]
				path = '/kv-store/keys/' + key
				string_dict = json.dumps({key:self.keystore[key]})
				data = json.loads(string_dict)
				forward = False

				res = self.router.PUT(address, path, data, forward)

				if res.status_code == 201:
					# delete key from self.keystore
					pass
				else:
					# error occured
					status += 1

		return status	

	'''
	Prints all events which have occured on this database
	'''
	def print_history(self):
		for event in self.history:
			print(event[0] + ': ' + event[1].strftime('%m/%d/%Y, %H:%M:%S'))

	'''
	Prints all events which have occured on this database
	'''
	def return_history(self):
		output = '<center>'
		for event in self.history:
			output = output + event[0] + ': ' + event[1].strftime('%m/%d/%Y, %H:%M:%S') + '<br>'
		return output + '</center>'





		
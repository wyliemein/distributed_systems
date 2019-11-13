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
from storage_host import Database 
from datetime import datetime
from flask import Flask, request, jsonify, make_response
import requests
import json

""" 
create a shard which stores addess of nodes in a hash ring
inherits from database.py
"""
class Partition(Database):
	"""docstring for consistent_hash class"""
	def __init__(self, address, view):
		Database.__init__(self, address)
		self.virtual_range = 10 # parameter for number of virtual nodes
		self.prime = 691        # parameter for hash mod value
		self.ADDRESS = address
		self.Physical_Nodes = view
		self.VIEW = []
		self.LABELS = {}      
		self.initial_view(view) 
		self.distribution = {ip:0 for ip in self.Physical_Nodes} 
		
		#self.distribution()

	def __repr__(self):
		return {'ADDRESS':self.ADDRESS, 'VIEW':self.Physical_Nodes, 'HASHED':self.LABELS}

	def __str__(self):
		return 'ADDRESS = '+self.ADDRESS+'\nVIEW = '+(', '.join(map(str, self.Physical_Nodes))) + '\nHASHED = ' + (', '.join(map(str, self.VIEW)))

	"""
	give a state report 
	this includes node data and distribution of keys to nodes
	"""
	def state_report(self):
		state = self.__repr__()

		string = "node"
		itr = 1
		for node in self.distribution:
			key = string + str(itr)
			itr += 1
			state[key] = self.distribution[node]

		return state

	"""
	hash frunction is a composit of xxhash modded by prime
	"""
	def hash(self, key):
		hash_val = hasher.xxh32(key).intdigest()

		# may be expensive but will produce better distribution
		return (hash_val % self.prime) 

	"""
	construct initial view given ip/port pairs
	use concept of virtual nodes
	must handle collisions
	"""
	def initial_view(self, view):

		# insert ip addresses into dict
		for ip_port in view:
			
			self.add_node(ip_port)

		# we want to store ring values in sorted list
		self.VIEW.sort()

	def all_nodes(self):
		# return all nodes in our current view
		return self.Physical_Nodes

	"""
	given node address, hash and create virtual nodes
	after this method is called, self.VIEW should be sorted
	"""
	def add_node(self, ADDRESS):

		for v_num in range(self.virtual_range):

			virtural_node = ADDRESS + str(v_num)
			ring_num = self.hash(virtural_node) # unique value on "ring"

			# if ring_num is already in unsorted list, skip this iteration
			if ring_num in self.VIEW:
				print("system: Hash collision detected")
				continue

			self.VIEW.append(ring_num)
			self.LABELS[ring_num] = ADDRESS

	"""
	Perform a key operation, ie find the correct node given key
	First hash the key then perform binary search to find the correct node
	to store the key
	"""
	def find_match(self, key):
		
		ring_val = self.hash(key)

		# get the virtual ring number
		node_ring_val = self.find_node(ring_val)

		# convert to physical node ip
		ip_port = self.LABELS[node_ring_val]

		# for optimization purposes, store how many keys have been mapped to each ip
		self.distribution[ip_port] += 1

		return ip_port

	"""
	perform binary search on VIEW given ring value
	we need to be careful about wrap around case. If ring_val >= max_ring_val, return 0
	"""
	def find_node(self, ring_val):

		if abs(ring_val) == (self.prime - 1):
			print("must wrap around") 
			node_num = self.VIEW[0]
			return node_num

		node_num = bisect_right(self.VIEW, ring_val)
		if node_num:
			return self.VIEW[node_num]
		raise ValueError

	def all_nodes(self):
		return self.Physical_Nodes

	"""
	respond to view change request, perform a reshard
	this can only be done if all nodes have been given new view
	"""
	def view_change(self, new_view):

		add_nodes = new_view - self.Physical_Nodes
		remove_nodes = self.Physical_Nodes - new_view

		# add nodes to ring
		for shard in add_nodes:
			pass

		# remove nodes from ring
		for shard in remove_nodes:
			add = True
			self.reshard(add, shard)

	"""
	Perform a reshard, re-distribute minimun number of keys
	Need to get keys for given node
	"""
	def reshard(self, method, node):
		
		if method:
			pass

		else:
			# re-shard node's keys
			keys = node

	"""
	Prints all events which have occured on this database
	"""
	def print_history(self):
		for event in self.history:
			print(event[0] + ": " + event[1].strftime("%m/%d/%Y, %H:%M:%S"))

	"""
	Prints all events which have occured on this database
	"""
	def return_history(self):
		output = "<center>"
		for event in self.history:
			output = output + event[0] + ": " + event[1].strftime("%m/%d/%Y, %H:%M:%S") + "<br>"
		return output + "</center>"

	"""
	ping another node given address and path 
	"""
	def ping(self, ADDRESS, path, op, keyName, internal):

		ip_port = ADDRESS.split(":")
		endpoint = 'http://' + ip_port[0] + ":" + ip_port[1] + path
		headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
				
		# make recursive type call but to different ip
		# since this is a forward, add the forwarded address to response
		if op == "PUT":
			data = request.get_json()
			payload = json.dumps(data)
			r = requests.put(endpoint, data=payload, headers=headers)
			return make_response(r.content, r.status_code)

		elif op == "GET":
			r = requests.get(endpoint, data=keyName, headers=headers)
			if internal:
				return r.content
			return make_response(r.content, r.status_code)

		elif op == "DELETE":
			r = requests.delete(endpoint, data=keyName, headers=headers)
			return make_response(r.content, r.status_code)

		else:
			return jsonify({
					"error"     : "invalid requests method",
					"message"   : "Error in forward"
			}), 400

	"""
	perform a local database operation
	"""
	def local_operation(self, keyName):
		op = request.method
		
		if op == "PUT":
			data = request.get_json()
			value = data.get("value")
			return self.insertKey(keyName, value)

		elif op == "GET":
			return self.readKey(keyName)

		elif op == "DELETE":
			return self.removeKey(keyName)

		else:
			return jsonify({
					"error"     : "invalid requests method",
					"message"   : "Error in exec_op"
			}), 400




		
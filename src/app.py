'''
app.py defines the network nodes that listen to requests from
clients and other nodes int the system.
'''

from flask import Flask, request, jsonify, make_response
import json
import os
import requests
from node import Node

app = Flask(__name__)

@app.route("/")
def root():
	return '''<Home>: |\n
					  | CS 138: Assignment 4'''

'''
get the current state of this node
'''
@app.route("/kv-store/state", methods=["GET"])
def state():
	state = shard.state_report()
	str_state = json.dumps(state)
	json_state = json.loads(str_state)
	
	return jsonify({
				"node state:"     : json_state
	}), 200

'''
total system reset, wipe all local databases: only used in dubugging mode
'''
@app.route("/kv-store/system/reset", methods=["DELETE"])
def total_reset():
	data = request.get_json()
	forward = data.get('forward')

	if forward == "true":
		all_nodes = shard.all_nodes()

		for node in all_nodes:
			if node == shard.ADDRESS:
				continue
			
			path = '/kv-store/system/reset'
			forward = False
			data = json.dumps({"forward":"false"})
			router.DELETE(node, path, data, forward)

	shard.keystore = {}

	return jsonify({
				"System:"     : "all keys deleted"
	}), 200

'''
get/put/delete key for shard
'''
@app.route("/kv-store/keys/<keyName>", methods=["GET", "PUT", "DELETE"])
def update_keys(keyName):

	# get the shard address that is storing this key, or new shard
	key_shard = shard.find_match(keyName)

	# we have the key locally
	if (key_shard == shard.ADDRESS):
		method = request.method
		return local_operation(method, keyName)

	else:
		path = '/kv-store/keys/'+keyName
		method = request.method
		data = None

		return router.FORWARD(key_shard, method, path, keyName, data)

'''
internal key transfer
'''
@app.route("/kv-store/internal/keys/<keyName>", methods=["GET", "PUT", "DELETE"])
def key_transfer(keyName):
	method = request.method
	return local_operation(method, keyName)

'''
get number of keys in system
'''
@app.route("/kv-store/key-count", methods=["GET"])
def get_key_count():

	all_nodes = shard.all_nodes()
	path = '/kv-store/internal/key-count'
	keys = shard.numberOfKeys()

	for node in all_nodes:
		if node == shard.ADDRESS:
			continue

		# message node and ask them how many nodes you have
		# we want forward to return response content not response
		forward = False
		data = None

		# ADDRESS, PATH, OP, keyName, DATA, FORWARD
		res, status_code = router.GET(node, path, data, forward)
		jsonResponse = json.loads(res.decode('utf-8'))
		keys += (jsonResponse['key_count'])

	return jsonify({
				"key-count"     : {
					"message"   : "Key count retrieved successfully",
					"key-count" : keys
				}
	}), 200 

'''
Internal messaging endpoint so that we can determine if a client or 
node is pinging us
'''
@app.route("/kv-store/internal/key-count", methods=["GET"])
def internal_key_count():
	key_count = shard.numberOfKeys()

	return jsonify({
				"key_count"     : key_count
	}), 200

'''
Change our current view and re-shard keys
Before we re-shard, make sure new node is up 
'''
@app.route("/kv-store/view-change", methods=["PUT"])
def new_view():

	path = '/kv-store/internal/view-change'
	method = 'PUT'
	data = request.get_json()
	view = data.get('view')
	forward = False

	state = {}

	# let all nodes know off the view change
	all_nodes = view.split(',')
	for node in all_nodes:
		if node == shard.ADDRESS:
			continue

		res, status_code = router.PUT(node, path, view, forward)
		Response = json.loads(res.decode('utf-8'))
		address = Response['ADDRESS']
		keys = Response['keys']
		state[address] = keys

	address, keys = shard.view_change(view.split(','))
	state[address] = keys

	response = {}
	response['view-change'] = {"message": "View change successful", "shards": []}

	# format response
	for address in state:
		response['view-change']['shards'].append({"address": address, "key-count": state[address]})

	json_res = json.dumps(response)

	return json_res, 200

'''
internal endpoint for viewchange
'''
@app.route("/kv-store/internal/view-change", methods=["PUT"])
def spread_view():
	
	view = (request.get_data().decode('utf8')).split(',')
	address, keys = shard.view_change(view)

	return jsonify({
			'new_view'     : view,
			'ADDRESS'	   : address,
			'keys' 		   : keys
	}), 200

'''
perfrom operation on node's local database
'''
def local_operation(method, keyName):
	
	if method == 'PUT':
		data = request.get_json()
		value = data.get('value')
		return shard.insert_key(keyName, value)

	elif method == 'GET':
		return shard.read_key(keyName)

	elif method == 'DELETE':
		return shard.remove_key(keyName)

	else:
		return jsonify({
				'error'     : 'invalid requests method',
				'message'   : 'Error in exec_op'
		}), 400

'''
defines routing methods including GET, PUT, DELETE, and a general FORWARD
'''
class Message():
	def __init__(self):
		self.methods = ['GET', 'POST', 'DELETE']

	def base(self, address, path):
		ip_port = address.split(':')
		endpoint = 'http://' + ip_port[0] + ':' + ip_port[1] + path
		headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
		return endpoint, headers

	def GET(self, address, path, query, forward):
		
		endpoint, header = self.base(address, path)

		r = requests.get(endpoint, data=query, headers=header)

		# we want content not response
		if forward:
			return make_response(r.content, r.status_code)
		return r.content, r.status_code


	def PUT(self, address, path, data, forward):
		
		endpoint, header = self.base(address, path)

		if data == None:
			data = request.get_json() 
			data = json.dumps(data) 

		r = requests.put(endpoint, data=data, headers=header)

		if forward:
			return make_response(r.content, r.status_code)
		return r.content, r.status_code

	def DELETE(self, address, path, query, forward):
		
		endpoint, header = self.base(address, path)

		r = requests.delete(endpoint, data=query, headers=header)
		
		if forward:
			return make_response(r.content, r.status_code)
		return r.content, r.status_code

	def FORWARD(self, address, method, path, query, data):
		
		forward = False

		if method == 'GET':
			res, status_code = self.GET(address, path, query, forward)
			r_dict = json.loads(res.decode('utf-8'))
			if "get-key" in r_dict:
				r_dict["get-key"]["address"] = address

		elif method == 'PUT':
			res, status_code =  self.PUT(address, path, data, forward)
			r_dict = json.loads(res.decode('utf-8'))
			if "insert-key" in r_dict:
				r_dict["insert-key"]["address"] = address
			elif "update-key" in r_dict:
				r_dict["update-key"]["address"] = address

		elif method == 'DELETE':
			res, status_code = self.DELETE(address, path, query, forward)
			r_dict = json.loads(res.decode('utf-8'))
			if "delete-key" in r_dict:
				r_dict["delete-key"] = address

		else:
			return jsonify({
				'error'     : 'invalid requests method',
				'message'   : 'Error in exec_op'
			}), 400

		return make_response(r_dict, status_code)

'''
run the servers and extract instance metadata
'''
if __name__ == "__main__":

	# exract view and ip
	VIEW_STR = os.environ["VIEW"]
	views = VIEW_STR.split(",")
	ADDRESS = os.environ["ADDRESS"]

	# create initial shard for this node, hash this shard
	router = Message()
	shard = Node(router, ADDRESS, views)

	app.run(host='0.0.0.0', port=13800, debug=True)







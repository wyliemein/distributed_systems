# listen to requests clients and internal shards

from flask import Flask, request, jsonify, make_response
import json
import os
import requests

# import shard class
from shard import Partition

app = Flask(__name__)

@app.route("/")
def root():
	return "Home: CS 138: Assignment 3"

# get the current state
@app.route("/kv-store/state", methods=["GET"])
def state():
	state = shard.state_report()
	str_state = json.dumps(state)
	json_state = json.loads(str_state)
	
	return jsonify({
				"node state:"     : json_state
	}), 200

# get/put/delete key for shard
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
		forward = True
		data = None

		return router.FORWARD(key_shard, method, path, keyName, data, forward)

# get number of keys in system
@app.route("/kv-store/key-count", methods=["GET"])
def get_key_count():

	all_nodes = shard.all_nodes()
	path = '/kv-store/internal/key-count'
	method = 'GET'
	keys = shard.numberOfKeys()

	for node in all_nodes:
		if node == shard.ADDRESS:
			continue

		# message node and ask them how many nodes you have
		# we want forward to return response content not response
		forward = False
		data = None

		# ADDRESS, PATH, OP, keyName, DATA, FORWARD
		res = router.GET(node, path, data, forward)
		jsonResponse = json.loads(res.decode('utf-8'))
		keys += jsonResponse['key_count']

	return make_response({"keys: ":keys}, 200)

# internal messaging endpoint so that we can determine if a client or node is pinging us
@app.route("/kv-store/internal/key-count", methods=["GET"])
def internal_key_count():
	key_count = shard.numberOfKeys()

	return jsonify({
				"key_count"     : key_count
	}), 200

# change our current view, repartition keys
# before we send keys, make sure new node is up 
@app.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	
	path = '/kv-store/internal/view-change'
	method = 'PUT'
	data = request.get_json()
	view = data.get('view')
	forward = True

	all_nodes = shard.all_nodes()
	for node in all_nodes:
		if node == shard.ADDRESS:
			continue

		res = router.PUT(node, path, view, forward)
		#jsonResponse = json.loads(res.decode('utf-8'))
		#view = jsonResponse['new_view']
		if res.status_code != 200:
			return make_response("error in spreading new new view", 500)

	shard.view_change(view.split(','))
	
	return jsonify({
			'new_view'     : view
	}), 200

# internal endpoint for viewchange
@app.route("/kv-store/internal/view-change", methods=["PUT"])
def spread_view():
	
	view = (request.get_data().decode('utf8')).split(',')
	shard.view_change(view)

	return jsonify({
			'new_view'     : view[0]
	}), 200

# perfrom operation on node's shard
def local_operation(self, keyName):
	method = request.method
	
	if method == 'PUT':
		data = request.get_json()
		value = data.get('value')
		return shard.insertKey(keyName, value)

	elif method == 'GET':
		return shard.readKey(keyName)

	elif method == 'DELETE':
		return shard.removeKey(keyName)

	else:
		return jsonify({
				'error'     : 'invalid requests method',
				'message'   : 'Error in exec_op'
		}), 400


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
		return r.content


	def PUT(self, address, path, data, forward):
		
		endpoint, header = self.base(address, path)

		if data == None:
			data = request.get_json() 
			data = json.dumps(data) 

		r = requests.put(endpoint, data=data, headers=header)

		if forward:
			return make_response(r.content, r.status_code)
		return r.content

	def DELETE(self, address, path, query, forward):
		
		endpoint, header = self.base(address, path)

		r = requests.delete(endpoint, data=query, headers=header)
		
		if forward:
			return make_response(r.content, r.status_code)
		return r.content

	def FORWARD(self, address, method, path, query, data, forward):
		if method == 'GET':
			return self.GET(address, path, query, forward)

		elif method == 'PUT':
			return self.PUT(address, path, data, forward)

		elif method == 'DELETE':
			return self.DELETE(address, path, query, forward)

		else:
			return jsonify({
				'error'     : 'invalid requests method',
				'message'   : 'Error in exec_op'
			}), 400

# run the servers
if __name__ == "__main__":

	# exract view and ip
	VIEW_STR = os.environ["VIEW"]
	views = VIEW_STR.split(",")
	ADDRESS = os.environ["ADDRESS"]

	# create initial shard for this node, hash this shard
	router = Message()
	shard = Partition(router, ADDRESS, views)

	app.run(host='0.0.0.0', port=13800, debug=True)







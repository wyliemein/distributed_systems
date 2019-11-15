# listen to requests clients and internal shards

from flask import Flask, request, jsonify, make_response
import json
import os
import requests
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
import http.client
import socket

# import shard class
from Node import Node

app = Flask(__name__)

node = Node()

@app.route("/")
def root():
	return node.returnInfo()

# get/put/delete key for shard
@app.route("/kv-store/keys/<keyName>", methods=["GET", "PUT", "DELETE"])
def update_keys(keyName):
	data = request.get_json()
	if (data is None):
		data = {
			"forward" : False
		}
	if (request.method == "GET"):
		if node.containsKey(keyName):
			return node.readKey(keyName, data["forward"])
		else:
			correctNode = node.hashKey(keyName)
			data["forward"] = True
			forward = True
			return router.GET(node.others[correctNode], "/kv-store/keys/"+keyName, data, forward)
	elif (request.method == "PUT"):
		if (node.keyBelongsHere(keyName)):
			return node.insertKey(keyName, data.get("value"), ("forward" in data))
		# key doesnt belong here
		data["forward"] = True
		correctNode = node.hashKey(keyName)
		#print(data, file=sys.stderr)
		forward = True
		return router.PUT(node.others[correctNode], "/kv-store/keys/"+keyName, data, forward)
	elif (request.method == "DELETE"):
		if (data is None):
				data = {
					"forward" : False
				}
		if node.containsKey(keyName):
			return node.removeKey(keyName, data["forward"])
		else:
			correctNode = node.hashKey(keyName)
			data["forward"] = True
			forward = True
			return router.DELETE(node.others[correctNode], "/kv-store/keys/"+keyName, data, forward)
	else:
		return jsonify({
			"error"		: "somethings not right"
		}), 400

# get number of keys in system
@app.route("/kv-store/key-count", methods=["GET"])
def get_key_count():

	all_nodes = node.all_nodes()
	path = '/kv-store/internal/key-count'
	op = "GET"
	keys = node.numberOfKeys()

	for shard in all_nodes:
		if shard == node.IPAddress:
			continue

		# message node and ask them how many nodes you have
		# we want forward to return response content not response
		forward = False
		data = None

		# ADDRESS, PATH, OP, keyName, DATA, FORWARD
		res = router.GET(shard, path, data, forward)
		jsonResponse = json.loads(res.decode('utf-8'))
		keys += jsonResponse['key_count']

	return make_response({"keys: ":keys}, 200)

# internal messaging endpoint so that we can determine if a client or node is pinging us
@app.route("/kv-store/internal/key-count", methods=["GET"])
def internal_key_count():
	key_count = node.numberOfKeys()

	return jsonify({
				"key_count"     : key_count
	}), 200

# change our current view, repartition keys
@app.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	data = request.get_json()
	reshardResult = node.reshard(data["view"])
	for result in reshardResult:
		if (bool(result[1]) == True):
			forward = True
			router.PUT(result[0],"/kv-store/view-change", {
				"sender"	: node.IPAddress,
				"view"		: data["view"],
				"keys"		: result[1]
			}, forward)
	if ("keys" in data):
		for key in data["keys"]:
			node.insertKey(key, data["keys"][key], False)

	return make_response("Done", 200)

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
	# exract view and ip from environment
	VIEW_STR = os.environ["VIEW"]
	ADDRESS = os.environ["ADDRESS"]
	node.setInfo(ADDRESS, VIEW_STR.split(","))
	# others = VIEW_STR.split(",")
	
	router = Message()

	app.run(host='0.0.0.0', port=13800, debug=True, threaded=True)



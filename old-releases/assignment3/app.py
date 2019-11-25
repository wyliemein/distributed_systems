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
			return forward(node.others[correctNode], "/kv-store/keys/"+keyName, "GET", data)
	elif (request.method == "PUT"):
		if ("forward" in data):
			return node.insertKey(keyName, data.get("value"), ("forward" in data))
		if (node.keyBelongsHere(keyName)):
			return node.insertKey(keyName, data.get("value"), ("forward" in data))
		# key doesnt belong here
		data["forward"] = True
		correctNode = node.hashKey(keyName)
		print(str(correctNode) + str(data), file=sys.stderr)
		return forward(node.others[correctNode], "/kv-store/keys/"+keyName, "PUT", data)
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
			return forward(node.others[correctNode], "/kv-store/keys/"+keyName, "DELETE", data)
	else:
		return jsonify({
			"error"		: "somethings not right"
		}), 400

	# get the shard that is storing this key, or new shard
	"""key_shard = '10.10.0.3:13800' #shard.key_op(keyName)

	op = request.method
	data = request.get_json()

	# we have the key locally
	if (key_shard.IP == shard.IP):
		return exec_op(op, data)

	else:
	path = '/kv-store/keys/'+keyName
	return forward(key_shard, path, op, data)"""

# get number of keys in system
@app.route("/kv-store/key-count", methods=["GET"])
def get_key_count():
	keycount = node.numberOfKeys()
	for ip in node.others:
		if (ip != node.IPAddress):
			ip_port = ip.split(":")
			endpoint = 'http://' + ip_port[0] + ":" + ip_port[1] + "/kv-store/key-count/internal"
			headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
			r = requests.get(endpoint, headers=headers)
			response_dict = r.json()
			keycount = keycount + response_dict["key-count"]
	return jsonify({
		"message"	: "Key count retrieved successfully",
		"key-count"	: keycount
	}), 200

@app.route("/kv-store/key-count/internal", methods=["GET"])
def get_key_count_internal():
	return jsonify({
		"key-count"	: node.numberOfKeys()
	}), 200
# change our current view, repartition keys
@app.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	data = request.get_json()
	reshardResult = node.reshard(data["view"])
	for result in reshardResult:
		if (bool(result[1]) == True):
			forward(result[0],"/kv-store/view-change","PUT", {
				"sender"	: node.IPAddress,
				"view"		: data["view"],
				"keys"		: result[1]
			})
	if ("keys" in data):
		for key in data["keys"]:
			node.insertKey(key, data["keys"][key], False)
	resultList = []
	for ip in node.others:
		if (ip != node.IPAddress):
			ip_port = ip.split(":")
			endpoint = 'http://' + ip_port[0] + ":" + ip_port[1] + "/kv-store/key-count/internal"
			headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
			r = requests.get(endpoint, headers=headers)
			response_dict = r.json()
			resultList.append({
				"address"	: ip,
				"key-count"	: response_dict["key-count"]
			})
		else:
			resultList.append({
				"address"	: node.IPAddress,
				"key-count"	: node.numberOfKeys()
			})
	return jsonify({
		"message"	: "View change successful",
		"shards"	: resultList
	}), 200

# forward 
def forward(ADDRESS, path, method, data):

	ip_port = ADDRESS.split(":")
	endpoint = 'http://' + ip_port[0] + ":" + ip_port[1] + path
	headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
			
	# make recursive type call but to different ip
	if method == "PUT":
		print(endpoint, file=sys.stderr)
		r = requests.put(endpoint, json=data, headers=headers)
		return make_response(r.content, r.status_code)
	elif method == "GET":
		r = requests.get(endpoint, json=data, headers=headers)
		return make_response(r.content, r.status_code)
	elif method == "DELETE":
		r = requests.delete(endpoint, json=data, headers=headers)
		return make_response(r.content, r.status_code)
	else:
		return jsonify({
				"error"     : "invalid requests method",
				"message"   : "Error in forward"
		}), 400

def exec_op(method, data):
	pass


# run the servers
if __name__ == "__main__":
	# exract view and ip from environment
	VIEW_STR = os.environ["VIEW"]
	ADDRESS = os.environ["ADDRESS"]
	node.setInfo(ADDRESS, VIEW_STR.split(","))
	# others = VIEW_STR.split(",")
	
	# node.setIp(ADDRESS)
	# create initial shard for this node, hash this shard
	# shard = Partition(ADDRESS, views)

	app.run(host='0.0.0.0', port=13800, debug=True, threaded=True)







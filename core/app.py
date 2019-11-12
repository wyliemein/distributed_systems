# listen to requests clients and internal shards

from flask import Flask, request, jsonify, make_response
import json
import os
import requests
import sys

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

	if (request.method == "GET"):
		if (data is None):
				data = {
					"forward" : False
				}
		if node.containsKey(keyName):
			return node.readKey(keyName, data["forward"])
		else:
			correctNode = node.hashKey(keyName)
			data["forward"] = True
			internal = False
			return forward(node.others[correctNode], "/kv-store/keys/"+keyName, "GET", data, internal)

	elif (request.method == "PUT"):
		if (node.keyBelongsHere(keyName)):
			return node.insertKey(keyName, data.get("value"), ("forward" in data))
		# key doesnt belong here
		data["forward"] = True
		correctNode = node.hashKey(keyName)
		print(data, file=sys.stderr)
		internal = False
		return forward(node.others[correctNode], "/kv-store/keys/"+keyName, "PUT", data, internal)

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
			internal = False
			return forward(node.others[correctNode], "/kv-store/keys/"+keyName, "DELETE", data, internal)
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
		internal_request = True
		res = forward(shard, path, op, 'key_count', internal_request)
		jsonResponse = json.loads(res.decode('utf-8'))
		keys += jsonResponse['keys']

	return make_response({"keys: ":keys}, 200)

# internal messaging endpoint so that we can determine if a client or node is pinging us
@app.route("/kv-store/internal/key-count", methods=["GET"])
def internal_key_count():
	key_count = node.numberOfKeys()

	return jsonify({
				"keys"     : key_count
	}), 200

# change our current view, repartition keys
@app.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	# call node.view_change(new_view)
	return True

# forward 
def forward(ADDRESS, path, op, keyName, internal):

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


# run the servers
if __name__ == "__main__":
	# exract view and ip from environment
	VIEW_STR = os.environ["VIEW"]
	ADDRESS = os.environ["ADDRESS"]
	node.setInfo(ADDRESS, VIEW_STR.split(","))
	# others = VIEW_STR.split(",")


	app.run(host='0.0.0.0', port=13800, debug=True)


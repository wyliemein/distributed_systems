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

# get/put/delete key for shard
@app.route("/kv-store/keys/<keyName>", methods=["GET", "PUT", "DELETE"])
def update_keys(keyName):

	# get the shard address that is storing this key, or new shard
	key_shard = shard.key_op(keyName)

	# we have the key locally
	if (key_shard == shard.ADDRESS):
		return exec_op(keyName)

	else:
		path = '/kv-store/keys/'+keyName
		op = request.method
		internal_request = False
		return forward(key_shard, path, op, keyName, internal_request)

# get number of keys in system
@app.route("/kv-store/key-count", methods=["GET"])
def get_key_count():

	all_nodes = shard.all_nodes()
	path = '/kv-store/internal/key-count'
	op = "GET"
	keys = shard.numberOfKeys()

	for node in all_nodes:
		if node == shard.ADDRESS:
			continue

		# message node and ask them how many nodes you have
		# we want forward to return response content not response
		internal_request = True
		res = forward(node, path, op, 'key_count', internal_request)
		jsonResponse = json.loads(res.decode('utf-8'))
		keys += jsonResponse['keys']

	return make_response({"keys: ":keys}, 200)

# internal messaging endpoint so that we can determine if a client or node is pinging us
@app.route("/kv-store/internal/key-count", methods=["GET"])
def internal_key_count():
	key_count = shard.numberOfKeys()

	return jsonify({
				"keys"     : key_count
	}), 200

# change our current view, repartition keys
@app.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	# call shard.view_change(new_view)
	pass

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

# perform key operation with local database
def exec_op(keyName):
	op = request.method
	
	if op == "PUT":
		data = request.get_json()
		value = data.get("value")
		return shard.insertKey(keyName, value)

	elif op == "GET":
		return shard.readKey(keyName)

	elif op == "DELETE":
		return shard.removeKey(keyName)

	else:
		return jsonify({
				"error"     : "invalid requests method",
				"message"   : "Error in exec_op"
		}), 400


# run the servers
if __name__ == "__main__":

	# exract view and ip
	VIEW_STR = os.environ["VIEW"]
	views = VIEW_STR.split(",")
	ADDRESS = os.environ["ADDRESS"]

	# create initial shard for this node, hash this shard
	shard = Partition(ADDRESS, views)

	app.run(host='0.0.0.0', port=13800, debug=True)







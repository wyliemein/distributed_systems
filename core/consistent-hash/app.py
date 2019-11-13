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
		return shard.local_operation(keyName)

	else:
		path = '/kv-store/keys/'+keyName
		op = request.method
		internal_request = False
		return shard.ping(key_shard, path, op, keyName, internal_request)

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
		res = shard.ping(node, path, op, 'key_count', internal_request)
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

# internal messaging endpoint to get all keys
@app.route("/kv-store/internal/all-keys", methods=["GET"])
def all_keys():
	key_val = shard.allKeys()

	return jsonify({
				"all_keys"     : key_val
	}), 200

# change our current view, repartition keys
@app.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	# call shard.view_change(new_view)
	pass

# run the servers
if __name__ == "__main__":

	# exract view and ip
	VIEW_STR = os.environ["VIEW"]
	views = VIEW_STR.split(",")
	ADDRESS = os.environ["ADDRESS"]

	# create initial shard for this node, hash this shard
	shard = Partition(ADDRESS, views)
	#print(shard.state_report())

	app.run(host='0.0.0.0', port=13800, debug=True)







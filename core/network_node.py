# network_node.py
# listen to requests

from flask import Flask, request, jsonify, make_response
import json
import os
import requests

# import shard class
from shard import Partition

node = Flask(__name__)
@node.route("/")
def root():
	return "Home: CS 138: Assignment 3"

# get/put/delete key for shard
@node.route("/kv-store/keys/<keyName>", methods=["GET", "PUT", "DELETE"])
def update_keys(keyName):
	'''
		The main Key-Value store endpoint
		: GET		-> return the value for the key
		: PUT		-> add or update a key
		: DELETE	-> delete a key
	'''
	if (request.method == "GET"):
		pass

	elif (request.method == "PUT"):
		#data = request.get_json()
		#return shard.insertKey(keyName, data.get("value"))
		pass

	elif (request.method == "DELETE"):
		#return shard.removeKey(keyName)
		pass

	else:
		return jsonify({
			"error"		: "somethings not right"
		}), 400

# get number of keys in system
@node.route("/kv-store/key-count", methods=["GET"])
def get_key_count():
	return jsonify({
			"not implemented"		: "just a test"
		}), 200

# change our current view, repartition keys
@node.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	pass

# run the servers
if __name__ == "__main__":

	# exract view and ip
	VIEW_STR = os.environ["VIEW"]
	views = VIEW_STR.split(",")
	IP = os.environ["ADDRESS"]

	# create initial shard for this node, hash this shard
	shard = Partition(IP, views)

	node.run(host='0.0.0.0', port=13800, debug=True)







# network_node.py
# listen to requests

from flask import Flask, request, jsonify, make_response
import json
from Database import Database
import os
import requests

# import shard class
from shard import Partition

shard = Partition()
node = Flask(__name__)

@node.route("/")
def root():
	return "Home: CS 138: Assignment 3"

# get/put/delete key for shard
@node.route("/kv-store/keys/<keyName>", methods=["GET", "PUT", "DELETE"])
def kvstore(keyName):
	'''
		The main Key-Value store endpoint
		: GET		-> return the value for the key
		: PUT		-> add or update a key
		: DELETE	-> delete a key
	'''
	if (request.method == "GET"):
		return shard.readKey(keyName)
	elif (request.method == "PUT"):
		data = request.get_json()
		return shard.insertKey(keyName, data.get("value"))
	elif (request.method == "DELETE"):
		return shard.removeKey(keyName)
	else:
		return jsonify({
			"error"		: "somethings not right"
		}), 400

# get number of keys in system
@node.route("/kv-store/key-count", methods=["GET"])
def get_key_count():
	pass

# change our current view
@node.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	pass


# run the servers
if __name__ == "__main__":
	# run it on our localhost with a port of 8081 with debugging enabled so we can auto restart when changes are made
	node.run(host='0.0.0.0', port=13800, debug=True)
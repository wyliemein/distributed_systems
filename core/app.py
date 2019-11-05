# network_node.py
# listen to requests

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
	
	if (request.method == "GET"):
		return jsonify({
			"not implemented"		: "just a test"
		}), 200

	elif (request.method == "PUT"):
		return jsonify({
			"not implemented"		: "just a test"
		}), 200

	elif (request.method == "DELETE"):
		return jsonify({
			"not implemented"		: "just a test"
		}), 200

	else:
		return jsonify({
			"error"		: "somethings not right"
		}), 400

# get number of keys in system
@app.route("/kv-store/key-count", methods=["GET"])
def get_key_count():
	return jsonify({
			"not implemented"		: "just a test"
		}), 200

# change our current view, repartition keys
@app.route("/kv-store/view-change", methods=["PUT"])
def new_view():
	return jsonify({
			"not implemented"		: "just a test"
		}), 200

# run the servers
if __name__ == "__main__":

	# exract view and ip
	VIEW_STR = os.environ["VIEW"]
	views = VIEW_STR.split(",")
	IP_PORT = os.environ["ADDRESS"].split(":")

	# create initial shard for this node, hash this shard
	shard = Partition(IP_PORT[0], views)

	app.run(host='0.0.0.0', port=13800, debug=True)







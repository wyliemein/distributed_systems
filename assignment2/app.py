# Robert Crosby
# rncrosby@ucsc.edu
# 1529995
# Assignment 2
# Tuesday, October 8 @ 12:11 PM

from flask import Flask, request, jsonify
import json
import os

keyStore = {}

# name our application
app = Flask(__name__)

# respond to calls to the root of our app
@app.route("/")
def root():
	return "CS 138: Assignment 2"

@app.route("/kv-store/<keyName>", methods=["GET", "PUT", "DELETE"])
def kvstore(keyName):

	# determine node type
	if "FORWARDING_ADDRESS" in (os.environ):
		# I am a follower process, route requests to main
		pass
	else:
		# I am the main process, respond to client to route to follower
		pass

	data = request.get_json()

	if (request.method == "PUT"):

		# put request, ensure theres a value for the key
		if len(data) == 0:
			return jsonify({"error":"Value is missing","message":"Error in PUT"}), 400

		try:
			if keyName in keyStore:
				keyStore[keyName] = data["value"]
				return jsonify({"message":"Updated successfully","replaced":"true"}), 201
			keyStore[keyName] = data["value"]
			return jsonify({"message":"Added successfully","replaced":"false"}), 201
		except:
			return jsonify({"message":"Added successfully","replaced":"false"}), 201

	elif (request.method == "GET"):
		try:
			return jsonify({"message":"Added successfully","replaced":"false"}), 201
		except:
			return jsonify({"message":"Added successfully","replaced":"false"}), 201
	return 



# run the server
if __name__ == "__main__":
	# run it on our localhost with a port of 8081 with debugging enabled so we can auto restart when changes are made
	app.run(host='0.0.0.0', port=13800, debug=True)


# Robert Crosby
# Cody Hartsook
# Gardner Mein
# Contributors: Robert Crosby, Cody Hartsook, Wylie

from flask import Flask, request, jsonify, make_response
import json
import os
import requests

keyStore = {}

# name our application
app = Flask(__name__)

# respond to calls to the root of our app
@app.route("/")
def root():
	return "Home: CS 138: Assignment 2"

# client side enpoint
@app.route("/kv-store/<keyName>", methods=["GET", "PUT", "DELETE"])
def kvstore(keyName):
	# determine node type, either main-instance or follower-instance
	STATE = ""

	if "FORWARDING_ADDRESS" in (os.environ):
		# I am a follower process, route requests to main
		STATE = "follower" 
		ip_port = str(os.environ["FORWARDING_ADDRESS"]).split(":")

	else:
		# I am the main process, respond to client
		STATE = "main"

	# recieve request from client
	if (request.method == "PUT"):
		data = request.get_json()
		keyValue = data.get("value")

		if STATE == "main":
			return main_storeValue(keyName, keyValue)

		else:
			# get value from main instance keyStore/send to main
			endpoint = 'http://' + ip_port[0] + ":" + ip_port[1] + '/kv-store/' + keyName
			headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
			payload = json.dumps(data)
			
			# make recursive type call but to different ip
			r = requests.put(endpoint, data=payload, headers=headers)
			return make_response(r.content, r.status_code)

	elif (request.method == "GET"):

		if STATE == "main":
			# get value from local keyStore
			return main_retrieveValue(keyName)
		else:
			# get value from main instance keyStore/send to main
			endpoint = 'http://' + ip_port[0] + ":" + ip_port[1] + '/kv-store/' + keyName
			headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
			payload = keyName
			
		 	# make recursive type call but to different ip
			r = requests.put(endpoint, data=payload, headers=headers)
			return make_response(r.content, r.status_code)
	
	elif (request.method == "DELETE"):

		if STATE == "main":
			return main_deleteKey(keyName)
		else:
			# delete from main
			endpoint = 'http://' + ip_port[0] + ":" + ip_port[1] + '/kv-store/' + keyName
			headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
			payload = keyName
			
		 	# make recursive type call but to different ip
			r = requests.put(endpoint, data=payload, headers=headers)
			return make_response(r.content, r.status_code)
	
def main_storeValue(key,value):
	if (len(value) > 50):
		# too long
		return jsonify({
			"error"		: "Key is too long",
			"message"	: "Error in PUT"
		}), 400
	elif key in keyStore:
		# updated
		keyStore[key] = value
		return jsonify({
			"message"	: "Updated successfully",
			"replaced"	: True
		}), 200
	else:
		keyStore[key] = value
		return jsonify({
			"message"	: "Added successfully",
			"replaced"	: False
		}), 201

def main_retrieveValue(key):
	if (key in keyStore):
		return jsonify({
			"doesExist"	: True,
			"message"	: "Retrieved successfully",
			"value"		: keyStore[key]
		}), 200
	else:
		return jsonify({
			"doesExist"	: False,
			"error"		: "Key does not exist",
			"message"	: "Error in GET"
		}), 404

def main_deleteKey(key):
	if (key in keyStore):
		del keyStore[key]
		return jsonify({
			"doesExist"	: True,
			"message"	: "Deleted successfully",
		}), 200
	else:
		return jsonify({
			"doesExist"	: False,
			"error"		: "Key does not exist",
			"message"	: "Error in DELETE"
		}), 404
	


# run the server
if __name__ == "__main__":
	# run it on our localhost with a port of 8081 with debugging enabled so we can auto restart when changes are made
	app.run(host='0.0.0.0', port=13800, debug=True)











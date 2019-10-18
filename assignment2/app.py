# Contributors: Robert Crosby, Cody Hartsook, Wylie
# rncrosby@ucsc.edu
# 1529995
# Assignment 2
# Tuesday, October 8 @ 12:11 PM

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
	return "CS 138: Assignment 2"

# endpoint that recieves internal messages between main and follower
@app.route("/kv-store/route/<message>", methods=["GET", "PUT", "DELETE"])
def recieve(message):
	return "internal message recieved"

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
	data = request.get_json()

	if (request.method == "PUT"):
		# put request, ensure theres a value for the key
		if len(data) == 0:
			return jsonify({"error":"Value is missing","message":"Error in PUT"}), 400

		if STATE == "main":
			# put value in local keyStore
			if keyName in keyStore:
				keyStore[keyName] = data["value"]
				return jsonify({"message":"Updated successfully","replaced":"true"}), 201

			keyStore[keyName] = data["value"]
			return jsonify({"message":"Added successfully","replaced":"false"}), 201

		else:
			# put keyName:data in keyStore of main
			# request endpoint of main
			#print("FORWARDING_ADDRESS:", ip_port[0], " -- ", ip_port[1])
			endpoint = 'http://' + ip_port[0] + ":" + ip_port[1] + '/kv-store/' + keyName
			headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
			payload = data['value']
			r = requests.post(endpoint, data=payload, headers=headers)
			
			return r, 201
			#return jsonify({"FORWARDING_ADDRESS":ip_port[0],"port":ip_port[1], "url":endpoint}), 201


	elif (request.method == "GET"):

		if STATE == "main":
			# get value from local keyStore

			try:
				return jsonify({"message":"Added successfully","replaced":"false"}), 201
			except:
				return jsonify({"message":"Added successfully","replaced":"false"}), 201

		else:
			# get value from main instance keyStore/send to main
			pass
	
	elif (request.method == "DELETE"):

		if STATE == "main":
			# delete value from local
			pass

		else:
			# delete from main
			pass


# run the server
if __name__ == "__main__":
	# run it on our localhost with a port of 8081 with debugging enabled so we can auto restart when changes are made
	app.run(host='0.0.0.0', port=13800, debug=True)











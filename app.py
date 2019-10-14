# Robert Crosby
# rncrosby@ucsc.edu
# 1529995
# Assignment 1
# Tuesday, October 8 @ 12:11 PM

from flask import Flask, request, jsonify
import json

keyStore = {}

# name our application
app = Flask(__name__)

# respond to calls to the root of our app
@app.route("/")
def root():
	return "CS 138: Assignment 2"

# respond to get and post at /hello
@app.route("/hello", methods=["GET", "POST"])
def hello():
	# check the method
	if (request.method == "POST"):
		# respond with the proper string and error number
		return "This method is unsupported", 405
	# must be a get method, return the correct response
	return "Hello, world!"

# respond to get and post at /check
@app.route("/check", methods=["GET", "POST"])
def check():
	# check the method
	if (request.method == "POST"):
		# use try/except to ensure the proper parameters are recieved and if not dont crash
		try:
			# parse the arguments
			message = request.args.get("msg")
			# return the response
			return "POST message received: " + message
		except:
			# respond with the error
			return "This method is unsupported", 405
	else:
		# GET request response
		return "GET message received"

@app.route("/kv-store/<keyName>", methods=["GET", "PUT", "DELETE"])
def kvstore(keyName):
	data = request.get_json()
	if (request.method == "PUT"):
		# put request, ensure theres a value for the key
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
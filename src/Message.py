from flask import Flask, request, jsonify, make_response
import json
import os
import requests
import time
import timeout_decorator

'''
Defines routing methods including GET, PUT, DELETE, and a general FORWARD
Defines causal objects and provides parsing methods
'''
max_wait = 5

class Router():
	def __init__(self):
		self.methods = ['GET', 'POST', 'DELETE']
		self.max_wait = 5
		self.error_message = 'Timeout'

	# -------------------------------------------------------------------------
	def base(self, address, path):
		ip_port = address.split(':')
		endpoint = 'http://' + ip_port[0] + ':' + ip_port[1] + path
		headers = {'content-type': 'application/json'}
		return endpoint, headers

	# -------------------------------------------------------------------------
	@timeout_decorator.timeout(max_wait, use_signals=False)
	def GET(self, address, path, data):
		
		endpoint, header = self.base(address, path)

		r = requests.get(endpoint, json=data, headers=header)

		return r.get_json(), r.status_code


	# -------------------------------------------------------------------------
	@timeout_decorator.timeout(max_wait, use_signals=False)
	def PUT(self, address, path, data):
		
		endpoint, header = self.base(address, path)

		if data == None:
			data = request.get_json() 

		r = requests.put(endpoint, json=data, headers=header)
		return r.get_json(), r.status_code

	# -------------------------------------------------------------------------
	@timeout_decorator.timeout(max_wait, use_signals=False)
	def DELETE(self, address, path, data):
		
		endpoint, header = self.base(address, path)

		r = requests.delete(endpoint, json=data, headers=header)
		
		if forward:
			return make_response(r.content, r.status_code)
		return r.get_json(), r.status_code

	# -------------------------------------------------------------------------
	def FORWARD(self, address, method, path, data):
		if method == "GET":
			return self.GET(address,path,data)
		if method == "PUT":
			return self.PUT(address,path,data)
		if method == "DELETE":
			return self.DELETE(address,path,data)













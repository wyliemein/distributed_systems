from flask import Flask, request, jsonify, make_response
import json
import os
import requests
import time
#from wrapt_timeout_decorator import *

'''
Defines routing methods including GET, PUT, DELETE, and a general FORWARD
Defines causal objects and provides parsing methods
'''
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
	#@timeout(5, use_signals=False)
	def GET(self, address, path, query, forward):
		
		endpoint, header = self.base(address, path)

		r = requests.get(endpoint, data=query, headers=header)

		# we want content not response
		if forward:
			return make_response(r.content, r.status_code)
		return r.content, r.status_code


	# -------------------------------------------------------------------------
	#@timeout(5, use_signals=False)
	def PUT(self, address, path, data, forward):
		
		endpoint, header = self.base(address, path)

		if data == None:
			data = request.get_json() 
			data = json.dumps(data) 

		r = requests.put(endpoint, data=data, headers=header)

		if forward:
			return make_response(r.content, r.status_code)
		return r.content, r.status_code

	# -------------------------------------------------------------------------
	#@timeout(5, use_signals=False)
	def DELETE(self, address, path, query, forward):
		
		endpoint, header = self.base(address, path)

		r = requests.delete(endpoint, data=query, headers=header)
		
		if forward:
			return make_response(r.content, r.status_code)
		return r.content, r.status_code

	# -------------------------------------------------------------------------
	def FORWARD(self, address, method, path, query, data, forward):

		if method == 'GET':
			res, status_code = self.GET(address, path, query, False)
			r_dict = json.loads(res.decode('utf-8'))
			if 'get-key' in r_dict:
				r_dict['get-key']['address'] = address

		elif method == 'PUT':
			res, status_code =  self.PUT(address, path, data, False)
			r_dict = json.loads(res.decode('utf-8'))
			if 'insert-key' in r_dict:
				r_dict['insert-key']['address'] = address
			elif 'update-key' in r_dict:
				r_dict['update-key']['address'] = address

		elif method == 'DELETE':
			res, status_code = self.DELETE(address, path, query, False)
			r_dict = json.loads(res.decode('utf-8'))
			if 'delete-key' in r_dict:
				r_dict['delete-key'] = address

		else:
			return jsonify({
				'error'     : 'invalid requests method',
				'message'   : 'Error in exec_op'
			}), 400

		if forward:
			return make_response(json.dumps(r_dict), status_code)
		else:
			return r_dict, status_code

















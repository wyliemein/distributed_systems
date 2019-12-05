'''
app.py defines the network nodes that listen to requests from
clients and other nodes int the system.
'''

from flask import Flask, request, jsonify, make_response
import json
import os
import requests
from node import Node
from Message import Router
from vectorclock import VectorClock

app = Flask(__name__)

@app.route('/')
def root():
	return '''
		-----------------------------------
		<=| Welcome to the KV_Store API |=>
		-----------------------------------
		     ------------------------
		           ------------
		'''

'''
get the current state of this node
'''
@app.route('/kv-store/state', methods=['GET'])
def state():
	state = shard.state_report()
	str_state = json.dumps(state)
	json_state = json.loads(str_state)
	
	return jsonify({
				'node state:'     : json_state
	}), 200

'''
all production/public endpoints
---------------------------------------------------------------------------------
'''

'''
get number of keys for a node and stored replicas
'''
@app.route('/kv-store/key-count', methods=['GET'])
def get_key_count():

	data = request.get_json()
	causal_obj = data.get('causal-context')

	# this has to be 
	#key_count = shard.numberOfKeys()
	key_count = 0

	return jsonify({
				'key_count'     : key_count
	}), 200

'''
get shard ID and key count for each shard
'''
@app.route('/kv-store/shards', methods=['GET'])
def get_shards():

	all_shards = shard.all_shards()
	path = '/kv-store/key-count'
	keys = shard.numberOfKeys()

	for shard in all_shards:
		if node == shard.shard_ID:
			continue

		# message node and ask them how many nodes you have
		# we want forward to return response content not response
		forward = False
		data = None

		# ADDRESS, PATH, OP, keyName, DATA, FORWARD
		res, status_code = router.GET(node, path, data, forward)
		jsonResponse = json.loads(res.decode('utf-8'))
		keys += (jsonResponse['key_count'])

	return jsonify({
				'key-count'     : {
					'message'   : 'Key count retrieved successfully',
					'key-count' : keys
				}
	}), 200 

'''
get state information for specific shard
'''
@app.route('/kv-store/shards/<id>', methods=['GET'])
def get_shard():
	pass

'''
Change our current view and re-shard keys
Before we re-shard, make sure new node is up 
'''
@app.route('/kv-store/view-change', methods=['PUT'])
def new_view():
	pass

	'''path = '/kv-store/internal/view-change'
	method = 'PUT'
	data = request.get_json()
	view = data.get('view')
	forward = False

	state = {}

	# let all nodes know off the view change
	all_nodes = view.split(',')
	for node in all_nodes:
		if node == shard.ADDRESS:
			continue

		res, status_code = router.PUT(node, path, view, forward)
		Response = json.loads(res.decode('utf-8'))
		address = Response['ADDRESS']
		keys = Response['keys']
		state[address] = keys

	address, keys = shard.view_change(view.split(','))
	state[address] = keys

	response = {}
	response['view-change'] = {'message': 'View change successful', 'shards': []}

	# format response
	for address in state:
		response['view-change']['shards'].append({'address': address, 'key-count': state[address]})

	json_res = json.dumps(response)

	return json_res, 200'''

'''
get/put/delete key for shard
'''
@app.route('/kv-store/keys/<keyName>', methods=['GET', 'PUT', 'DELETE'])
def update_keys(keyName):
	pass

	'''
	# find the shard that is associated with this key
	key_shard = shard.find_match(keyName)

	# we have the key locally
	if (key_shard == shard.shard_ID):
		method = request.method
		return local_operation(method, keyName)

	else:
		path = '/kv-store/keys/'+keyName
		method = request.method
		data = None

		return router.FORWARD(key_shard, method, path, keyName, data)
		'''

'''
all internal endpoints
---------------------------------------------------------------------------------
'''

'''
internal key transfer
'''
@app.route('/kv-store/internal/keys/<keyName>', methods=['GET', 'PUT', 'DELETE'])
def key_transfer(keyName):
	method = request.method
	return local_operation(method, keyName)

'''
internal endpoint for viewchange
'''
@app.route('/kv-store/internal/view-change', methods=['PUT'])
def spread_view():
	pass
	'''view = (request.get_data().decode('utf8')).split(',')
	address, keys = shard.view_change(view)

	return jsonify({
			'new_view'     : view,
			'ADDRESS'	   : address,
			'keys' 		   : keys
	}), 200'''

'''
perfrom operation on node's local key-store
'''
def local_operation(method, keyName):
	
	if method == 'PUT':
		data = request.get_json()
		value = data.get('value')
		return shard.insert_key(keyName, value)

	elif method == 'GET':
		return shard.read_key(keyName)

	elif method == 'DELETE':
		return shard.remove_key(keyName)

	else:
		return jsonify({
				'error'     : 'invalid requests method',
				'message'   : 'Error in exec_op'
		}), 400

'''
run the servers and extract instance metadata
'''
if __name__ == '__main__':

	# exract view and ip
	VIEW_STR = os.environ['VIEW']
	VIEW = VIEW_STR.split(',')
	ADDRESS = os.environ['ADDRESS']
	REPL_FACTOR = int(os.environ['REPL_FACTOR'])

	# create node and message router
	router = Router()
	shard = Node(router, ADDRESS, VIEW, REPL_FACTOR)

	app.run(host='0.0.0.0', port=13800, debug=True)

'''
vc = VectorClock()
vc.appendShard("node1")
vc.increment("node1")
vc.appendShard("node2")
vc.appendShard("node3")
vc.increment("node3")
vc.increment("node3")
vc.printclock()
print("done")
'''

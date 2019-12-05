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
import sys

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

	#data = request.get_json()
	#causal_obj = data.get('causal-context')
	path = '/kv-store/internal/key-count'
	all_replicas = shard.shard_replicas(shard.shard_ID) 
	KEY_COUNT = my_key_count = shard.numberOfKeys()

	for node in all_replicas:
		if node == shard.ADDRESS:
			continue

		# internal request
		forward = False
		data = None

		try:
			res, status_code = router.GET(node, path, data, forward)
		except:
			# node may be down, handle it in node.py
			print('<warning>', node, 'is unresponsive', file=sys.stderr)
			shard.handle_unresponsive_node(node)
			continue

		jsonResponse = json.loads(res.decode('utf-8'))
		rep_key_count = (jsonResponse['key_count'])
		rep_vector_clock = shard.json_to_vc(jsonResponse['VC'])

		if my_key_count != rep_key_count:
			if shard.local_is_greater(rep_vector_clock):
				# should use gossip protocol to let reps know latest state
				pass
			else:
				KEY_COUNT = rep_key_count
				# update my local state and use gossip protocol to let reps know latest state

	return jsonify({
				'key_count'     : KEY_COUNT
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

	path = '/kv-store/internal/view-change'
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

	return json_res, 200

'''
get/put/delete key for shard
'''
@app.route('/kv-store/keys/<keyName>', methods=['GET', 'PUT', 'DELETE'])
def update_keys(keyName):

	try:
		# find the shard that is associated with this key
		key_shard = shard.find_match(keyName)
		all_replicas = shard.shard_replicas(key_shard)

		# we have the key locally
		if (key_shard == shard.shard_ID):
			method = request.method
			print('local operation', file=sys.stderr)
			return self.guarantee_causal_consistency(all_replicas, keyName, method)

		# forward request to another shard
		else:
			path = '/kv-store/keys/'+keyName
			method = request.method
			data = None

			# forward request to replicas in key_shard shard
			for replica in all_replicas:
				try:
					return router.FORWARD(replica, method, path, keyName, data)
				except:
					shard.handle_unresponsive_node(replica)
					continue

			# we have gone through all replicas and none have responded
			if method == 'PUT':
				return jsonify({"error":"Unable to satisfy request","message":"Error in PUT"}), 503
			elif method == 'GET':
				return jsonify({"error":"Unable to satisfy request","message":"Error in GET"}), 503
			else:
				return jsonify({"error":"Unable to satisfy request","message":"Error in DELETE"}), 503

	except Exception as e:
		print('caught exception in kv-store/keys\n', e, file=sys.stderr)

'''
all internal endpoints
---------------------------------------------------------------------------------
'''

'''
internal shard key count
'''
@app.route('/kv-store/internal/key-count', methods=['GET'])
def internal_key_count():

	data = request.get_json()
	causal_obj = data.get('causal-context')
	key_count = shard.numberOfKeys()

	return jsonify({
				'key_count'     : key_count
	}), 200

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

	view = (request.get_data().decode('utf8')).split(',')
	address, keys = shard.view_change(view)

	return jsonify({
			'new_view'     : view,
			'ADDRESS'	   : address,
			'keys' 		   : keys
	}), 200

'''
internal endpoint to gossip/send state to all other replicas
'''
@app.route('/kv-store/internal/gossip', methods=['PUT'])
def shard_gossip():

	all_replicas = shard.shard_replicas(shard.shard_ID)

'''
garantee causal consistency by messaging all replicas and checking vector clocks
'''
def guarantee_causal_consistency(all_replicas, keyName, method):
	path = '/kv-store/internal/keys/'+keyName
	RES = {'message':''}
	max_VC = [0 for len(shard.nodes)]
	
	for replica in all_replicas:
		try:
			res = router.FORWARD(replica, method, path, keyName, data)
			
			# if respondes from replicates are not identical, check vector clocks
			if res['message'] != RES['message']:
				vc = shard.json_to_vc(res['causal-context'])
				if shard.first_is_greater(vc, max_VC):
					RES = res
					max_VC = vc
		except:
			shard.handle_unresponsive_node(replica)
			continue

	# return causally consisten response
	return RES

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
vc.appendShard('node1')
vc.increment('node1')
vc.appendShard('node2')
vc.appendShard('node3')
vc.increment('node3')
vc.increment('node3')
vc.printclock()
print('done')
'''

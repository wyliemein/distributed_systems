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
from vectorclock import VectorClock

app = Flask(__name__)

if __name__ == '__main__':

	# exract view and ip
	VIEW_STR = os.environ['VIEW']
	VIEW = VIEW_STR.split(',')
	ADDRESS = os.environ['ADDRESS']
	REPL_FACTOR = int(os.environ['REPL_FACTOR'])

	# create node and message router
	router = Router()
	shard = Node(router, ADDRESS, VIEW, REPL_FACTOR)

	app.run(host='0.0.0.0', port=13800, debug=False)


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

	# update my vector clock
	data = request.get_json()
	causal_obj = data.get('causal-context')
	#shard.VC.merge(causal_obj, shard.ADDRESS)

	path = '/kv-store/internal/key-count'
	all_replicas = shard.shard_replicas(shard.shard_ID) 
	KEY_COUNT = my_key_count = shard.numberOfKeys()

	for node in all_replicas:
		if node == shard.ADDRESS:
			continue

		# internal request
		forward = False
		payload = {'causal-context': shard.VC.vectorclock}

		res, status_code = router.PUT(node, path, json.dumps(payload), forward)

		jsonResponse = json.loads(res.decode('utf-8'))
		rep_key_count = (jsonResponse['key_count'])
		rep_vector_clock = shard.VC.json_to_vc(jsonResponse['causal-context'])

		if my_key_count != rep_key_count:
			if shard.VC.after(rep_vector_clock):
				# should use gossip protocol to let reps know latest state
				pass
			else:
				KEY_COUNT = rep_key_count
				# update my local state and use gossip protocol to let reps know latest state'''

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
def get_shard(shard_ID):
	replicas = shard.shard_replicas(shard_ID)

	return jsonify({
		'shard_ID': shard_ID,
		'replicas': replicas
		}), 200

'''
Change our current view and re-shard keys
Before we re-shard, make sure new node is up 
'''
@app.route("/kv-store/view-change", methods=['PUT'])
def new_view():

	path = '/kv-store/internal/view-change'
	method = 'PUT'
	data = request.get_json()

	print('data:', data, file=sys.stderr)

	all_nodes = shard.all_nodes()
	for node in all_nodes:
		if node == shard.ADDRESS:
			continue

		res = router.PUT(node, method, path, data)

		Response = json.loads(res.decode('utf-8'))
		keys = Response['keys']
		keys[node] = keys

	view = data.get('view')
	repl_factor = data.get('repl-factor')
	shard.view_change(view, repl_factor)
	keys[shard.ADDRESS] = shard.KV_Store.numberOfKeys()
	
	response = {}
	response['view-change'] = {"message": "View change successful", "causal-context":data.get('causal-context'), "shards": []}
	response['view-change']['shards'] = {}
	for shard in range(len(shard.P_SHARDS)):
		response['view-change']['shards']['shard-id'] = shard
		response['view-change']['shards']['replicas'] = shard.P_SHARDS[shard]

	json_res = json.dumps(response)
	return json_res, 200


'''
get/put/delete key for shard
'''
@app.route('/kv-store/keys/<keyName>', methods=['GET', 'PUT', 'DELETE'])
def update_keys(keyName):

	# find the shard that is associated with this key
	key_shard = shard.find_match(keyName)
	all_replicas = shard.shard_replicas(key_shard)

	# we have the key locally
	if (key_shard == shard.shard_ID):
		method = request.method

		shard.VC.increment(shard.ADDRESS)
		if method == 'PUT':
			data = request.get_json()
			value = data["value"]
			response, code = shard.insertKey(keyName, value, shard.VC, ADDRESS if (forward is not None) else None)
			if (code == 200):
				share_request("PUT", keyName, data)
			return response, code
		elif method == 'GET':
			return shard.readKey(keyName, shard.VC, ADDRESS if (forward is not None) else None)
		elif method == 'DELETE':
			response, code = shard.removeKey(keyName, shard.VC, ADDRESS if (forward is not None) else None)
			if (code == 200):
				share_request("DELETE", keyName, data)
			return response, code
		else:
			'error occured'

	# forward request to another shard
	else:
		path = '/kv-store/keys/'+keyName
		method = request.method
		data = None

		# forward request to replicas in key_shard shard
		for replica in all_replicas:
			print('sending request to replica', replica, file=sys.stderr)
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


def share_request(method ,key, data=None):
	headers = {'content-type': 'application/json'}
	replica_ip_addresses = shard.shard_replicas(shard.shard_ID)
	if method == "DELETE":
		for replica in replica_ip_addresses:
			if (replica != ADDRESS):
				requests.delete(replica + '/kv-store/keys/' + key, headers=headers, timeout=0.00001)
	elif method == "PUT":
		for replica in replica_ip_addresses:
			if (replica != ADDRESS):
				requests.put(replica + '/kv-store/keys/' + key, json=data, headers=headers, timeout=0.00001)
'''
all internal endpoints
---------------------------------------------------------------------------------
'''

'''
internal shard key count
'''
@app.route('/kv-store/internal/key-count', methods=['PUT'])
def internal_key_count():

	causal_obj = json.dumps(shard.VC.vectorclock)
	key_count = shard.numberOfKeys()

	return jsonify({
				'key_count'     : key_count,
				'causal-context': causal_obj
	}), 200

'''
internal key transfer
'''
@app.route('/kv-store/internal/keys/<keyName>', methods=['GET', 'PUT', 'DELETE'])
def internal_keys(keyName):
	#shard.VC.merge(causal_obj, shard.ADDRESS)

	# update shard's vector clock, one for recieving and one for sending
	shard.VC.increment(shard.ADDRESS)

	method = request.method
	data = request.get_json()
	data['causal-context'] = json.dumps(shard.VC.vectorclock)
	commit = data.get('commit')
	return local_operation(method, keyName, data, commit)

'''
get the entire KV store for a given node
'''
@app.route('/kv-store/internal/KV', methods=['GET'])
def get_kv():
	kv_res = json.dumps(self.KV_Store.keystore)
	return jsonify({
		'KV_Store' : kv_res
		}), 201

'''
internal endpoint for viewchange
'''
@app.route('/kv-store/internal/view-change', methods=['PUT'])
def spread_view():

	data = request.get_json()
	#view = (request.get_data().decode('utf8')).split(',')
	view = data.get('view')
	repl_factor = data.get('repl_factor')
	shard.view_change(view)

	return jsonify({
			'new_view'     : view,
			'ADDRESS'	   : address,
			'keys' 		   : shard.numberOfKeys
	}), 200

@app.route('/kv-store/internal/state-transfer', methods=['PUT'])
def state_transfer():
	data = request.get_json()
	other_vector_clock = data["context"]
	if shard.VC.selfHappensBefore(other_vector_clock):
		shard.keystore = data["kv-store"]
		shard.vc = VectorClock(view=None, clock=other_vector_clock)
	return {
			"message"	: "Acknowledged"
	}, 201

'''
internal endpoint to gossip/send state to all other replicas
'''
'''@app.route('/kv-store/internal/gossisp/', methods=["PUT"])
def gossip():
	data = request.get_json()
	# checks if I am currently gossiping with someone else
	if not shard.gossiping:
		# causal context of the incoming node trying to gossip
		other_context = data["context"]
		# key store of incoming node trying to gossip
		other_kvstore = data["kv-store"]
		# this is true if the other node determined i will be the tiebreaker
		tiebreaker = True if data["tiebreaker"] == ADDRESS else False
		if shard.VC.selfHappensBefore(other_context):
			# I am before
			# i accept your data
			shard.keystore = other_kvstore
			shard.vc = VectorClock(None, other_context)
			return {
				"message"	: "I took your data"
			}, 200
		elif tiebreaker:
			return {
					"message" : "I am the tiebreaker, take my data",
					"context" : shard.VC.__repr__,
					"kv-store": shard.keystore,
				}, 501
		elif not tiebreaker:
			shard.keystore = other_kvstore
			shard.vc = VectorClock(None, other_context)
			return {
				"message"	: "I took your data"
			}, 200
		return {
			"message"	: "I am gossiping with someone else",
		}, 400'''

'''
perfrom operation on node's local key-store
'''
def local_operation(method, keyName, data, commit):
	if commit:
		shard.VC.increment(shard.ADDRESS)
	
	if method == 'PUT':
		value = data.get('value')
		return shard.insertKey(keyName, value, False, shard.VC.vectorclock, commit)

	elif method == 'GET':
		return shard.readKey(keyName, False, shard.VC.vectorclock, commit)

	elif method == 'DELETE':
		return shard.removeKey(keyName, False, shard.VC.vectorclock, commit)

	else:
		'error occured'

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

	app.run(host='0.0.0.0', port=13800, debug=False)








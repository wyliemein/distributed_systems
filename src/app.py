'''
app.py defines the network nodes that listen to requests from
clients and other nodes int the system.
'''

from flask import Flask, request, jsonify, make_response, g
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

		#try:
		res, status_code = router.PUT(node, path, json.dumps(payload), forward)
		#except Exception as e:
			# node may be down, handle it in node.py
		#	print('<warning>', node, 'is unresponsive:', e, file=sys.stderr)
		#	shard.handle_unresponsive_node(node)
		#	continue

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
def get_shard():
	pass

'''
Change our current view and re-shard keys
Before we re-shard, make sure new node is up 
'''
@app.route('/kv-store/view-change', methods=['PUT'])
def new_view():

	pass

'''
get/put/delete key for shard
'''
@app.route('/kv-store/keys/<keyName>', defaults={'forward': None}, methods=['GET', 'PUT', 'DELETE'])
@app.route('/kv-store/keys/<keyName>/<forward>', methods=['GET', 'PUT', 'DELETE'])
def update_keys(keyName, forward):
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
		path = '/kv-store/keys/' +keyName + '/forward'
		data = request.get_json()
		# forward request to replicas in key_shard shard
		for replica in all_replicas:
			try:
				return router.FORWARD(replica, request.method, path, data)
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

	key_count = shard.numberOfKeys()

	return jsonify({
				'key_count'     : key_count,
				'causal-context': shard.VC.__repr__()
	}), 200

'''
internal key transfer
'''
# @app.route('/kv-store/internal/keys/<keyName>', methods=['GET', 'PUT', 'DELETE'])
# def key_transfer(keyName):
# 	#shard.VC.merge(causal_obj, shard.ADDRESS)

# 	# update shard's vector clock, one for recieving and one for sending
# 	shard.VC.increment(shard.ADDRESS)

# 	method = request.method
# 	data = request.get_json()
# 	data['causal-context'] = json.dumps(shard.VC.vectorclock)
# 	commit = data.get('commit')
# 	return local_operation(method, keyName, data, commit)

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

@app.route('/kv-store/internal/gossisp/', methods=["PUT"])
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
		}, 400


'''
# def two_phase_causal_consistency(all_replicas, keyName, method):
# 	print('in two_phase_causal_consistency', file=sys.stderr)

# 	path = '/kv-store/internal/keys/'+keyName
# 	commit = False
# 	data = request.get_json() 

# 	RES = local_operation(method, keyName, data, commit)
# 	causal_king = shard.ADDRESS
# 	max_VC = shard.VC
	
# 	# phase 1, see what a response would be
# 	for replica in all_replicas:
# 		if replica == shard.ADDRESS:
# 			continue
# 		#try:

# 		print('sending request to', replica, file=sys.stderr)

# 		data['commit'] = commit
# 		payload = json.dumps(data)
# 		shard.VC.increment(shard.ADDRESS)
		
# 		forward = False
# 		res, status_code = router.FORWARD(replica, method, path, keyName, payload, forward)

# 		Jres = json.loads(RES)

# 		# if respondes from replicates are not identical, check vector clocks
# 		if res['message'] != Jres['message']:

# 			vc = VectorClock(res['causal-context'])

# 			print('comparing vector clocks', file=sys.stderr)
# 			print('VC1', max_VC, file=sys.stderr)
# 			print('VC2', vc, file=sys.stderr)

# 			if max_VC.after(vc):
# 				RES = res
# 				max_VC = vc
# 				causal_king = replica

# 		#except:
# 		#	print('<Warning:', replica, 'is unresponsive', file=sys.stderr)
# 		#	continue

# 	# phase 2, commit request
# 	commit = True
# 	forward = True
# 	if causal_king == shard.ADDRESS:
# 		return local_operation(method, keyName, data, commit)
# 	else:
# 		payload['commit'] = commit
# 		return router.FORWARD(replica, method, path, keyName, payload, forward)

'''









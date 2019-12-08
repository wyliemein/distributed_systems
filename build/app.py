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
@app.route('/kv-store/keys/<keyName>', methods=['GET', 'PUT', 'DELETE'])
def update_keys(keyName):

	# find the shard that is associated with this key
	key_shard = shard.find_match(keyName)
	all_replicas = shard.shard_replicas(key_shard)

	# we have the key locally
	if (key_shard == shard.shard_ID):
		method = request.method
		print('return local key op', file=sys.stderr)
		return causal_consistency(all_replicas, keyName, method)

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
def key_transfer(keyName):
	#shard.VC.merge(causal_obj, shard.ADDRESS)

	# update shard's vector clock, one for recieving and one for sending
	shard.VC.increment(shard.ADDRESS)

	method = request.method
	data = request.get_json()
	data['causal-context'] = json.dumps(shard.VC.vectorclock)
	commit = data.get('commit')
	return local_operation(method, keyName, data, commit)

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
def causal_consistency(all_replicas, keyName, method):
	print('in two_phase_causal_consistency', file=sys.stderr)


	data = request.get_json() 
	request_vc = VectorClock(data['causal-context'])

	print('clients causal-context:', request_vc, file=sys.stderr)

	RES = local_operation(method, keyName, data, commit)
	causal_king = shard.ADDRESS
	max_VC = shard.VC
	
	# choose the causally greatest response
	for replica in all_replicas:
		if replica == shard.ADDRESS:
			continue
		try:
			print('sending request to', replica, file=sys.stderr)

			data['commit'] = commit
			payload = json.dumps(data)
			shard.VC.increment(shard.ADDRESS)
			
			forward = False
			res, status_code = router.FORWARD(replica, method, path, keyName, payload, forward)

			Jres = json.loads(RES)

			# if respondes from replicates are not identical, check vector clocks
			if res['message'] != Jres['message']:

				vc = VectorClock(res['causal-context'])

				print('comparing vector clocks', file=sys.stderr)
				print('VC1', max_VC, file=sys.stderr)
				print('VC2', vc, file=sys.stderr)

				if max_VC.after(vc):
					RES = res
					max_VC = vc
					causal_king = replica

		except:
			print('<Warning:', replica, 'is unresponsive', file=sys.stderr)
			continue

	# phase 2, commit request
	commit = True
	forward = True
	if causal_king == shard.ADDRESS:
		return local_operation(method, keyName, data, commit)
	else:
		payload['commit'] = commit
		return router.FORWARD(replica, method, path, keyName, payload, forward)

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









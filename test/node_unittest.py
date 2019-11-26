import unittest
import collections
from node import Node
from app import Message

class Test_Shard_Methods(unittest.TestCase):

	# example node addresses
	addr1="10.10.0.2:13800"
	addr2="10.10.0.3:13800"
	addr3="10.10.0.4:13800"
	addr4="10.10.0.5:13800"

	# instanciate node class
	view = [addr1, addr2, addr3, addr4]
	router = Message()
	repl_factor = 2
	shard = Node(router, addr1, view, repl_factor)

	def test_state_report(self):
		report = self.shard.state_report()
		self.assertTrue(report != None)

	def test_initial_sharding(self):
		print(self.shard.V_SHARDS)
		print(self.shard.P_SHARDS)
		self.assertTrue(len(self.shard.V_SHARDS)>0)

	def test_add_node(self):
		pass

	def test_find_match(self):
		pass

	def test_find_shard(self):
		pass

	def test_view_change(self):
		pass

if __name__ == '__main__':
	unittest.main()
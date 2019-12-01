import unittest
import collections
from node import Node
from Message import Router

# example node addresses
addr1="10.10.0.2:13800"
addr2="10.10.0.3:13800"
addr3="10.10.0.4:13800"
addr4="10.10.0.5:13800"
addr5="10.10.0.6:13800"
addr6="10.10.0.7:13800"

# instanciate node class
router = Router()
repl_factor = 2

class Test_Shard_Methods(unittest.TestCase):

	"""def test_state_report(self):
		print("test0\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		report = shard.state_report()
		self.assertTrue(report != None)

	def test_initial_sharding(self):
		print("test1\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		self.assertTrue(len(shard.V_SHARDS)>0)

	def test_view_change_add_one(self):
		print("test2\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		old_card = len(shard.nodes)
		new_view = view.copy()
		new_view.append(addr5)

		shard.view_change(new_view)
		new_card = len(shard.nodes)
		self.assertTrue(new_card>old_card)

	def test_view_change_remove_one(self):
		print("test3\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		old_card = len(shard.nodes)
	
		new_view = [addr1, addr2, addr3]

		shard.view_change(new_view)
		new_card = len(shard.nodes)
		self.assertTrue(new_card<old_card)"""

	def test_view_change_key_tansfer(self):
		print("test4\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		old_card = len(shard.nodes)
		new_view = view.copy()
		new_view.append(addr5)
		new_view.append(addr6)

		shard.view_change(new_view)

		new_card = len(shard.nodes)
		self.assertTrue(new_card>old_card)

	def test_view_change_reshard(self):
		pass

if __name__ == '__main__':
	unittest.main()









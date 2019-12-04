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

	def test_initial_state(self):
		print("test0\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		print(shard)
		self.assertTrue(shard != None)

	"""def test_view_change_add_node(self):
		print("test2\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		old_card = len(shard.nodes)
		new_view = view.copy()
		new_view.append(addr5)

		shard.view_change(new_view, 0, repl_factor)
		new_card = len(shard.nodes)
		self.assertTrue(new_card>old_card)

	def test_view_change_remove_node(self):
		print("test3\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		old_card = len(shard.nodes)
	
		new_view = [addr1, addr2, addr3]

		shard.view_change(new_view, 0)
		new_card = len(shard.nodes)
		self.assertTrue(new_card<old_card)

	def test_view_change_add_shard(self):
		print("test4\n")
		view = [addr1, addr2, addr3, addr4]
		shard = Node(router, addr1, view, repl_factor)
		old_card = len(shard.nodes)
		new_view = view.copy()
		new_view.append(addr5)
		new_view.append(addr6)

		shard.view_change(new_view, 0)

		new_card = len(shard.nodes)
		self.assertTrue(new_card>old_card)

	def test_view_change_remove_shard(self):
		pass"""

if __name__ == '__main__':
	unittest.main()









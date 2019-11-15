import unittest
import shard
import collections

class Test_Shard_Methods(unittest.TestCase):

	# example node addresses
	addr1="10.10.0.2:13800"
	addr2="10.10.0.3:13800"
	addr3="10.10.0.4:13800"

	# convenience variables
	full_view = addr1 + "," + addr2 + "," + addr3
	view = full_view.split(",")
	shard = shard.Partition(addr1, view)

	def test_find_node(self):
		node = self.shard.find_node(500)
		print(node)
		self.assertTrue(node != None)

	def test_isupper(self):

		self.assertTrue('FOO'.isupper())
		self.assertFalse('Foo'.isupper())

	def test_split(self):
		s = 'hello world'
		self.assertEqual(s.split(), ['hello', 'world'])
		# check that s.split fails when the separator is not a string
		with self.assertRaises(TypeError):
			s.split(2)

if __name__ == '__main__':
	unittest.main()
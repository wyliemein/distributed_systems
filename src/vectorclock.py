from flask import jsonify

import sys

class Node():
    
    def __init__(self):
        self.vectorclock = {}
    
    def increment(self, index):
        self.vectorclock[index] += 1
    
    def returnClock:
        return jsonisfy({
            self.vectorclock
        })
    
    def appendShard(self, node):
        self.vectorclock(node, 0)
    




    
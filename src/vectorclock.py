from flask import jsonify

import sys

class VectorClock():
    
    vectorclock = {}

    def __init__(self, vector=None):
        if vector is None:
            self.vector = {}
        elif isinstance(vector, list):
            self.vector = { key: val for key, val in enumerate(vector) }
        else:
            self.vector = dict(vector)

    def increment(self, index):
        self.vectorclock[index] = self.vectorclock[index] + 1
        return self
    
    def returnClock(self):
        // NEED TO RETURN JSON OBECT
        return jsonify(self.vectorclock)
    
    def appendShard(self, index):
        self.vectorclock[index] = 0

    def printclock(self):
        for x, y in self.vectorclock.items():
            print(x, y)
    





    
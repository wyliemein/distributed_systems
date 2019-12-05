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

    vectorclock = {}

    def increment(self, index):
        self.vectorclock[index] = self.vectorclock[index] + 1
        return self

    def merge(self, other, index):
        if (len(self.vectorclock.items()) >= len(other.items())):
            keys = self.vectorclock.keys()
        else:
            keys = other.keys()
        t_vectorclock = {}
        for k in keys:
            if k not in self.vectorclock.keys():
                self.vectorclock[k] = 0
            if k not in other.keys():
                other[k] = 0
            if (self.vectorclock[k] >= other[k]):
                t_vectorclock[k] = self.vectorclock[k]
            else:
                t_vectorclock[k] = other[k]
        t_vectorclock[index] = t_vectorclock[index] + 1
        self.vectorclock = t_vectorclock
    
    def returnClock(self):
        // NEED TO RETURN JSON OBECT
        return jsonify(self.vectorclock)
    
    def appendShard(self, index):
        self.vectorclock[index] = 0

    def printclock(self):
        for x, y in self.vectorclock.items():
            print(x, y)
    





    
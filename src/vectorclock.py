from flask import jsonify

import sys

class VectorClock():
    
    def __init__(self, vectorclock=None):
        if vectorclock:
            for x in vectorclock.keys():
                 self.vectorclock[x] = 0
        else:
            self.vectorclock = {}

    def __repr__(self):
        return jsonify(self.vectorclock)

    def increment(self, index):
        if index not in self.vectorclock:
            self.vectorclock[index] = 0
        self.vectorclock[index] = self.vectorclock[index] + 1

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

    def after(self, clock):
        # True if clock -> self
        if (len(self.vectorclock.items()) > len(clock.items())):
            return False
        if self == clock:
            return False
        for key, value in self.vectorclock.items():
            if clock.vectorclock.get(key, 0) > value:
                return False
        return True
    
    def returnClock(self):
        return jsonify(self.vectorclock)
    
    def appendShard(self, index):
        self.vectorclock[index] = 0

    def comparePosition(self, other, index):
        if (other[index] >= self.vectorclock[index]):
            return True
        return False

    def printclock(self):
        for x, y in self.vectorclock.items():
            print(x, y)    





    
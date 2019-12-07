from flask import jsonify
import json
import sys

class VectorClock():
    
    def __init__(self, VC=None):
        self.vectorclock = {}
        if VC:
            self.vectorclock = VC

    def __repr__(self):
        return jsonify(self.vectorclock)

    def __str__(self):
        return str(self.vectorclock)

    def initialize(self, nodes):
        for node in nodes:
            self.vectorclock[node] = 0

        return self

    def increment(self, index):
        if index not in self.vectorclock:
            self.vectorclock[index] = 0
        self.vectorclock[index] = self.vectorclock[index] + 1

    def merge(self, other, index):
        if (len(self.vectorclock.items()) >= len(other.vectorclock.items())):
            keys = self.vectorclock.keys()
        else:
            keys = other.vectorclock.keys()
        t_vectorclock = {} 
        for k in keys:
            if k not in self.vectorclock.keys():
                self.vectorclock[k] = 0
            if k not in other.vectorclock.keys():
                other.vectorclock[k] = 0
            if (self.vectorclock[k] >= other.vectorclock[k]):
                t_vectorclock[k] = self.vectorclock[k]
            else:
                t_vectorclock[k] = other.vectorclock[k]
        t_vectorclock[index] = t_vectorclock[index] + 1
        self.vectorclock = t_vectorclock

    def after(self, clock):
        # True if clock -> self
        if (len(self.vectorclock.items()) > len(clock.vectorclock.items())):
            return False
        if self.vectorclock == clock.vectorclock:
            return False
        for key, value in self.vectorclock.items():
            if clock.vectorclock.get(key, 0) > value:
                return False
        return True
    
    def returnClock(self):
        return jsonify(self.vectorclock)
    
    def deleteShard(self, index):
        del self.vectorclock[index]

    def comparePosition(self, other, index):
        if (other.vectorclock[index] >= self.vectorclock[index]):
            return True
        return False

    def printclock(self):
        for x, y in self.vectorclock.items():
            print(x, y)    





    
from flask import jsonify

import sys

class VectorClock():
    
    def __init__(self, view=None, clock=None):
        if view is not None:
            self.vectorclock = {}
            for node in view:
                self.vectorclock[node] = 0
        elif clock is not None:
            self.vectorclock = clock

    def __repr__(self):
        return self.vectorclock

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

    def selfHappensBefore(self, other):
        # IF SELF HAPPENS BEFORE OTHER -> TRUE
        if (len(other.keys()) < len(self.vectorclock.keys())):
            return False
        if other == self.vectorclock:
            return False
        for key, value in self.vectorclock.items():
            if other[key] < value:
                return False
        return True

    def allFieldsZero(self):
        for key,value in self.vectorclock.items():
            if value != 0:
                return False
        return True
    
    def returnClock(self):
        return self.vectorclock
    
    def appendShard(self, index):
        if index not in self.vectorclock:
            self.vectorclock[index] = 0

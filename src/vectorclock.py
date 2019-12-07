from flask import jsonify

import sys

class VectorClock():
    
<<<<<<< HEAD
    def __init__(self, view=None, clock=None):
        if view is not None:
            self.vectorclock = {}
            for key in view.items():
                self.vectorclock[key] = 0
        elif clock is not None:
            self.vectorclock = clock
=======
    def __init__(self, vectorclock=None):
        if vectorclock:
            for x in vectorclock.keys():
                 self.vectorclock[x] = 0
        else:
            self.vectorclock = {}
>>>>>>> cd2ee208ce4632f7a5546f94f01ede1807c20905

    def __repr__(self):
        return self.vectorclock

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

<<<<<<< HEAD
    def selfHappensBefore(self, other):
        # IF SELF HAPPENS BEFORE OTHER -> TRUE
        if (len(other.keys()) < len(self.vectorclock.keys())):
=======
    def after(self, clock):
        # True if clock -> self
        if (len(self.vectorclock.items()) > len(clock.items())):
>>>>>>> cd2ee208ce4632f7a5546f94f01ede1807c20905
            return False
        if other == self.vectorclock:
            return False
        for key, value in self.vectorclock.keys():
            if other[key] < value:
                return False
        return True
    
    def returnClock(self):
        return self.vectorclock
    
    def appendShard(self, index):
<<<<<<< HEAD
        if index not in self.vectorclock:
            self.vectorclock[index] = 0
=======
        self.vectorclock[index] = 0

    def comparePosition(self, other, index):
        if (other[index] >= self.vectorclock[index]):
            return True
        return False

    def printclock(self):
        for x, y in self.vectorclock.items():
            print(x, y)    





    
>>>>>>> cd2ee208ce4632f7a5546f94f01ede1807c20905

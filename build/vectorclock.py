from flask import jsonify

import sys

class VectorClock():
    
    vectorclock = {}

    def increment(self, index):
        self.vectorclock[index] = self.vectorclock[index] + 1
        return self
    
    def returnClock(self):
        # NEED TO RETURN JSON OBECT
        return jsonify(self.vectorclock)
    
    def appendShard(self, index):
        self.vectorclock[index] = 0

    def printclock(self):
        for x, y in self.vectorclock.items():
            print(x, y)

    # compare vc to self.vectorclock and return true if self.vc is > vc
    def local_is_greater(vc):
        pass

    def json_to_vc(self, json_vc):
        pass
    




    
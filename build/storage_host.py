from datetime import datetime
from flask import jsonify
import sys
import json

class KV_store():

    def __init__(self, IPAddress):
        self.history = [("Initialized", datetime.now())]
        self.keystore = {}
        self.IPAddress = IPAddress

    """
        Functions below only read from the database
    """

    def containsKey(self, key):
        '''
        Returns the True if the key is contained in the local database
        '''
        return (key in self.keystore)

    def readKey(self, key, forward, vc, commit):
        if (self.containsKey):
            if (forward == True):
                res = {
                    "doesExist"     : True,
                    "message"       : "Retrieved successfully",
                    "value"         : self.keystore[key],
                    "address"       : self.IPAddress,
                    "causal-context": vc
                }
                if commit:
                    return jsonify(res), 200
                else:
                    return res
            else:
                res = {
                    "doesExist"     : True,
                    "message"       : "Retrieved successfully",
                    "value"         : self.keystore[key],
                    "causal-context": vc
                }
                if commit:
                    return jsonify(res), 200
        else:
            res = {
                "doesExist"     : False,
                "error"         : "Key does not exist",
                "message"       : "Error in GET",
                "causal-context": vc
            }
            if commit:
                return jsonify(res), 404
            else:
                return res

    def numberOfKeys(self):
        '''
        Returns the number of keys in the local database
        '''
        return len(self.keystore)

    def printHistory(self):
        '''
        Prints all events which have occured on this database
        '''
        for event in self.history:
            print(event[0] + ": " + event[1].strftime("%m/%d/%Y, %H:%M:%S"))

    def returnHistory(self):
        '''
        Prints all events which have occured on this database
        '''
        output = "<center>"
        for event in self.history:
            output = output + event[0] + ": " + event[1].strftime("%m/%d/%Y, %H:%M:%S") + "<br>"
        return output + "</center>"

    """
        Functions below perform writes to the database
        
    """

    '''
    Inserts a key into the keystore
        : if the vlaue is None, return the json error
        : if the value is length > 50, return the json error
        : if the key exists it updates the key with the new value
        : logs the event to the history with the time
    '''
    def insertKey(self, key, value, forward, vc, commit):
        if (value is None):
            res = {
                "error"         : "Value is missing",
                "message"       : "Error in PUT",
                "causal-context": vc
            }
            if commit:
                return jsonify(res), 400
            else:
                return json.dumps(res)

        elif (len(value) > 50):
            res = {
                "error"         : "Key is too long",
                "message"       : "Error in PUT",
                "causal-context": vc
            }
            if commit:
                return jsonify(res), 400
            else:
                return json.dumps(res)
        elif (self.containsKey(key)):
            if commit:
                self.keystore[key] = value
                self.history.append(("Updated " + key + " to value " + value, datetime.now()))
                if (forward == True):
                    return jsonify({
                        "message"       : "Updated successfully",
                        "replaced"      : True,
                        "address"       : self.IPAddress,
                        "causal-context": vc
                    }), 201
                else:
                    return jsonify({
                    "message"       : "Updated successfully",
                    "replaced"      : True,
                    "causal-context": vc
                }), 201
            else:
                res = {
                    "message"       : "Updated successfully",
                    "replaced"      : True,
                    "causal-context": vc
                }
                return json.dumps(res)
            
        else:
            replaced = True if self.containsKey(key) else False
            message = "Updated Successfully" if replaced else "Added successfully"
            
            if commit:
                if (forward == True):
                    self.keystore[key] = value
                    self.history.append(("Added " + key + " with value " + value, datetime.now()))
                    return jsonify({
                        "message"       : message,
                        "replaced"      : replaced,
                        "address"       : self.IPAddress,
                        "causal-context": vc
                    }), 201
                else:
                    self.keystore[key] = value
                    self.history.append(("Added " + key + " with value " + value, datetime.now()))
                    return jsonify({
                        "message"       : message,
                        "replaced"      : replaced,
                        "causal-context": vc
                    }), 201
            else:
                res = {
                    "message"       : message,
                    "replaced"      : replaced,
                    "causal-context": vc
                }
                return json.dumps(res)

    def removeKey(self, key, forward, vc, commit):
        '''
        Checks whether the key is found and removes it
            : returns true if it is found and removed
            : returns false if the key is not found
        '''
        if (self.containsKey):
            if commit == False:
                res = {
                    "doesExist"     : True,
                    "message"       : "Deleted successfully",
                    "causal-context": vc
                }
                return json.dumps(res)
            if (forward == True):
                del self.keystore[key]
                self.history.append(("Removed " + key, datetime.now()))
                return jsonify({
                "doesExist"     : True,
                "message"       : "Deleted successfully",
                "address"       : self.IPAddress,
                "causal-context": vc
            }), 200
            else:
                del self.keystore[key]
                self.history.append(("Removed " + key, datetime.now()))
                return jsonify({
                "doesExist"     : True,
                "message"       : "Deleted successfully",
                "causal-context": vc
            }), 200
        else:
            if commit == False:
                res = {
                    "doesExist"     : False,
                    "error"         : "Key does not exist",
                    "message"       : "Error in DELETE",
                    "causal-context": vc
                }
                return json.dumps(res)
            return jsonify({
                "doesExist"     : False,
                "error"         : "Key does not exist",
                "message"       : "Error in DELETE",
                "causal-context": vc
            }), 404



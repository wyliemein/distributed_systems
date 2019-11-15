from datetime import datetime
from flask import jsonify

import sys

class Node():

    def __init__(self):
        self.history = [("Initialized", datetime.now())]
        self.keystore = {}
        self.number = 0
        self.IPAddress = ""
        self.port = 0
        self.others = []

    def setInfo(self,_IPAddress, _others):
        splitIP = _IPAddress.split(":")[0]
        self.port = 13800 + int((splitIP.split(".")[3]))
        self.number = int((splitIP.split(".")[3])) - 2
        self.IPAddress = _IPAddress
        self.others = _others

    def returnInfo(self):
        return "<center><b>Node {}</b><br>SUBNET IP: {}<br>ALL NODES({}): {}<br>".format(str(self.number), self.IPAddress,str(len(self.others)),str(self.others)) + "<br><b>KEYS   VALUES   NODE<b><br>" + self.returnAllKeysWithHash() + "<br><b>EVENTS:<b><br>" + self.returnHistory() + "</center>"

    """
        Functions below only read from the database
    """

    def hashKey(self,key):
        '''
        Returns the hash value modded by the number of others
        '''
        value = hash(key)
        return value % len(self.others)

    def keyBelongsHere(self,key):
        '''
        Returns the True if the key hash belongs on this node
        '''
        value = hash(key) % len(self.others)
        if (value == self.number):
            return True
        return False

    def all_nodes(self):
        # return all nodes in our current view
        return self.others

    def reshard(self,_othersString):
        self.others = _othersString.split(",")
        # create a temporary list of dictionaries of payloads to send k/v's to proper nodes
        forward_data = []
        for ip in self.others:
            forward_data.append((ip,{}))
        temporary_keystore = {}
        for key in self.keystore:
            if (self.keyBelongsHere(key) != True):
                newHashValue = self.hashKey(key)
                forward_data[newHashValue][1][key] = self.keystore[key]
            else:
                temporary_keystore[key] = self.keystore[key]
        self.keystore = temporary_keystore
        # now iterate through forward_data
        # print(forward_data, file=sys.stderr)
        return forward_data
        
    def containsKey(self, key):
        '''
        Returns the True if the key is contained in the local database
        '''
        return (key in self.keystore)

    def readKey(self, key, forward):
        if (self.containsKey):
            if (forward == True):
                return jsonify({
                "doesExist"     : True,
                "message"       : "Retrieved successfully",
                "value"         : self.keystore[key],
                "address"       : self.IPAddress
            }), 200
            else:
                return jsonify({
                "doesExist"     : True,
                "message"       : "Retrieved successfully",
                "value"         : self.keystore[key]
            }), 200
        else:
             return jsonify({
                "doesExist"     : False,
                "error"         : "Key does not exist",
                "message"       : "Error in GET"
            }), 404

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

    def returnAllKeysWithHash(self):
        output = "<br><center>"
        for key in self.keystore:
            output = output + "<b>" + key + ": " + self.keystore[key] + "<br>"
        return output + "</center>"

    """
        Functions below perform writes to the database
        
    """

    def insertKey(self, key, value, forward):
        '''
        Inserts a key into the keystore
            : if the vlaue is None, return the json error
            : if the value is length > 50, return the json error
            : if the key exists it updates the key with the new value
            : logs the event to the history with the time
        '''
        if (value is None):
            return jsonify({
                "error"     : "Value is missing",
                "message"   : "Error in PUT"
            }), 400
        elif (len(value) > 50):
            return jsonify({
                "error"     : "Key is too long",
                "message"   : "Error in PUT"
            }), 400
        elif (self.containsKey(key)):
            self.keystore[key] = value
            self.history.append(("Updated " + key + " to value " + value, datetime.now()))
            return jsonify({
                "message"       : "Updated successfully",
                "replaced"      : True,
                "address"       : "TO DO",
            }), 201
        else:
            replaced = True if self.containsKey(key) else False
            message = "Updated Successfully" if replaced else "Added successfully"
            if (forward == True):
                self.keystore[key] = value
                self.history.append(("Added " + key + " with value " + value, datetime.now()))
                return jsonify({
                    "message"       : message,
                    "replaced"      : replaced,
                    "address"       : self.IPAddress,
                }), 201
            else:
                self.keystore[key] = value
                self.history.append(("Added " + key + " with value " + value, datetime.now()))
                return jsonify({
                    "message"       : message,
                    "replaced"      : replaced,
                }), 201

    def removeKey(self, key, forward):
        '''
        Checks whether the key is found and removes it
            : returns true if it is found and removed
            : returns false if the key is not found
        '''
        if (self.containsKey):
            if (forward == True):
                del self.keystore[key]
                self.history.append(("Removed " + key, datetime.now()))
                return jsonify({
                "doesExist"     : True,
                "message"       : "Deleted successfully",
                "address"       : self.IPAddress
            }), 200
            else:
                del self.keystore[key]
                self.history.append(("Removed " + key, datetime.now()))
                return jsonify({
                "doesExist"     : True,
                "message"       : "Deleted successfully"
            }), 200
        else:
             return jsonify({
                "doesExist"     : False,
                "error"         : "Key does not exist",
                "message"       : "Error in DELETE"
            }), 404
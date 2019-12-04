import json
import os
import requests

from flask import jsonify
from vectorclock import VectorClock


vc = VectorClock()
vc.appendShard("node1")
vc.increment("node1")
vc.appendShard("node2")
vc.appendShard("node3")
vc.increment("node3")
vc.increment("node3")
vc.printclock()
print("done")
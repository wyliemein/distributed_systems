# Overview

Kv_store is a distributed key-value store that is partition-tolerant, available,  
and causally consistent. 

# Endpoints
IP address: 10.10.0.0/16  
Port numbers: 13800/16

### Key operations:
http path: http://IP:Port/kv-store/keys/<key>  
supported methods: GET, PUT, DELETE  
data: keyName, value (if PUT)  

### Total key count:
http path: http://IP:Port/kv-store/key-count  
supported methods: GET  

### View change:
A view change adds or removes storage nodes from the system.  This is   
done by providing a view or list of ip:port pairs that represent nodes.  

http path: http://IP:Port/kv-store/view-change  
supported methods: PUT  
data: new view  

# Examples

### add a key-value pair
curl --request   PUT \
     --header    "Content-Type: application/json" \
     --write-out "\n%{http_code}\n" \
     --data      '{"value": "sampleValue"}' \
     http://10.10.0.0:13800/kv-store/keys/sampleKey

### get a key-value pair
curl --request GET \
	 --header 'Content-Type: application/json' \
	 --write-out '%{http_code}\n' \
	 http://10.10.0.0:13800/kv-store/keys/<keyName>
     
### delete a key-value pair
curl --request DELETE \
	 --header 'Content-Type: application/json' \
	 --write-out '%{http_code}\n' \
	 http://10.10.0.0:13800/kv-store/keys/<keyName>

### get total system key count
curl --request GET \
	 --header 'Content-Type: application/json' \
	 --write-out '%{http_code}\n' \
	 http://10.10.0.0:13800/kv-store/key-count
      
### update the view to add or remove a node
curl --request PUT                                   \
     --header "Content-Type: application/json"       \
     --data      '{"view":"10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800"}' \
     --write-out "%{http_code}\n"                    \
     http://10.10.0.0:13800/kv-store/view-change


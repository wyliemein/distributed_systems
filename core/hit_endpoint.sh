# example node addresses
addr1="10.10.0.2:13800"
addr2="10.10.0.3:13800"
addr3="10.10.0.4:13800"


curl --request   PUT \
     --header    "Content-Type: application/json" \
     --write-out "\n%{http_code}\n" \
     --data      '{"value": "sampleValue"}' \
     http://127.0.0.1:13802/kv-store/keys/sampleKey


#curl --request GET \
#	 --header 'Content-Type: application/json' \
#	 --write-out '%{http_code}\n' \
#	 http://127.0.0.1:13802/kv-store/key-count

#curl --request PUT                                   \
#     --header "Content-Type: application/json"       \
#     --data      '{"view":"10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800"}' \
#     --write-out "%{http_code}\n"                    \
#     http://127.0.0.1:13802/kv-store/view-change
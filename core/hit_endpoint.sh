# example node addresses
addr1="10.10.0.2:13800"
addr2="10.10.0.3:13800"
addr3="10.10.0.4:13800"


curl --request   PUT \
     --header    "Content-Type: application/json" \
     --write-out "\n%{http_code}\n" \
     --data      '{"value": "sampleValue"}' \
     http://127.0.0.1:13802/kv-store/keys/sampleKey

<<<<<<< HEAD

#curl --request GET \
#	 --header 'Content-Type: application/json' \
#	 --write-out '%{http_code}\n' \
#	 http://127.0.0.1:13802/kv-store/key-count

#curl --request PUT                                   \
#     --header "Content-Type: application/json"       \
#     --data      '{"view":"10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800"}' \
#     --write-out "%{http_code}\n"                    \
#     http://127.0.0.1:13802/kv-store/view-change
=======
curl --request GET \
	 --header 'Content-Type: application/json' \
	 --write-out '%{http_code}\n' \
	 http://127.0.0.1:13802/kv-store/keys/sampleKey
<<<<<<< HEAD
s
=======
	 
>>>>>>> 5c91e08c0c335d31617ba324fbc5b8d81a74569d
>>>>>>> 932ffe45ae3311d7bd0d4c9f9edc598bc65813b8

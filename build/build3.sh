docker stop node3

docker rm node3

docker build -t kv-store:3.0 .

# example node addresses
addr1="10.10.0.2:13800"
addr2="10.10.0.3:13800"
addr3="10.10.0.4:13800"

# convenience variables
initial_full_view="${addr1},${addr2},${addr3}"

docker run -p 13804:13800 \
		--net=kv_subnet \
		--ip=10.10.0.4 \
		--name="node3" \
		-e ADDRESS="${addr3}" \
		-e VIEW=${initial_full_view} \
		kv-store:3.0



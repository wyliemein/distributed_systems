docker stop node1

docker rm node1

docker network prune 

docker network create --subnet=10.10.0.0/16 kv_subnet

docker build -t kv-store:3.0 .

addr1="10.10.0.2:13800"
addr2="10.10.0.3:13800"
addr3="10.10.0.4:13800"

# convenience variables
initial_full_view="${addr1}"


docker run --name="node1"        --net=kv_subnet     \
           --ip=10.10.0.2        -p 13802:13800      \
           -e ADDRESS="${addr1}"                     \
           -e VIEW=${initial_full_view}              \
           kv-store:3.0



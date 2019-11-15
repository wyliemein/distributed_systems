# NODENUMBER=2
# NODENAME="node2"
# NODEPORT=13802
# NODEIP=10.10.0.2
# NODEFULLADDRESS="$NODEIP:$NODEPORT"

docker network create --subnet=10.10.0.0/16 kv_subnet

docker build -t kv-store:3.0 .

# docker run --name=node1 --net=kv_subnet --ip=$NODEIP -p $NODEPORT:13800 -e ADDRESS=$NODEIP -e VIEW=$NODEIP kv-store:3.0

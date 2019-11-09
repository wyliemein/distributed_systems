NODENUMBER=$1
NODENAME="node$1"
NODEPORT=1380$1
NODEIP=10.10.0.$1
NODEFULLADDRESS ="$NODEIP:$NODEPORT"


if [ $NODENUMBER -eq 2 ]; then
    echo "First Node Being Created"
    docker network create --subnet=10.10.0.0/16 kv_subnet
    docker build -t kv-store:3.0 .
fi

echo $NODEFULLADDRESS

docker run --name=$NODENAME        --net=kv_subnet     \
        --ip=$NODEIP        -p $NODEPORT:13800       \
        -e ADDRESS=$addr1                            \
        -e VIEW=$addr1                               \
        kv-store:3.0

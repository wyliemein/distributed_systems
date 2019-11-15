# ------------------------------
# heredoc strings of example requests
# for info on heredoc strings:
#   http://www.guguncube.com/2140/unix-set-a-multi-line-text-to-a-string-variable-or-file-in-bash

read -d '' key_count_template <<"KEY_COUNT_STR"
curl -s --request   GET
        --header    "Content-Type: application/json"
        --write-out "\n%{http_code}\n"
        http://localhost:13802/kv-store/key-count
KEY_COUNT_STR

read -d '' view_change_template <<"VIEW_CHANGE_STR"
curl -s --request   PUT
        --header    "Content-Type: application/json"
        --write-out "\n%{http_code}\n"
        --data      '{"view":"10.10.0.2:13800,10.10.0.3:13800,10.10.0.4:13800"}'
        http://localhost:13802/kv-store/view-change
VIEW_CHANGE_STR

read -d '' get_key_template <<"GET_KEY_STR"
curl -s --request   GET
        --header    "Content-Type: application/json"
        --write-out "\n%{http_code}\n"
        http://localhost:13802/kv-store/keys/sampleKey
GET_KEY_STR

read -d '' put_key_template <<"GET_KEY_STR"
curl -s --request   PUT
        --header    "Content-Type: application/json"
        --write-out "\n%{http_code}\n"
        --data      '{"value": "sampleValue"}'
        http://localhost:13802/kv-store/keys/sampleKey
GET_KEY_STR

read -d '' del_key_template <<"GET_KEY_STR"
curl -s --request   DELETE
        --header    "Content-Type: application/json"
        --write-out "\n%{http_code}\n"
        http://localhost:13802/kv-store/keys/sampleKey
GET_KEY_STR


# ------------------------------
# Main

# print out the above strings for convenience
echo "Sample requests:"

echo ${key_count_template}
echo ${view_change_template}
echo ${get_key_template}
echo ${put_key_template}
echo ${del_key_template}

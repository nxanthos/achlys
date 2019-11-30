#!/bin/bash

# Configuration :

COOKIE='MyCookie'
NAME=$(hostname -s)

# Networking :

IP=$(ifconfig | grep 'inet ' | grep -m 1 -Fv 127.0.0.1 | awk '{print $2}')
PEER_IP=$(echo $IP | sed 's/\./,/g')
PEER_PORT=27000

# Board :

function SPAWN_BOARD() {
  NAME=$NAME \
  PEER_IP=$PEER_IP \
  PEER_PORT="$PEER_PORT" \
  IP=$IP \
  BOARDS=$2 \
  xterm -hold -e "rebar3 as test shell --sname ${1} --setcookie $COOKIE --apps achlys" &
}

PATTERN='^\(([^\)]+)\)\[(.+)\]$'

python3 ./utils/graph.py --n=3 --hostname="${NAME}" |
while IFS= read -r LINE
do
  if [[ $LINE =~ $PATTERN ]]; then
    SNAME="${BASH_REMATCH[1]}"
    BOARDS="${BASH_REMATCH[2]}"
    SPAWN_BOARD "${SNAME}" "${BOARDS}" "${PEER_PORT}"
    ((PEER_PORT = PEER_PORT + 1))
    # sleep 3
  fi
done

read -p "Done !"
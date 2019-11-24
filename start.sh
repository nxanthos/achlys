#!/bin/bash

NAME=$(hostname -s)
COOKIE='MyCookie'

IP=$(ifconfig | grep 'inet ' | grep -m 1 -Fv 127.0.0.1 | awk '{print $2}')
PEER_IP=$(ifconfig | grep 'inet ' | grep -m 1 -Fv 127.0.0.1 | awk '{print $2}' | sed 's/\./,/g')
PEER_PORT=27000

SNAMES=()
for k in {1..2}
do
  SNAMES+=("achlys_${k}@${NAME}")
done

BOARDS=$(printf "%s, " "${SNAMES[@]}")
BOARDS=${BOARDS::-2}

for k in "${!SNAMES[@]}"
do
  echo -e "${SNAMES[k]}"
  # NAME=$NAME PEER_IP=$PEER_IP PEER_PORT=$((PEER_PORT + k)) IP=$IP BOARDS=$BOARDS xterm -e "nohup rebar3 as test shell --sname ${SNAMES[k]} --setcookie $COOKIE --apps achlys" &
  NAME=$NAME PEER_IP=$PEER_IP PEER_PORT=$((PEER_PORT + k)) IP=$IP BOARDS=$BOARDS xterm -e "rebar3 as test shell --sname ${SNAMES[k]} --setcookie $COOKIE --apps achlys" &
done

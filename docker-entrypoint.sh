#!/bin/bash
set -eo pipefail
shopt -s nullglob
zeus_name="zeus"
if [ "$ZEUS_NAME" ] ;then
  zeus_name=$ZEUS_NAME
fi

zookeeper_addr="106.15.225.249:3030"
if [ "$ZOOKEEPER_ADDR" ] ;then
  zookeeper_addr=$ZOOKEEPER_ADDR
fi

/root/app -n "$zeus_name" -sp :50001 -zk "$zookeeper_addr"
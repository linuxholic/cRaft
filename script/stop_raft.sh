#!/bin/bash

pids=$(ps aux | grep "bin/raft-server" | grep -v grep | awk '{print $2}')

for pid in $pids
do
    echo "Stopping process with pid $pid"
    kill $pid
done

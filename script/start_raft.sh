#!/bin/bash

make raft-server

for i in {0..2}
do
    log_file="raft-server-$i.log"
    echo "start raft node ${i}"
    bin/raft-server $i 3 > "$log_file" 2>&1 &
done

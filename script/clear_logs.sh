#!/bin/bash

for i in {0..2}
do
    rm -f raft-server-$i.log
    rm -f replicated-$i.log
done

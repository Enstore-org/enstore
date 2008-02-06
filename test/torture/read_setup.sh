#!/bin/bash

nodes=$*

if [ ${#nodes} -le 1 ]; 
then
    nodes="fcdfcaf560 fcdfcaf561 fcdfcaf562 fcdfcaf563 fcdfcaf564 fcdfcaf565 fcdfcaf566 fcdfcaf567 fcdfcaf568 fcdfcaf569"
fi 
    
for node in $nodes
do
  echo $node
  scp ~/.bashrc  cdfcaf@${node}:~/
  scp read.sh cdfcaf@${node}:~/bin
  ssh cdfcaf@${node} "cd /scratch_dcache/cdfcaf; rm -f read_$$.out; read.sh > read_$$.out 2>&1&"
done


#!/bin/bash

nodes=$*

if [ ${#nodes} -le 1 ]; 
then
    nodes="fcdfcaf560 fcdfcaf561 fcdfcaf562 fcdfcaf563 fcdfcaf564 fcdfcaf565 fcdfcaf566 fcdfcaf567 fcdfcaf568 fcdfcaf569"
fi 
    
for node in $nodes
do
  echo $node
  scp torture.sh cdfcaf@${node}:~/bin
  scp ~/.bashrc  cdfcaf@${node}:~/
  ssh cdfcaf@${node} "cd /scratch_dcache/cdfcaf; rm -f torture_$$.out; torture.sh > torture_$$.out 2>&1&"
done


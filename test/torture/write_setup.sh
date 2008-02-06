#!/bin/bash

nodes=$*

if [ ${#nodes} -le 1 ];
then
    nodes="fcdfcaf560 fcdfcaf561 fcdfcaf562 fcdfcaf563 fcdfcaf564 fcdfcaf565 fcdfcaf566 fcdfcaf567 fcdfcaf568 fcdfcaf569"
fi



#nodes="fcdfcaf560 fcdfcaf561 fcdfcaf562 fcdfcaf563 fcdfcaf564 fcdfcaf565 fcdfcaf566 fcdfcaf567 fcdfcaf568 fcdfcaf569"
for node in $nodes
do
  echo $node
  scp ~/.bashrc  cdfcaf@${node}:~/
  scp fill.sh cdfcaf@${node}:~/bin
  ssh cdfcaf@${node} "cd /scratch_dcache/cdfcaf; rm filler.out; fill.sh > filler.out 2>&1&"
done


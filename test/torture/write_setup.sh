#!/bin/bash
#############################################################
#
#  $Id$
#
#############################################################
pnfs_path=""
user=""
data=""
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
if [ "${1:-}" = "-p" ] ; then shift; pnfs_path=$1; shift; fi
if [ "${1:-}" = "-u" ] ; then shift; user=$1; shift; fi
if [ "${1:-}" = "-d" ] ; then shift; data=$1; shift; fi

if [ -z $user ]; then user=cdfcaf;fi
if [ -z $data ]; then data="/scratch_dcache/cdfcaf";fi

nodes=$*

#if [ ${#nodes} -le 1 ];
if [ ${#nodes} -eq 0 ];
then
    nodes="fcdfcaf560 fcdfcaf561 fcdfcaf562 fcdfcaf563 fcdfcaf564 fcdfcaf565 fcdfcaf566 fcdfcaf567 fcdfcaf568 fcdfcaf569"
fi



#nodes="fcdfcaf560 fcdfcaf561 fcdfcaf562 fcdfcaf563 fcdfcaf564 fcdfcaf565 fcdfcaf566 fcdfcaf567 fcdfcaf568 fcdfcaf569"
for node in $nodes
do
  echo $node
  scp ~/.bashrc  ${user}@${node}:~/
  scp fill.sh ${user}@${node}:~/bin
  ssh ${user}@${node} "cd $data; rm filler.out; fill.sh > filler.out $pnfs_path 2>&1&"
done


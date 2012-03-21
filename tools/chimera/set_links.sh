#!/bin/bash

#
# this script needs to be run after pnfs -> chimera migration
#

mount localhost:/ /mnt

cd /mnt/pnfs
ln -s fs fnal.gov

cd /mnt

dirs=`ls pnfs/fs/usr`

for d in $dirs
do
    ln -s pnfs/fs/usr/${d} ${d}
done
ln -s pnfs/fs fs
#ln -s /pnfs/fs fs

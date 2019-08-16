#!/bin/sh

mt -f /dev/rmt/tps10d0n rewind
mt -f mt -f /dev/rmt/tps10d0n compression 1
cnt=0
while : ; do
    dd if=5GB of=/dev/rmt/tps10d0n bs=1MB >> test_with_compression_1 2>&1
    if [ $? -ne 0 ]; then
	break
    fi
    let "cnt = cnt + 1"
    echo "CNT=" $cnt >> test_with_compression_1
done

#!/bin/bash

[ $(date -d +1day +%d) -eq 1 ] && python $ENSTORE_DIR/sbin/historic_bytes_on_tape.py
#!/bin/bash

# Execute Python script only on last day of month.
if [ $(date -d +1day +%d) -eq 1 ]; then
    python $ENSTORE_DIR/sbin/historic_bytes_on_tape.py
fi

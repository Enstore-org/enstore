

PATH="$PATH:${ENSTORE_DIR}/bin"; export PATH

if [ "${PYTHONPATH:-1}" = "1" ]; then
   PYTHONPATH=$ENSTORE_DIR/src
else
   PYTHONPATH=${PYTHONPATH}:$ENSTORE_DIR/src
fi
export PYTHONPATH

encp() { $PYTHON_DIR/bin/python encp.py $@ ; }
pnfs() { $PYTHON_DIR/bin/python pnfs.py $@ ; }

PATH="$PATH:${ENSTORE_DIR}/bin"; export PATH

if [ "${PYTHONPATH:-1}" = "1" ]; then
   PYTHONPATH=$ENSTORE_DIR/src
else
   PYTHONPATH=${PYTHONPATH}:$ENSTORE_DIR/src
fi
export PYTHONPATH

encp()   { python $ENSTORE_DIR/src/encp.py $@ ; }
pnfs()   { python $ENSTORE_DIR/src/pnfs.py $@ ; }
econ()   { python $ENSTORE_DIR/src/configuration_client.py $@ ; }

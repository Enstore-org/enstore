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
fcc()    { python $ENSTORE_DIR/src/file_clerk_client.py $@ ; }
vcc()    { python $ENSTORE_DIR/src/volume_clerk_client.py $@ ; }

rddt()   { python $ENSTORE_DIR/src/driver.py $@ ; }
clbk()   { python $ENSTORE_DIR/src/callback.py $@ ; }
udpc()   { python $ENSTORE_DIR/src/udp_client.py $@ ; }
ranf()   { python $ENSTORE_DIR/src/ranfile.py $@ ; }
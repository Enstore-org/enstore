set path=($path ${ENSTORE_DIR})
rehash

if ("${?PYTHONPATH}" == "0") then
    setenv PYTHONPATH "${ENSTORE_DIR}/src:${ENSTORE_DIR}/modules"
else
    setenv PYTHONPATH "${PYTHONPATH}:${ENSTORE_DIR}/src:${ENSTORE_DIR}/modules"
endif

setenv PVER `cd $PYTHON_DIR/lib;ls -d python*`
setenv PYTHONINC $PYTHON_DIR/include/$PVER
setenv PYTHONLIB $PYTHON_DIR/lib/$PVER
setenv PVER `cd $PYTHON_DIR;ls -d Python*`
setenv PYTHONMOD $PYTHON_DIR/$PVER/Modules
unsetenv PVER

# alias encp   python $ENSTORE_DIR/src/encp.py '\!*'
# alias pnfs   python $ENSTORE_DIR/src/pnfs.py '\!*'

# alias config python $ENSTORE_DIR/src/configuration_client.py '\!*'
# alias fcc    python $ENSTORE_DIR/src/file_clerk_client.py '\!*'
# alias vcc    python $ENSTORE_DIR/src/volume_clerk_client.py '\!*'

# alias rddt   python $ENSTORE_DIR/src/driver.py '\!*'
# alias clbk   python $ENSTORE_DIR/src/callback.py '\!*'
# alias udpc   python $ENSTORE_DIR/src/udp_client.py '\!*'
# alias ranf   python $ENSTORE_DIR/src/ranfile.py '\!*'

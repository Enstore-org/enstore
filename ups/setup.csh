set path=($path ${ENSTORE_DIR}/bin)
rehash

if ("${?PYTHONPATH}" == "0") then
    setenv PYTHONPATH "${ENSTORE_DIR}/src:${ENSTORE_DIR}/modules:$LIBTPPY_DIR/lib"
else
    setenv PYTHONPATH "${PYTHONPATH}:${ENSTORE_DIR}/src:${ENSTORE_DIR}/modules:$LIBTPPY_DIR/lib"
endif

setenv PVER `cd $PYTHON_DIR/lib;ls -d python*`
setenv PYTHONINC $PYTHON_DIR/include/$PVER
setenv PYTHONLIB $PYTHON_DIR/lib/$PVER
setenv PVER `cd $PYTHON_DIR;ls -d Python*`
setenv PYTHONMOD $PYTHON_DIR/$PVER/Modules
unsetenv PVER

setenv ENSTORE_CONFIG_FILE $ENSTORE_DIR/etc/willow.conf
setenv ENSTORE_CONFIG_PORT 7510
setenv ENSTORE_CONFIG_HOST pcfarm9.fnal.gov
export ENSTORE_CONFIG_HOST

source $ENSTORE_DIR/etc/defaults

# . $ENSTORE_DIR/etc/pnfs.sh

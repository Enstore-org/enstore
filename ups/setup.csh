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

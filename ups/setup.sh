PATH="$PATH:${ENSTORE_DIR}/bin"
export PATH

if [ "${PYTHONPATH:-1}" = "1" ]; then
   PYTHONPATH=$ENSTORE_DIR/src:${ENSTORE_DIR}/modules
else
   PYTHONPATH=${PYTHONPATH}:$ENSTORE_DIR/src:${ENSTORE_DIR}/modules
fi
export PYTHONPATH

PVER=`cd $PYTHON_DIR/lib;ls -d python*`
PYTHONINC=$PYTHON_DIR/include/$PVER; export PYTHONINC
PYTHONLIB=$PYTHON_DIR/lib/$PVER;     export PYTHONLIB
PVER=`cd $PYTHON_DIR;ls -d Python*`
PYTHONMOD=$PYTHON_DIR/$PVER/Modules;     export PYTHONMOD

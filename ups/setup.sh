PATH="$PATH:${ENSTORE_DIR}/bin"
export PATH

if [ "${PYTHONPATH:-1}" = "1" ]; then
   PYTHONPATH=$ENSTORE_DIR/src:${ENSTORE_DIR}/modules
else
   PYTHONPATH=${PYTHONPATH}:$ENSTORE_DIR/src:${ENSTORE_DIR}/modules
fi
export PYTHONPATH

ver=`/bin/ls -d $PYTHON_DIR/lib/python*`
PVER=`basename $ver`
PYTHONINC=$PYTHON_DIR/include/$PVER
export PYTHONINC
PYTHONLIB=$PYTHON_DIR/lib/$PVER
export PYTHONLIB

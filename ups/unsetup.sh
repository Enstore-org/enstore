
PATH=`dropit /enstore/`; export PATH;

PYTHONPATH=`dropit -p"$PYTHONPATH" /enstore/`;
export PYTHONPATH


if [ "${PYTHONPATH:-1}" = "1" ]; then
    unset PYTHONPATH
fi

unset encp
unset pnfs
unset econ

unset rddt


set path=($path ${ENSTORE_DIR})
rehash

if ("${?PYTHONPATH}" == "0") then
    setenv PYTHONPATH "${ENSTORE_DIR}/src"
else
    setenv PYTHONPATH "${PYTHONPATH}:${ENSTORE_DIR}/src"
endif

alias encp $PYTHON_DIR/bin/python encp.py '\!*'
alias pnfs $PYTHON_DIR/bin/python pnfs.py '\!*'

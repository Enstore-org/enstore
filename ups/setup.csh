
set path=($path ${ENSTORE_DIR})
rehash

if ("${?PYTHONPATH}" == "0") then
    setenv PYTHONPATH "${ENSTORE_DIR}/src"
else
    setenv PYTHONPATH "${PYTHONPATH}:${ENSTORE_DIR}/src"
endif

alias encp $PYTHON_DIR/bin/python encp.py '\!*'
alias pnfs $PYTHON_DIR/bin/python pnfs.py '\!*'

alias econ $PYTHON_DIR/bin/python configuration_client.py '\!*'
alias bfid $PYTHON_DIR/bin/python file_clerk_client.py '\!*'
alias rddt $PYTHON_DIR/bin/python driver.py '\!*'
alias clbk $PYTHON_DIR/bin/python callback.py '\!*'
alias udpc $PYTHON_DIR/bin/python udp_client.py '\!*'

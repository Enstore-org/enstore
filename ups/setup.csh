set path=($path ${ENSTORE_DIR})
rehash

if ("${?PYTHONPATH}" == "0") then
    setenv PYTHONPATH "${ENSTORE_DIR}/src"
else
    setenv PYTHONPATH "${PYTHONPATH}:${ENSTORE_DIR}/src"
endif

alias encp python $ENSTORE_DIR/src/encp.py '\!*'
alias pnfs python $ENSTORE_DIR/src/pnfs.py '\!*'

alias econ python $ENSTORE_DIR/src/configuration_client.py '\!*'
alias fcc  python $ENSTORE_DIR/src/file_clerk_client.py '\!*'
alias vcc  python $ENSTORE_DIR/src/volume_clerk_client.py '\!*'

alias rddt python $ENSTORE_DIR/src/driver.py '\!*'
alias clbk python $ENSTORE_DIR/src/callback.py '\!*'
alias udpc python $ENSTORE_DIR/src/udp_client.py '\!*'
alias ranf python $ENSTORE_DIR/src/ranfile.py '\!*'

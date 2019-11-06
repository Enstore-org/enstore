ENCP_DIR="/opt/encp"; export ENCP_DIR
#
ENSTORE_CONFIG_HOST="`$ENCP_DIR/chooseConfig -c`"; export ENSTORE_CONFIG_HOST
#
SETUP_ENCP='encp  -f ANY -z /local/ups/db -r /opt/encp -m encp.table -M /opt/encp'; export SETUP_ENCP
#
if [ "${PATH:-}" = "" ]; then
  PATH="/opt/encp"
else
  PATH="/opt/encp:${PATH}"
fi
export PATH
#
ENSTORE_CONFIG_PORT="7500"; export ENSTORE_CONFIG_PORT

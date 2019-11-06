setenv ENCP_DIR "/opt/encp"
#
setenv ENSTORE_CONFIG_HOST "`$ENCP_DIR/chooseConfig -c`"
#
setenv SETUP_ENCP 'encp  -f ANY -z /local/ups/db -r /opt/encp -m encp.table -M /opt/encp'
#
if (! ${?PATH}) then
  setenv PATH "/opt/encp"
else
  setenv PATH "/opt/encp:${PATH}"
endif
#
rehash
#
setenv ENSTORE_CONFIG_PORT "7500"
#


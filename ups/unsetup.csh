# generic csh unsetup script
eval set PROD_DIR=\$`echo $UPS_PROD_NAME | tr '[a-z]' '[A-Z]'`_DIR
source $PROD_DIR/ups/unsetup.common
unset PROD_DIR

# generic csh setup script, jut put $PROD_DIR/bin in the path

eval set PROD_DIR=\$`echo $UPS_PROD_NAME | tr '[a-z]' '[A-Z]'`_DIR

source $PROD_DIR/ups/setup.common

unset PROD_DIR

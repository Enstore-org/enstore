# generic sh setup script, just put $PROD_DIR in the path
eval PROD_DIR=\$`echo $UPS_PROD_NAME | tr '[a-z]' '[A-Z]'`_DIR

setenv() {
	eval $1=\""$2"\"
	export $1
}

. $PROD_DIR/ups/setup.common

unset PROD_DIR

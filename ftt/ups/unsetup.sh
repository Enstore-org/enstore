# generic sh unsetup script
eval PROD_DIR=\$`echo $UPS_PROD_NAME | tr '[a-z]' '[A-Z]'`_DIR

setenv() {
	eval $1=\""$2"\"
	export $1
}
unsetenv() {
	unset "$1"
}

. $PROD_DIR/ups/unsetup.common

unset PROD_DIR

#!/bin/sh
# Fermilab internal enstore system use ups / upd product installation and setup
# utilities
# outside fermilab we do not have this environment
# this is to replace the functionality, provided by corresponding utility tha uses ups/upd
# the default place os this utility is /usr/local/etc/setups.sh
if [ "${ENSTORE_DIR:-x}" = "x" ];
then
	# this is a very trivial way to check if enstore is set
	# the usual location of enstore is /home/enstore

	if [ -a "/home/enstore/enstore/external_distr/setup-enstore"  ]; then
		. /home/enstore/enstore/external_distr/setup-enstore
	else
    		echo '****'
    		echo '**** Unable to initialize the UPSII environment'
    		echo '****'
		return
	fi
fi
# fakes setup function
setup() {
	return 0
}

# fake ups function
ups() {
	last=${!#}
	if [ $last == "enstore" ];
		then
		echo $ENSTORE_DIR
	else
		return
		#echo `type $last`
	fi 
}

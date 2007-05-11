#!/bin/sh
# Fermilab internal enstore system use ups / upd product installation and setup
# utilities
# outside fermilab we do not have this environment
# this is to replace the functionality, provided by corresponding utility tha uses ups/upd
# the default place os this utility is /usr/local/etc/setups.sh
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
if [ "${ENSTORE_DIR:-x}" = "x" ];
then
	# this is a very trivial way to check if enstore is set
	# the usual location of enstore is /home/enstore
	e_dir=
	#e_dir=`rpm -ql enstore_sa | head -1`

	if [ -a "${e_dir}/config/setup-enstore"  ]; then
		source ${e_dir}/config/setup-enstore
		return 0
	else
    		echo '****'
    		echo '**** Unable to initialize the UPSII environment'
    		echo '****'
		return 1
	fi
fi


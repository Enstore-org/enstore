#!/bin/sh

if [ "_root" != _`whoami` ]
then
	echo "enroute2 installation failed: not enough privilege"
	exit 1
fi

cp enroute2 /usr/local/bin/enroute2
chown root.sys /usr/local/bin/enroute2
chmod u+s /usr/local/bin/enroute2

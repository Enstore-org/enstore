#!/bin/sh

DIR=/tmp/ENSTORE_UPDATE_TMP
if [ ! -d $DIR ]
then
	mkdir -p $DIR
fi
cd $DIR
cvs co enstore
PATH=$DIR/enstore/bin:$PATH
cd $DIR/enstore/doc/WWW
make all
make install_home
cd $DIR
rm -rf enstore

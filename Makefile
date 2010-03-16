##############################################################################
#
#  Makefile to build xml2ddl
#
# Dmitry Litvintsev (litvinse@fnal.gov) 07/09
#
##############################################################################

all:
	@echo "building psycopg2"
	python setup.py build --executable="/usr/bin/env python" --build-lib=../modules --build-scripts=../bin
	python setup.py clean
	rm -rf build
	cd ../modules/xml2ddl && python -m compileall .
clean:
	rm -rf ../modules/xml2ddl
	rm ../bin/diffxml2ddl ../bin/downloadXml  ../bin/xml2ddl  ../bin/xml2html
	@echo "Cleaning xml2ddl"

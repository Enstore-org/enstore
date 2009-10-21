##############################################################################
#
#  Makefile to build xml2ddl
#
# Dmitry Litvintsev (litvinse@fnal.gov) 07/09
# 
##############################################################################

all:
	@echo "building psycopg2"
	CFLAGS="-m32" python setup.py build --build-lib=../modules --build-scripts=../bin  
	python setup.py clean
	cd ../modules/xml2ddl && python -m compileall .
clean:
	rm -rf ../modules/xml2ddl
	rm -rf build 
	cd ../bin && rm diffxml2ddl downloadXml  xml2ddl  xml2html
	@echo "Cleaning xml2ddl"

##############################################################################
#
#  Makefile to build psycopg2
#
# Dmitry Litvintsev (litvinse@fnal.gov) 07/09
# 
##############################################################################

all:
	@echo "building psycopg2"
	python setup.py build --build-lib=../modules
#	CFLAGS="-m32" python setup.py build --build-lib=../modules
	python setup.py clean
	cd ../modules/psycopg2 && python -m compileall .
clean:
	rm -rf ../modules/psycopg2
	rm -rf build
	@echo "Cleaning psycopg2"

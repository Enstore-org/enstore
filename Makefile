##############################################################################
#
#  Makefile to build psycopg2
#
# Dmitry Litvintsev (litvinse@fnal.gov) 07/09
# 
##############################################################################

all:
	@echo "building psycopg2"
	CFLAGS="-m32" python setup.py build --build-lib=../modules
	python setup.py clean
	cd ../modules/psycopg2 && python -m compileall .
clean:
	@echo "Cleaning psycopg2"

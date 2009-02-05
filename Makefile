##############################################################################
#
#  Makefile to build py
#
# Dmitry Litvintsev (litvinse@fnal.gov) 01/09
# 
##############################################################################


PGINC=$(shell pg_config --includedir)
PSINC=$(shell pg_config --includedir-server)
PGLIB=$(shell pg_config --libdir)
CFLAGS=fpic 
all: _pg.so

_pg.so:
	$(CC) -$(CFLAGS) -shared -o $@ -I$(PYTHONINC) -I$(PGINC) -I$(PSINC) -L$(PGLIB) -lpq pgmodule.c
clean:
	-rm -rf _pg.so

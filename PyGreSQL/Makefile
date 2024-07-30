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
CFLAGS=-fpic
all: _pg.so

_pg.so: pgmodule.o snprintf.o strerror.o
	$(CC) $(CFLAGS) -shared -o $@ -I$(PYTHONINC) -I$(PYTHONINC)/site-packages -I$(PGINC) -I$(PSINC) -I/usr/include -L$(PGLIB) -lpq pgmodule.o snprintf.o strerror.o

pgmodule.o: pgmodule.c
	$(CC) $(CFLAGS) -I$(PYTHONINC) -I$(PGINC) -I$(PSINC) -c pgmodule.c

snprintf.o: snprintf.c
	$(CC) $(CFLAGS) -I$(PYTHONINC) -I$(PGINC) -I$(PSINC) -c snprintf.c

strerror.o: strerror.c
	$(CC) $(CFLAGS) -I$(PYTHONINC) -I$(PGINC) -I$(PSINC) -c strerror.c

clean:
	-rm -rf _pg.so pgmodule.o snprintf.o strerror.o
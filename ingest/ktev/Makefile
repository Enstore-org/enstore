#
# $Id$
#

# Top Level Makfile

BIN_PATH = /afs/.desy.de/products/dcache

CP    = cp
RM    = rm -f
LINK  = ln -s
MV    = mv

CFLAGS = -g
INC = -I$(DCAP_DIR)/include -I.
LIBS = -L$(DCAP_DIR)/lib -L. -ldcap libktev.a
LIB_SRC = ktevapi.c
LIB_OBJS = ${LIB_SRC:.c=.o}


#ktev_main: libktev.a ktev_main.o
#	$(CC) $(LIBS) -o $@ ktev_main.o libktev.a libdcap.a
ktev_main: libktev.so ktev_main.o
	cc -o $@ ktev_main.o $(LIBS) -lc 

ktev_main.o: ktev_main.c $(LIB_SRC)
	 $(CC) $(CFLAGS) $(INC) -c ktev_main.c

libktev.so: libktev.a
	ld -shared -o $@ $(OBJS) $(LIBS) -lc
libktev.a: $(LIB_OBJS)
	ar cr $@ $(LIB_OBJS)

$(LIB_OBJS): $(LIB_SRC) ktevapi.h
	$(CC) $(CFLAGS) $(INC) -c $(LIB_SRC)

clean:
	rm -f *.o *.BAK libktev.a *.so core

#
# $Id$
#

CFLAGS = -g
INC = -I$(DCAP_DIR)/include -I$(KTEV_DCAP_API_DIR)/inc -I.
#LIBS = -L$(DCAP_DIR)/lib -L. -ldcap libktev.a
LIBS = libktev.a $(DCAP_DIR)/lib/libdcap.a libktev.a
OS_DEP_LIB = `if [ $(MACH_OS) = "Linux" ]; then echo "-ldl -lpthread"; else echo ""; fi` 
LIB_SRC = ktevapi.c
LIB_OBJS = ${LIB_SRC:.c=.o}

all: libktev.a libktev.so ktev_main

ktev_main: libktev.a ktev_main.o
	$(CC) -o $@ ktev_main.o $(LIBS) $(OS_DEP_LIB)
#ktev_main: libktev.so ktev_main.o
#	cc -o $@ ktev_main.o $(LIBS) -lc 

ktev_main.o: ktev_main.c
	 $(CC) $(CFLAGS) $(INC) -c ktev_main.c

libktev.so: libktev.a
	ld -shared -o $@ $(OBJS) $(LIBS) -lc
libktev.a: $(LIB_OBJS)
	ar cr $@ $(LIB_OBJS)

$(LIB_OBJS): $(LIB_SRC) $(KTEV_DCAP_API_DIR)/inc/ktevapi.h
	$(CC) $(CFLAGS) $(INC) -c $(LIB_SRC)

install:
	mv $(KTEV_DCAP_API_DIR)/src/libktev.* $(KTEV_DCAP_API_DIR)/lib
	mv $(KTEV_DCAP_API_DIR)/src/ktev_main $(KTEV_DCAP_API_DIR)/test
	rm $(KTEV_DCAP_API_DIR)/src/*.o

clean:
	rm -f $(KTEV_DCAP_API_DIR)/test/* $(KTEV_DCAP_API_DIR)/src/*.o $(KTEV_DCAP_API_DIR)/src/*.so
	rm -f $(KTEV_DCAP_API_DIR)/src/*.a $(KTEV_DCAP_API_DIR)/lib/*

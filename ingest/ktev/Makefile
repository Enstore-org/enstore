#
# $Id$
#

# Top Level Makfile

CFLAGS = -g
INC = -I$(DCAP_DIR)/include -I.
#LIBS = -L$(DCAP_DIR)/lib -L. -ldcap libktev.a
LIBS = libktev.a libdcap.a
OS_DEP_LIB = `if [ $(MACH_OS) = "Linux" ]; then echo "-ldl -lpthread"; else echo ""; fi` 
LIB_SRC = ktevapi.c
LIB_OBJS = ${LIB_SRC:.c=.o}


ktev_main: libktev.a ktev_main.o
	@echo "OS $(MACH_OS)"
	@echo "OPT" 
	$(CC) -static -o $@ ktev_main.o $(LIBS) $(OS_DEP_LIB)
#ktev_main: libktev.so ktev_main.o
#	cc -o $@ ktev_main.o $(LIBS) -lc 

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

#
#  $Id$
#


#
#  put here all sources which becomes an executable
#

ERR = /dev/null
BIN_SRC = ktevMainV0.c

SRC = interface.c getopt.c ktevApiV0.c

OBJS = $(sort ${SRC:.c=.o})
BIN_EXEC = ${BIN_SRC:.c=}

all: libktev$(SO_EXT) $(BIN_EXEC)

$(OBJS): Makefile.inc

LIBS = -L$(DCAP_DIR) -L. -ldcap 

mapfile: interface.h
	@grep -v include interface.h > interface.h_map
	$(CPP) $(CFLAGS) interface.h_map | ./mkmapfile.sh > mapfile
	@rm -f interface.h_map

ktev_test: $(OBJS) $(DCAP_O) ktev_test.o
	$(CC) $(EXE_INIT) -o $@$(EXE_EXT) ktev_test.o $(OBJS) $(LIBS)

ktevV0: $(OBJS) $(DCAP_O) ktevV0.o
	$(CC) $(EXE_INIT) -o $@$(EXE_EXT) ktevV0.o $(OBJS) $(LIBS)

ktevMainV0: $(OBJS) $(DCAP_O) ktevMainV0.o
	$(CC) $(EXE_INIT) -o $@$(EXE_EXT) ktevMainV0.o $(LIBS) -lktev

libktev$(SO_EXT): libktev$(A_EXT) 
	$(LD) $(LDFLAGS) $(MAP) $(SO_INIT) -o $@ $(OBJS) $(LIBS) -lc

libktev$(A_EXT): $(OBJS) 
	$(AR) $(ARFLAGS) $@ $(OBJS)
	$(RANLIB) $@


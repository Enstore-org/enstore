#
#  $Id$
#


#
#  put here all sources which becomes an executable
#

ERR = /dev/null
BIN_SRC = ktev_test.c ktevV0.c

SRC = interface.c getopt.c

OBJS = $(sort ${SRC:.c=.o})
BIN_EXEC = ${BIN_SRC:.c=}

all: $(BIN_EXEC)

$(OBJS): Makefile.inc

LIBS = -L$(DCAP_DIR) -ldcap 

mapfile: interface.h
	@grep -v include interface.h > interface.h_map
	$(CPP) $(CFLAGS) interface.h_map | ./mkmapfile.sh > mapfile
	@rm -f interface.h_map

ktev_test: $(OBJS) $(DCAP_O) ktev_test.o
	$(CC) $(EXE_INIT) -o $@$(EXE_EXT) ktev_test.o $(OBJS) $(LIBS)

ktevV0: $(OBJS) $(DCAP_O) ktevV0.o
	$(CC) $(EXE_INIT) -o $@$(EXE_EXT) ktevV0.o $(OBJS) $(LIBS)

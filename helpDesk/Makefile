#
# Parameters.

PROGRAM   = fliptabTicket
SOURCES   = fliptabTicket.c hack.c
OBJECTS   = fliptabTicket.o hack.o

# Compiler flags.

CC = cc
CFLAGS   = -g -DDEBUG -D_REENTRANT 
# CPPFLAGS = -I../include 
CPPFLAGS = -I$(REMEDY_API_DIR)/include
# LDFLAGS  = -L../lib
LDFLAGS  = -L$(REMEDY_API_DIR)/lib
LDLIBS   = -lar -lnsl -lpthread -lgen_util

# Standard targets.

all:	 $(PROGRAM)

objects: $(OBJECTS)

$(PROGRAM): $(OBJECTS)
	$(CC) -o $(PROGRAM) $(OBJECTS) $(LDFLAGS) $(LDLIBS)

clean:
	$(RM) $(PROGRAM) $(OBJECTS) core

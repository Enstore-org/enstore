INC = -I$(FTT_DIR)/include
LIB = -L$(FTT_DIR)/lib -lftt `cat $(FTT_DIR)/lib/libs`
CFLAGS = $(INC)
LDFLAGS = $(LIB)

EXAMPLES = tapecopy

all: $(EXAMPLES)

tapecopy: tapecopy.c
	$(CC) $(CFLAGS) -o tapecopy tapecopy.c $(LIB)

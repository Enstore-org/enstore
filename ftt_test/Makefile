SHELL=/bin/sh
OS="`uname -s`+`uname -r | cut -c1`"

LIBS=-L. -L../lib -lftt_test -lftt `cat libs.$$OS`

SRC= ftt_t_cmdline.c ftt_t_cmdloop.c ftt_t_cmdproc.c ftt_t_parse.c \
 ftt_t_stats.c ftt_t_rdwr.c ftt_t_open.c ftt_t_position.c ftt_t_misc.c \
 ftt_t_error.c ftt_t_block.c ftt_t_name.c ftt_t_label.c

OBJ= ftt_t_cmdline.o ftt_t_cmdloop.o ftt_t_cmdproc.o ftt_t_parse.o \
 ftt_t_stats.o ftt_t_rdwr.o ftt_t_open.o ftt_t_position.o ftt_t_misc.o \
 ftt_t_error.o ftt_t_block.o ftt_t_name.o ftt_t_label.o

# CFLAGS=-I. -I../ftt_lib -g -xansi -fullwarn 
CFLAGS=-I. -I../ftt_lib -g 

all: ftt_test

install: all
	cp libftt_test.a $(FTT_DIR)/lib
	cp ftt_test $(FTT_DIR)/test
	cp ftt_test $(FTT_DIR)/bin

libftt_test.a: $(OBJ)
	rm -f libftt_test.a
	ar qv libftt_test.a $(OBJ)

ftt_test: ftt_test.c libftt_test.a ../lib/libftt.a 
	OS=$(OS); $(CC) $(CFLAGS) -o ftt_test ftt_test.c  $(LIBS)

SHELL=/bin/sh

LIBS=-L. -L../ftt_lib -lftt_test -lftt `cat ../ftt_lib/local/libs`

SRC= ftt_t_cmdline.c ftt_t_cmdloop.c ftt_t_cmdproc.c ftt_t_parse.c \
 ftt_t_stats.c ftt_t_rdwr.c ftt_t_open.c ftt_t_position.c ftt_t_misc.c \
 ftt_t_error.c ftt_t_block.c ftt_t_name.c ftt_t_label.c ftt_t_partition.c

OBJ= ftt_t_cmdline.o ftt_t_cmdloop.o ftt_t_cmdproc.o ftt_t_parse.o \
 ftt_t_stats.o ftt_t_rdwr.o ftt_t_open.o ftt_t_position.o ftt_t_misc.o \
 ftt_t_error.o ftt_t_block.o ftt_t_name.o ftt_t_label.o ftt_t_partition.o

# CFLAGS=-I. -I../ftt_lib -g -xansi -fullwarn 
CFLAGS=-I. -I../ftt_lib -g $(WARN) `cat ../ftt_lib/local/flags`

BINDIR=../bin
MAIDIR=..
LIBDIR=../lib

all: ftt_test ftt_random ftt_copy

scripts: \
	$(BINDIR)/ftt_ansi_prelabel \
	$(BINDIR)/ftt_dump \
	$(BINDIR)/ftt_exercise \
	$(BINDIR)/ftt_fast \
	$(BINDIR)/ftt_multitest \
	$(BINDIR)/ftt_run_test \
	$(BINDIR)/ftt_slow 

exes: \
	$(BINDIR)/ftt_test \
	$(BINDIR)/ftt_copy \
	$(BINDIR)/ftt_random 

testlib: $(LIBDIR)/libftt_test.a

clean:
	rm -f *.o ftt_test ftt_random ftt_copy

install: all testlib exes scripts 

$(LIBDIR)/libftt_test.a: libftt_test.a 
	cp libftt_test.a $(LIBDIR)

$(MAIDIR)/test: ftt_test
	-mv $(MAIDIR)/test $(MAIDIR)/test.bak
	cp ftt_test $(MAIDIR)/test

$(MAIDIR)/random: ftt_random 
	-mv $(MAIDIR)/random $(MAIDIR)/random.bak
	cp ftt_random $(MAIDIR)/random

$(MAIDIR)/copy: ftt_copy
	-mv $(MAIDIR)/copy $(MAIDIR)/copy.bak
	cp ftt_copy $(MAIDIR)/copy

$(BINDIR)/ftt_test: ftt_test
	-mv $(MAIDIR)/bin/ftt_test $(MAIDIR)/bin/ftt_test.bak
	cp ftt_test $(MAIDIR)/bin

$(BINDIR)/ftt_random: ftt_random
	-mv $(MAIDIR)/bin/ftt_random $(MAIDIR)/bin/ftt_random.bak
	cp ftt_random $(MAIDIR)/bin

$(BINDIR)/ftt_copy: ftt_copy
	-mv $(MAIDIR)/bin/ftt_copy $(MAIDIR)/bin/ftt_copy.bak
	cp ftt_copy $(MAIDIR)/bin

$(BINDIR)/ftt_ansi_prelabel: scripts/ftt_ansi_prelabel
	cp scripts/ftt_ansi_prelabel $(BINDIR)/

$(BINDIR)/ftt_dump: scripts/ftt_dump
	cp scripts/ftt_dump $(BINDIR)/

$(BINDIR)/ftt_exercise: scripts/ftt_exercise
	cp scripts/ftt_exercise $(BINDIR)/

$(BINDIR)/ftt_fast: scripts/ftt_fast  
	cp scripts/ftt_fast $(BINDIR)/

$(BINDIR)/ftt_multitest: scripts/ftt_multitest
	cp scripts/ftt_multitest $(BINDIR)/

$(BINDIR)/ftt_run_test: scripts/ftt_run_test
	cp scripts/ftt_run_test $(BINDIR)/

$(BINDIR)/ftt_slow: scripts/ftt_slow
	cp scripts/ftt_slow $(BINDIR)/


libftt_test.a: $(OBJ)
	rm -f libftt_test.a
	ar qv libftt_test.a $(OBJ)

ftt_test: ftt_test.c libftt_test.a ../ftt_lib/libftt.a 
	$(CC) $(CFLAGS) -o ftt_test ftt_test.c  $(LIBS)

ftt_random: ftt_random.c libftt_test.a ../ftt_lib/libftt.a 
	$(CC) $(CFLAGS) -o ftt_random ftt_random.c  $(LIBS)

ftt_copy: ftt_copy.c libftt_test.a ../ftt_lib/libftt.a 
	$(CC) $(CFLAGS) -o ftt_copy ftt_copy.c  $(LIBS)

ftt_t_cmdline.o :       ftt_t_cmdline.c        			\
                        ftt_t_cmdline.h
 
ftt_t_cmdloop.o :       ftt_t_cmdloop.c        			\
                        ftt_t_cmdline.h        			\
                        ftt_t_parse.h          			\
                        ftt_t_cmdtable.h
 
ftt_t_cmdproc.o :       ftt_t_cmdproc.c        			\
                        ftt_t_cmdline.h        			\
                        ftt_t_parse.h          			\
                        ftt_t_cmdtable.h

ftt_t_parse.o :         ftt_t_parse.c				\
			ftt_t_parse.h
 
ftt_t_block.o :         ftt_t_block.c           
 
ftt_t_error.o :		ftt_t_error.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			../ftt_lib/ftt.h		\
			../ftt_lib/ftt_defines.h	\
			../ftt_lib/ftt_voidtypes.h	\
			../ftt_lib/ftt_common.h	

ftt_t_label.o :		ftt_t_label.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			../ftt_lib/ftt.h		\
			../ftt_lib/ftt_defines.h	\
			../ftt_lib/ftt_voidtypes.h	\
			../ftt_lib/ftt_common.h	

ftt_t_misc.o :		ftt_t_misc.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			../ftt_lib/ftt.h		\
			../ftt_lib/ftt_defines.h	\
			../ftt_lib/ftt_voidtypes.h	\
			../ftt_lib/ftt_common.h	

ftt_t_name.o :		ftt_t_name.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			../ftt_lib/ftt.h		\
			../ftt_lib/ftt_defines.h	\
			../ftt_lib/ftt_voidtypes.h	\
			../ftt_lib/ftt_common.h	

ftt_t_open.o :		ftt_t_open.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			../ftt_lib/ftt.h		\
			../ftt_lib/ftt_defines.h	\
			../ftt_lib/ftt_voidtypes.h	\
			../ftt_lib/ftt_common.h	

ftt_t_position.o :	ftt_t_position.c			\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			../ftt_lib/ftt.h		\
			../ftt_lib/ftt_defines.h	\
			../ftt_lib/ftt_voidtypes.h	\
			../ftt_lib/ftt_common.h	

ftt_t_rdwr.o :		ftt_t_rdwr.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			../ftt_lib/ftt.h		\
			../ftt_lib/ftt_defines.h	\
			../ftt_lib/ftt_voidtypes.h	\
			../ftt_lib/ftt_common.h	

ftt_t_stats.o :		ftt_t_stats.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			../ftt_lib/ftt.h		\
			../ftt_lib/ftt_defines.h	\
			../ftt_lib/ftt_voidtypes.h	\
			../ftt_lib/ftt_common.h	


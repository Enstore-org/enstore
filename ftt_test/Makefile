SHELL=/bin/sh

LIBS=-L. -L../ftt_lib -lftt_test -lftt `cat ../ftt_lib/local/libs`

SRC= ftt_t_cmdline.c ftt_t_cmdloop.c ftt_t_cmdproc.c ftt_t_parse.c \
 ftt_t_stats.c ftt_t_rdwr.c ftt_t_open.c ftt_t_position.c ftt_t_misc.c \
 ftt_t_error.c ftt_t_block.c ftt_t_name.c ftt_t_label.c

OBJ= ftt_t_cmdline.o ftt_t_cmdloop.o ftt_t_cmdproc.o ftt_t_parse.o \
 ftt_t_stats.o ftt_t_rdwr.o ftt_t_open.o ftt_t_position.o ftt_t_misc.o \
 ftt_t_error.o ftt_t_block.o ftt_t_name.o ftt_t_label.o

# CFLAGS=-I. -I../ftt_lib -g -xansi -fullwarn 
CFLAGS=-I. -I../ftt_lib -g $(WARN) `cat ../ftt_lib/local/flags`

BIN=$(FTT_DIR)/bin

all: ftt_test ftt_random ftt_copy

scripts: \
	$(BIN)/ftt_ansi_prelabel \
	$(BIN)/ftt_dump \
	$(BIN)/ftt_exercise \
	$(BIN)/ftt_fast \
	$(BIN)/ftt_multitest \
	$(BIN)/ftt_run_test \
	$(BIN)/ftt_slow 

exes: \
	$(BIN)/ftt_test \
	$(FTT_DIR)/test \
	$(BIN)/ftt_copy \
	$(FTT_DIR)/copy \
	$(BIN)/ftt_random \
	$(FTT_DIR)/random 

testlib: $(FTT_DIR)/lib/libftt_test.a

clean:
	rm *.o ftt_test ftt_random ftt_copy

install: all testlib exes scripts 

$(FTT_DIR)/lib/libftt_test.a: libftt_test.a 
	cp libftt_test.a $(FTT_DIR)/lib

$(FTT_DIR)/test: ftt_test
	-mv $(FTT_DIR)/test $(FTT_DIR)/test.bak
	cp ftt_test $(FTT_DIR)/test

$(FTT_DIR)/random: ftt_random 
	-mv $(FTT_DIR)/random $(FTT_DIR)/random.bak
	cp ftt_random $(FTT_DIR)/random

$(FTT_DIR)/copy: ftt_copy
	-mv $(FTT_DIR)/copy $(FTT_DIR)/copy.bak
	cp ftt_copy $(FTT_DIR)/copy

$(BIN)/ftt_test: ftt_test
	-mv $(FTT_DIR)/bin/ftt_test $(FTT_DIR)/bin/ftt_test.bak
	cp ftt_test $(FTT_DIR)/bin

$(BIN)/ftt_random: ftt_random
	-mv $(FTT_DIR)/bin/ftt_random $(FTT_DIR)/bin/ftt_random.bak
	cp ftt_random $(FTT_DIR)/bin

$(BIN)/ftt_copy: ftt_copy
	-mv $(FTT_DIR)/bin/ftt_copy $(FTT_DIR)/bin/ftt_copy.bak
	cp ftt_copy $(FTT_DIR)/bin

$(BIN)/ftt_ansi_prelabel: scripts/ftt_ansi_prelabel
	cp scripts/ftt_ansi_prelabel $(BIN)/

$(BIN)/ftt_dump: scripts/ftt_dump
	cp scripts/ftt_dump $(BIN)/

$(BIN)/ftt_exercise: scripts/ftt_exercise
	cp scripts/ftt_exercise $(BIN)/

$(BIN)/ftt_fast: scripts/ftt_fast  
	cp scripts/ftt_fast $(BIN)/

$(BIN)/ftt_multitest: scripts/ftt_multitest
	cp scripts/ftt_multitest $(BIN)/

$(BIN)/ftt_run_test: scripts/ftt_run_test
	cp scripts/ftt_run_test $(BIN)/

$(BIN)/ftt_slow: scripts/ftt_slow
	cp scripts/ftt_slow $(BIN)/


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


SHELL=/bin/sh

LIBS=-L. -L../ftt_lib -lftt_test -lftt `cat ../ftt_lib/local/libs`

SRC= ftt_t_cmdline.c ftt_t_cmdloop.c ftt_t_cmdproc.c ftt_t_parse.c \
 ftt_t_stats.c ftt_t_rdwr.c ftt_t_open.c ftt_t_position.c ftt_t_misc.c \
 ftt_t_error.c ftt_t_block.c ftt_t_name.c ftt_t_label.c

OBJ= ftt_t_cmdline.o ftt_t_cmdloop.o ftt_t_cmdproc.o ftt_t_parse.o \
 ftt_t_stats.o ftt_t_rdwr.o ftt_t_open.o ftt_t_position.o ftt_t_misc.o \
 ftt_t_error.o ftt_t_block.o ftt_t_name.o ftt_t_label.o

# CFLAGS=-I. -I../ftt_lib -g -xansi -fullwarn 
CFLAGS=-I. -I../ftt_lib -g $(WARN)


all: ftt_test ftt_random

clean:
	rm $(OBJ) ftt_test ftt_random

install: all
	cp libftt_test.a $(FTT_DIR)/lib
	-mv $(FTT_DIR)/test $(FTT_DIR)/test.bak
	-mv $(FTT_DIR)/random $(FTT_DIR)/random.bak
	cp ftt_test $(FTT_DIR)/test
	cp ftt_random $(FTT_DIR)/random
	-mv $(FTT_DIR)/bin/ftt_test $(FTT_DIR)/bin/ftt_test.bak
	-mv $(FTT_DIR)/bin/ftt_random $(FTT_DIR)/bin/ftt_random.bak
	cp ftt_test $(FTT_DIR)/bin
	cp ftt_random $(FTT_DIR)/bin
	cp scripts/ftt_* $(FTT_DIR)/bin

libftt_test.a: $(OBJ)
	rm -f libftt_test.a
	ar qv libftt_test.a $(OBJ)

ftt_test: ftt_test.c libftt_test.a ../ftt_lib/libftt.a 
	$(CC) $(CFLAGS) -o ftt_test ftt_test.c  $(LIBS)

ftt_random: ftt_random.c libftt_test.a ../lib/libftt.a 
	$(CC) $(CFLAGS) -o ftt_random ftt_random.c  $(LIBS)

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
			$(FTT_DIR)/ftt_lib/ftt.h		\
			$(FTT_DIR)/ftt_lib/ftt_defines.h	\
			$(FTT_DIR)/ftt_lib/ftt_voidtypes.h	\
			$(FTT_DIR)/ftt_lib/ftt_common.h	

ftt_t_label.o :		ftt_t_label.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			$(FTT_DIR)/ftt_lib/ftt.h		\
			$(FTT_DIR)/ftt_lib/ftt_defines.h	\
			$(FTT_DIR)/ftt_lib/ftt_voidtypes.h	\
			$(FTT_DIR)/ftt_lib/ftt_common.h	

ftt_t_misc.o :		ftt_t_misc.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			$(FTT_DIR)/ftt_lib/ftt.h		\
			$(FTT_DIR)/ftt_lib/ftt_defines.h	\
			$(FTT_DIR)/ftt_lib/ftt_voidtypes.h	\
			$(FTT_DIR)/ftt_lib/ftt_common.h	

ftt_t_name.o :		ftt_t_name.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			$(FTT_DIR)/ftt_lib/ftt.h		\
			$(FTT_DIR)/ftt_lib/ftt_defines.h	\
			$(FTT_DIR)/ftt_lib/ftt_voidtypes.h	\
			$(FTT_DIR)/ftt_lib/ftt_common.h	

ftt_t_open.o :		ftt_t_open.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			$(FTT_DIR)/ftt_lib/ftt.h		\
			$(FTT_DIR)/ftt_lib/ftt_defines.h	\
			$(FTT_DIR)/ftt_lib/ftt_voidtypes.h	\
			$(FTT_DIR)/ftt_lib/ftt_common.h	

ftt_t_position.o :	ftt_t_position.c			\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			$(FTT_DIR)/ftt_lib/ftt.h		\
			$(FTT_DIR)/ftt_lib/ftt_defines.h	\
			$(FTT_DIR)/ftt_lib/ftt_voidtypes.h	\
			$(FTT_DIR)/ftt_lib/ftt_common.h	

ftt_t_rdwr.o :		ftt_t_rdwr.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			$(FTT_DIR)/ftt_lib/ftt.h		\
			$(FTT_DIR)/ftt_lib/ftt_defines.h	\
			$(FTT_DIR)/ftt_lib/ftt_voidtypes.h	\
			$(FTT_DIR)/ftt_lib/ftt_common.h	

ftt_t_stats.o :		ftt_t_stats.c				\
			ftt_t_parse.h				\
			ftt_t_macros.h				\
			$(FTT_DIR)/ftt_lib/ftt.h		\
			$(FTT_DIR)/ftt_lib/ftt_defines.h	\
			$(FTT_DIR)/ftt_lib/ftt_voidtypes.h	\
			$(FTT_DIR)/ftt_lib/ftt_common.h	


# $Id$

PROGS=enstore_write_tape enstore_init_tape

WRITE_OBJS=add_to_tape.o do_add_file.o verify.o dbfuncs.o tape.o \
	cpio.o pathutil.o ECRC.o global_data.o	

INIT_OBJS=init_tape.o verify.o tape.o dbfuncs.o pathutil.o global_data.o 

HDRS=volume_import.h


all: $(PROGS)

enstore_write_tape: $(WRITE_OBJS)
	gcc -g -o enstore_write_tape $(WRITE_OBJS)

enstore_init_tape: $(INIT_OBJS)
	gcc -g -o enstore_init_tape $(INIT_OBJS)

add_to_tape.o: add_to_tape.c $(HDRS)
	gcc -c -g -Wall add_to_tape.c

init_tape.o: init_tape.c $(HDRS)
	gcc -c -g -Wall init_tape.c

do_add_file.o: do_add_file.c $(HDRS)
	gcc -c -g -Wall do_add_file.c

dbfuncs.o: dbfuncs.c $(HDRS)
	gcc -c -g -Wall dbfuncs.c

verify.o: verify.c $(HDRS)
	gcc -c -g -Wall verify.c

tape.o: tape.c $(HDRS)
	gcc -c -g -Wall tape.c

ECRC.o: ECRC.c $(HDRS)
	gcc -c -g -Wall ECRC.c

cpio.o: cpio.c $(HDRS)
	gcc -c -g -Wall cpio.c

pathutil.o: pathutil.c $(HDRS)
	gcc -c -g -Wall pathutil.c

clean:
	rm -f *.o $(PROGS)




# $Id$

PROGS=enstore_write_tape enstore_init_tape

WRITE_OBJS=add_to_tape.o do_add_file.o verify.o dbfuncs.o tape.o \
	cpio.o pathutil.o ECRC.o time.o global_data.o	

INIT_OBJS=init_tape.o verify.o tape.o dbfuncs.o pathutil.o time.o \
	global_data.o 

HDRS=volume_import.h

all: $(PROGS)

#For Linux
CC=gcc -Wall
#For Irix and others
#CC=cc

enstore_write_tape: $(WRITE_OBJS)
	$(CC) -o enstore_write_tape $(WRITE_OBJS)

enstore_init_tape: $(INIT_OBJS)
	$(CC) -o enstore_init_tape $(INIT_OBJS)

add_to_tape.o: add_to_tape.c $(HDRS)
	$(CC) -c add_to_tape.c

init_tape.o: init_tape.c $(HDRS)
	$(CC) -c init_tape.c

do_add_file.o: do_add_file.c $(HDRS)
	$(CC) -c do_add_file.c

dbfuncs.o: dbfuncs.c $(HDRS)
	$(CC) -c dbfuncs.c

verify.o: verify.c $(HDRS)
	$(CC) -c verify.c

tape.o: tape.c $(HDRS)
	$(CC) -c tape.c

ECRC.o: ECRC.c $(HDRS)
	$(CC) -c ECRC.c

cpio.o: cpio.c $(HDRS)
	$(CC) -c cpio.c

pathutil.o: pathutil.c $(HDRS)
	$(CC) -c pathutil.c

time.o: time.c $(HDRS)
	$(CC) -c time.c

clean:
	rm -f *.o $(PROGS)




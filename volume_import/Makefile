# $Id$

PROGS=enstore_tape

OBJS=ECRC.o add_to_tape.o cpio.o dbfuncs.o do_add_file.o dump_db.o \
 global_data.o init_tape.o main.o pathutil.o tape.o time.o verify.o

HDRS=volume_import.h

all: $(PROGS)

#For Linux
CC=gcc -Wall
#For Irix and others
#CC=cc

enstore_tape: $(OBJS)
	$(CC) -o enstore_tape $(OBJS)

main.o: main.c
	$(CC) -c main.c

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

dump_db.o: dump_db.c
	$(CC) -c dump_db.c
clean:
	rm -f *.o $(PROGS)




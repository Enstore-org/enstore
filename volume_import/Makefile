# $Id$

PROGS=add_to_tape init_tape

ADD_OBJS=add_to_tape.o do_add_file.o verify.o dbfuncs.o tape.o ECRC.o
INIT_OBJS=init_tape.o verify.o tape.o

HDRS=volume_import.h globals.h


all: $(PROGS)

add_to_tape: $(ADD_OBJS)
	gcc -g -o add_to_tape $(ADD_OBJS)

init_tape: $(INIT_OBJS)
	gcc -g -o init_tape $(INIT_OBJS)

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

clean:
	rm -f *.o $(PROGS)




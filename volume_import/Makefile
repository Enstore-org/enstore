all: add_to_tape

add_to_tape: add_to_tape.o do_add_file.o dbfuncs.o ECRC.o
	gcc -g -o add_to_tape add_to_tape.o do_add_file.o dbfuncs.o ECRC.o

add_to_tape.o: add_to_tape.c
	gcc -c -g -Wall add_to_tape.c

do_add_file.o: do_add_file.c
	gcc -c -g -Wall do_add_file.c

dbfuncs.o: dbfuncs.c
	gcc -c -g -Wall dbfuncs.c

ECRC.o: ECRC.c
	gcc -c -g -Wall ECRC.c

clean:
	rm -f *.o add_to_tape



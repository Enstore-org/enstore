# XXX rewrite these assignments to use gnu autoconf or something similar!
CC=gcc
CFLAGS=-Wall -g

all: add_to_tape

add_to_tape: add_to_tape.o
	$(CC)  -o add_to_tape add_to_tape.o

add_to_tape.o: add_to_tape.c
	$(CC) $(CFLAGS) -c add_to_tape.c

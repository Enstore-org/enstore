# This file (trace.mk) was created by Ron Rechenmacher <ron@fnal.gov> on
# Jul 24, 1998. "TERMS AND CONDITIONS" governing this file are in the README
# or COPYING file. If you do not have such a file, one can be obtained by
# contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
# $RCSfile$
# $Revision$
# $Date$

WARN=  `case \`uname\` in Linux) echo "-Wall";;IRIX*) echo "-fullwarn";;esac`
CC=`grep '^CC=' $(PYTHON_DIR)/lib/python*/config/Makefile | sed -e 's/CC=[       ]*//'`
CFLAGS=-D`uname` -g

all:		traceShow lib trace_delta

lib:		trace.a

trace_delta:	trace_delta.c
		@echo $(CC) $(CFLAGS) $(WARN) -o trace_delta trace_delta.c
		@$(CC) $(CFLAGS) $(WARN) -o trace_delta trace_delta.c

trace.a:	trace.o
		ar r trace.a trace.o

trace_test:	trace.o trace_test.o
		@ echo $(CC) $(CFLAGS) -o trace_test trace_test.o trace.o
		@$(CC) $(CFLAGS) -o trace_test trace_test.o trace.o

traceShow:	traceShow.o trace.o
		@echo $(CC) $(CFLAGS) -o traceShow traceShow.o trace.o
		@$(CC) $(CFLAGS) -o traceShow traceShow.o trace.o


trace.o:	trace.c trace.h
		@echo $(CC) $(CFLAGS) $(WARN) -o trace.o -c trace.c
		@$(CC) $(CFLAGS) $(WARN) -o trace.o -c trace.c

trace_test.o:	trace_test.c trace.h
		@echo $(CC) $(CFLAGS) $(WARN) -o trace_test.o -c trace_test.c
		@$(CC) $(CFLAGS) $(WARN) -o trace_test.o -c trace_test.c


traceShow.o:	traceShow.c trace.h
		@echo $(CC) $(CFLAGS) $(WARN) -o traceShow.o -c traceShow.c
		@$(CC) $(CFLAGS) $(WARN) -o traceShow.o -c traceShow.c

clean:
		rm -f *.o *.a traceShow trace_delta

reset:
		@key_file=$$TRACE_KEY.key;\
		if [ `uname` = Linux ];then \
		    ipcrm shm `od -A n      -N 4 -t d4 $$key_file`;\
		    ipcrm sem `od -A n -j 4 -N 4 -t d4 $$key_file`;\
		else \
		    ipcrm -m `expr "\`od -D $$key_file 0\`" : '[^ ]* *\([0-9]*\)'`;\
		    ipcrm -s `expr "\`od -D $$key_file 4\`" : '[^ ]* *\([0-9]*\)'`;\
		fi;\
		rm -f $$key_file



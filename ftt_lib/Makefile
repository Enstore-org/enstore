
OS="`funame -s`+`funame -r | cut -c1`"

LIBS=`cat libs.$$OS`

SYSOBJ= ftt_scsi.o ftt_machdep.o

SCSISRC= ftt_scsi_AIX+2.c ftt_scsi_IRIX+4.c ftt_scsi_IRIX+5.c \
	ftt_scsi_SunOS+5.c ftt_scsi_VMS.c

MACHDEPSRC=ftt_machdep_IRIX+5.c ftt_machdep_AIX+2.c

SUIDSRC= ftt_suid.c 
SUIDOBJ= ftt_suid.o

SRC= ftt_do_scsi.c ftt_error.c ftt_debug.c ftt_global.c ftt_open.c \
	ftt_rdwr.c ftt_tables.c ftt_skip.c ftt_stats.c ftt_info.c \
	ftt_label.c ftt_higher.c ftt_async.c ftt_find.c\
	$(SCSISRC) $(MACHDEPSRC) 

OBJ= $(SYSOBJ) ftt_do_scsi.o ftt_error.o ftt_debug.o ftt_global.o \
	ftt_open.o ftt_rdwr.o ftt_tables.o ftt_skip.o ftt_stats.o ftt_info.o \
	ftt_label.o ftt_higher.o ftt_async.o ftt_find.o

#CFLAGS=-I. -g -D`funame -s` -fullwarn
CFLAGS=-I. -g -D`funame -s` 

LIB=libftt.a

PROGS=ftt_suid asyncdemo

all: $(LIB) $(PROGS)


install: all
	cp libftt.a $(FTT_DIR)/lib
	cp ftt.h ftt_common.h ftt_scsi.h ftt_voidtypes.h $(FTT_DIR)/include

testtables:
	rm -f ftt_error.o
	cc $(CFLAGS) -DTESTTABLES ftt_error.c ftt_global.o -o testtables
	rm -f ftt_error.o

print: 
	a2ps Makefile $(SRC) $(SCSISRC) | flpr

ftt_suid: $(SUIDOBJ) $(LIB)
	OS=$(OS); cc -o $@ $(SUIDOBJ) $(LIB) $(LIBS)

asyncdemo: asyncdemo.c $(LIB)
	OS=$(OS); cc $(CFLAGS) -o $@ asyncdemo.c $(LIB) $(LIBS)

$(LIB): $(OBJ)
	rm -f $(LIB)
	ar qv $(LIB) $(OBJ)

ftt_scsi.o: $(SCSISRC)
	cc $(CFLAGS) -c ftt_scsi_$(OS).c
	mv ftt_scsi_$(OS).o ftt_scsi.o

ftt_machdep.o: $(MACHDEPSRC)
	cc $(CFLAGS) -c ftt_machdep_$(OS).c
	mv ftt_machdep_$(OS).o ftt_machdep.o

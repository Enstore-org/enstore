SHELL=/bin/sh

all: obj.ps other

obj.ps:
	./mk_inherit

other:
	./class4module

clean :
	@ $(ENSTORE_DIR)/bin/enstoreClean
	rm -f [1-9].out *.dot *.ps classes.known

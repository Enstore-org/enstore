SHELL=/bin/sh

all:
	@echo nothing to build

clean:
	for d in test doc ; do (cd $$d; make clean) ; done
	rm -f *~
	rm -f *.pyc
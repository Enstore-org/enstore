SHELL=/bin/sh

dpnfs = `echo "/pnfs/enstore/hppc/test1-\`uname -n\`-\`whoami\`"`

all:
	@echo "nothing to build"

newdbs:
	- killall -9 longtest
	- pidkill $$ENSTORE_DIR/test/longtest_pids
	- for d in 1 2 3 4 5 6 7 8 9 ; do \
		pidkill $$d/loop_longtest.pids ;\
	done

	rm -f $$ENSTORE_DIR/test/files.in
	if [ ! -d data ] ; then mkdir data ; fi
	touch data/null0.dat
	echo short > data/short1.dat
	dd if=/dev/zero of=data/null1.dat count=10
	dd if=/dev/zero of=data/null2.dat count=20
	dd if=/dev/zero of=data/null3.dat count=60
	ecmd random-file --size=1024   --file=data/ran1.dat
	ecmd random-file --size=32768  --file=data/ran2.dat
	ecmd random-file --size=131072 --file=data/ran3.dat

	if [ ! -d $(dpnfs) ] ; then mkdir $(dpnfs); fi

	rm -f $$ENSTORE_DIR/test/files.in
	( cd data; \
	for f in * ; do \
		f2=$(dpnfs)/`basename $$f`;\
		ecmd encp $$f $$f2 ; \
		echo $$f2 >> $$ENSTORE_DIR/test/files.in ; \
	done ; )

	for d in 1 2 3 4 5 6 7 8 9 ; do \
		if [ ! -d $$d ] ; then mkdir $$d ; fi; \
		( cd $$d; rm -f *; \
			cp $$ENSTORE_DIR/test/longtest . ; \
			$$ENSTORE_DIR/test/kickoff & ); \
	done

	@ echo "Tests are running. Do a	   killall -9 longtest	  to stop"
	@ echo "   It takes a bit for them to die off"

olddbs:
	- killall -9 longtest
	- pidkill $$ENSTORE_DIR/test/longtest_pids
	- for d in 1 2 3 4 5 6 7 8 9 ; do \
		pidkill $$d/loop_longtest.pids ;\
	done

	rm -f $$ENSTORE_DIR/test/longtest_pids
	for d in 1 2 3 4 5 6 7 8 9 ; do \
		( cd $$d; $$ENSTORE_DIR/test/kickoff & ); \
	done

	@ echo "Tests are running. Do a	   killall -9 longtest	  to stop"
	@ echo "   It takes a bit for them to die off"

datafiles:	jobs
	if [ ! -d data ] ; then mkdir data ; fi
	echo -n "1" > data/d1
	echo -n "12" > data/d2
	echo -n "123" > data/d3
	echo -n "1234" > data/d4
	dd if=/dev/zero of=data/null30k.dat count=60
	ecmd random-file --size=131072  --file=data/ran128k.dat
	ecmd random-file --size=1048576 --file=data/ran1024k.dat
	rm $$ENSTORE_DIR/test/data.in
	find $$ENSTORE_DIR/test/data -type f -print > $$ENSTORE_DIR/test/data.in
jobs:
	for d in 1 2 3 4 5 6 7 8 9 ; do \
		if [ ! -d $$d ] ; then mkdir $$d ; fi; \
		( cd $$d; rm -f *; \
			cp $$ENSTORE_DIR/test/longtest . ;\
			cp $$ENSTORE_DIR/test/reader . ; \
			cp $$ENSTORE_DIR/test/writer . ; ); \
	done

stop:
	touch $$ENSTORE_DIR/test/readstop
	touch $$ENSTORE_DIR/test/writestop
	touch $$ENSTORE_DIR/test/longstop
	find . -name '*.pids' -exec $$ENSTORE_DIR/bin/pidkill {} \;

clean:
	make stop
	@ $$ENSTORE_DIR/bin/enstoreClean
	for d in 1 2 3 4 5 6 7 8 9 data ; do rm -rf $$d ; done
	rm -f files.in
	rm -f readstop writestop longstop


dpnfs = `echo "/pnfs/enstore/test1-\`whoami\`"`

olddbs:
	enstore-stop

	enstore-start

	rm -f $$ENSTORE_DIR/test/longtest_pids
	for d in 1 2 3 4 5 6 7 8 9 ; do \
		( cd $$d; $$ENSTORE_DIR/test/kickoff & ); \
	done

	echo "Tests are running. Do a    killall -9 longtest    to stop"
	echo "   It takes a bit for them to die off"


newdbs:
	enstore-stop

	rm -f *.jou

	enstore-start

	volume-declare

	rm -f $ENSTORE_DIR/test/files.in
	if [ ! -d data ] ; then mkdir data ; fi
	touch data/long0.dat
	echo short > data/long1.dat
	dd if=/dev/zero of=data/long2.dat count=10
	dd if=/dev/zero of=data/long3.dat count=20
	dd if=/dev/zero of=data/long4.dat count=30
	dd if=/dev/zero of=data/long5.dat count=40
	dd if=/dev/zero of=data/long6.dat count=50
	dd if=/dev/zero of=data/long7.dat count=60

	if [ -d $(dpnfs) ] ; then rm -f $(dpnfs)/* ; else mkdir $(dpnfs); fi

	rm -f $ENSTORE_DIR/test/files.in
	( cd data; \
	for f in * ; do \
		f2=$(dpnfs)/`basename $$f`;\
		python $$ENSTORE_DIR/src/encp.py --list $$f $$f2 ; \
		echo $$f2 >> $$ENSTORE_DIR/test/files.in ; \
	done ; )

	rm -f $$ENSTORE_DIR/test/longtest_pids
	for d in 1 2 3 4 5 6 7 8 9 ; do \
		if [ ! -d $$d ] ; then mkdir $$d ; fi; \
		( cd $$d; rm -f *; \
			cp $$ENSTORE_DIR/test/longtest . ; \
			$$ENSTORE_DIR/test/kickoff & ); \
	done

	echo "Tests are running. Do a    killall -9 longtest    to stop"
	echo "   It takes a bit for them to die off"

clean:
	- killall -9 longtest
	@ $(ENSTORE_DIR)/bin/enstoreClean
	for d in 1 2 3 4 5 6 7 8 9 data ; do rm -rf $$d ; done
	rm -f *.jou
	rm -f *.fake
	rm -f files.in
	rm -f longtest_pids

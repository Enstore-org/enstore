
all:
	for d in 1 2 3 4 5 6 7 8 9 data ; do\
		if [ ! -d $$d ] ; then mkdir $$d ; fi\
	done

	touch data/long0.dat
	echo short > data/long1.dat
	dd if=/dev/zero of=data/long2.dat count=10 
	dd if=/dev/zero of=data/long3.dat count=20 
	dd if=/dev/zero of=data/long4.dat count=30 
	dd if=/dev/zero of=data/long5.dat count=40 
	dd if=/dev/zero of=data/long6.dat count=50 
	dd if=/dev/zero of=data/long7.dat count=60

clean:
	@ $(ENSTORE_DIR)/bin/enstoreClean
	for d in 1 2 3 4 5 6 7 8 9 data ; do rm -rf $$d ; done
	rm -f *.jou
	rm -f *.fake
	rm -f files.in

# doc/other/Makefile $Revision$
SHELL = /bin/sh
#
# There are three targets:
#	all:  to make the html files
#	clean: to get rid of extra files (core, *~, and so on)
#	install: copy selected parts to $ENSTORE_DIR
#

.IGNORE:
.SUFFIXES: .py .ps .gif  .dtim  .Wtim .html .obj

.py.ps :
	python $<  > $@.tmp
	mv $@.tmp $@
	if [ ! -z  "$(CONVERT)" ] ; then  $(CONVERT) ps:$@ ../WWW/$*.gif ; fi

SBIN=../../bin

# a .dtim file stands for a .gif _and_ a .ps file rendered into the doc area
# maybe somene better at make knows a more elegant way to handle this.

.obj.dtim:
	$(TGIF) -print -o../ -ps -color $*.obj  
	$(CONVERT) ../$*.ps $@
	touch $*.dtim


PIC_FILES = \
	./Server-card.dtim  \
	./christmas.dtim \
	./enstore.dtim \
	./hardware.dtim \
	./library_manager.dtim \
	./mover_write.dtim  \
	./pnfs.dtim \
	./superficial_overviewobj.dtim \
	./test-stand.dtim 


#
# TARGETS
#
# Policy: if a desired documentation processor is not present, then the
# the makefile shall still run
# Mechanism: in the "all' target, Use "hash" to sense if the processor 
# is present, then define a  make macro to be either the processor or to 
# be "/bin/true" whcih functions as a no-op. 
all:
	$(MAKE) \
	TGIF=`if hash tgif 2>/dev/null ; then echo tgif ; else echo true ; fi`\
	DOT=`if hash dot 2>/dev/null ; then echo dot ; else echo true ; fi`\
        CONVERT=`if hash convert 2>/dev/null ; then echo convert ; else echo true ; fi ` rest

drawings : any_unmount.ps write_badmount.ps write_error.ps write_tapebusy.ps \
write.ps write_drivebusy.ps write_notape.ps read_error.ps

rest: drawings pics ../obj.ps

../obj.ps:
	./mk_inherit
	$(DOT) -Tps obj.dot > ../obj.ps


pics:	$(PIC_FILES)

#
# Move into the distribution directory.
install :
	@mkdir		 $(ENSTORE_INSTALL_DIR)/doc/other
	cp `$(SBIN)/justfiles` $(ENSTORE_INSTALL_DIR)/doc

#
# Scrub down to what would have been had from the source repository 
clean :
	@ $(SBIN)/enstoreClean
	@ rm -rf $(PIC_FILES) obj.dot
	@ rm -f *.pyc
	@ rm -f *.tmp
	@ rm -f *.ps

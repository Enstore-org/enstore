
# ----------------- Things folks may need to change ---------------------

PROD  =ftt#				# Product name for ups		#
DEPEND=					# Dependencies			#
QUALS =#				# +debug, etc. 			#
CHAIN =development#			# chain for addproduct		#
OS    =`funame -s`#			# OS for addproduct		#
CUST  =+`funame -r | sed -e 's|\..*||'`## customization for addproduct	#
FLAVOR=$(OS)$(CUST)$(QUALS)#	 	# Flavor for declares		#
A_HOST=dcdsv0			 	# host to run addproduct on	#


SUBDIRS=ftt_lib ftt_test
all: 
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) install); done

clean: 					 # clean up unneeded files	#
	for d in $(SUBDIRS); do (cd $$d; $(MAKE) $@); done

# ----------------- Things which ought to be standard--------------------

undeclare: ups/Version			 # declare this product		#
	@$(UNDECLARE)

declare: ups/Version 		 	# declare this product		#
	@$(DECLARE)

ups/Version:
	echo $(VERS) > ups/Version

test: clean				 # make sure tests pass cleaned	#
	sh ups/TestScript

tarfile: clean ups/declare.dat .manifest # make tarfile after prereqs	#
	@echo tar cvf $(TARFILE) . ; tar cvf $(TARFILE) .

addproduct: tarfile			 # addproduct needs a tarfile	#
	@$(ADDPRODUCT)
	rm $(TARFILE)

delproduct: 				 # clean product back out 	#
	@$(DELPRODUCT)

complete:				 # the whole 9 yards		#
	set +e					;\
	. /usr/local/etc/setups.sh		;\
	PRODUCTS="$(DPRODUCTS) $$PRODUCTS" 	;\
	export PRODUCTS				;\
	make FLAVOR=$(FLAVOR) undeclare > /dev/null 2>&1 || true ;\
	make FLAVOR=$(FLAVOR) declare		;\
	setup -b -f $(FLAVOR) $(PROD) $(VERS)||true	;\
	make all test addproduct

partial:				 # just build 'n test	#
	set +e					;\
	. /usr/local/etc/setups.sh		;\
	PRODUCTS="$(DPRODUCTS) $$PRODUCTS" 	;\
	export PRODUCTS				;\
	make FLAVOR=$(FLAVOR) undeclare  > /dev/null 2>&1 || true ;\
	make FLAVOR=$(FLAVOR) declare 		;\
	setup -b -f $(FLAVOR) $(PROD) $(VERS)||true	;\
	make all test 

# -----------------------    support targets ------------------------

ups/declare.dat: FORCE
	$(UPS_DIR)/bin/ups_list -l -f $(FLAVOR) $(PROD) $(VERSION) > $@

# --------------------   manifest file support  ---------------------

MANIFEST = find . -type f ! -name .manifest -print | xargs sum -r | sort +2

.manifest: FORCE
	$(MANIFEST) > $@

check_manifest:
	$(MANIFEST) > /tmp/check$$$$ 	;\
	diff /tmp/check$$$$ .manifest	;\
	rm /tmp/check$$$$

FORCE:

# ---------------------- Ugly Definitions ---------------------------

DIR    =`pwd | sed -e  's|/tmp_mnt||'`#	 # Declare directory for ups -- here
VERS   =`pwd | sed -e 's|.*/||'`# 	 # version is our last sub-directory
TARFILE="$(DIR)/../$(PROD)$(VERS)$(FLAVOR).tar"## tarfile name to use

# declare/undeclare command stuff

UNDECLARE_CMD= \
	$(UPS_DIR)/bin/ups_undeclare \
		-z $(DPRODUCTS) \
		-f $(FLAVOR) \
		$(PROD) $(VERS) 

DECLARE_CMD= \
	$(UPS_DIR)/bin/ups_declare \
		$(DEPEND) \
		-r $(DIR) \
		-f $(FLAVOR) \
		$(PROD) $(VERS)

ADDPRODUCT_CMD = \
    rsh $(A_HOST) /bin/sh -c "'\
	. /usr/local/etc/setpath.sh ; \
	. /usr/local/etc/setups.sh ; \
	cmd addproduct \
		-t $(TARFILE) \
		-o $(OS) \
		-c $(CUST)$(QUALS) \
		-m $(CUST)$(QUALS) \
		-p $(PROD)  \
		-C $(CHAIN) \
		-v $(VERS) \
		-y '"

DELPRODUCT_CMD = \
    rsh $(A_HOST) /bin/sh -c "'\
	. /usr/local/etc/setpath.sh ; \
	. /usr/local/etc/setups.sh ; \
	cmd delproduct \
		-o $(OS) \
		-c $(CUST)$(QUALS) \
		-m $(CUST)$(QUALS) \
		-p $(PROD)  \
		-v $(VERS) \
		-y '"

#
# we echo them and then do them so folks see backquoted stuff (above)
# expanded.
#
UNDECLARE  = echo $(UNDECLARE_CMD); $(UNDECLARE_CMD)
DECLARE    = echo $(DECLARE_CMD);   $(DECLARE_CMD)
ADDPRODUCT = echo $(ADDPRODUCT_CMD); $(ADDPRODUCT_CMD)
DELPRODUCT = echo $(DELPRODUCT_CMD); $(DELPRODUCT_CMD)


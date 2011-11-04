#!/usr/bin/env python
###############################################################################
#
# This script is used to check if postresql db is up
#
#
# $Id$
###############################################################################

import os
import sys
import time

#Maximum number of retries if the checks fail
MAX_RETRY = 20
#Seconds
TIME_TO_RETRY = 5
#Postgres command
PSQL_COMMAND = "psql -U enstore postgres -c 'select datname from pg_database' 2> /dev/null"


def checkPostgres():
	psqlFile = os.popen(PSQL_COMMAND)
	psqlOutput = psqlFile.readlines()
	psqlFile.close()
	if not psqlOutput:
		return False
	return True



def checkAndRetry():
	retry = True
	tries = 0
	while retry:
		if checkPostgres():
			retry = False
		tries = tries + 1
		if tries >= MAX_RETRY:
			break
		if retry:
			time.sleep(TIME_TO_RETRY)
	return not retry

val = checkAndRetry()
if val:
	sys.exit(0)
sys.exit(1)



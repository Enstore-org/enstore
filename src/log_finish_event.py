#!/usr/bin/env python

import sys
import accounting_client
import option

if __name__ == '__main__':

	if len(sys.argv) < 3:
		# quit with error yet quiet
		sys.exit(1)

	intf = option.Interface()
	acc = accounting_client.accClient((intf.config_host, intf.config_port))
	if len(sys.argv) > 3:
		acc.log_finish_event(sys.argv[1], int(sys.argv[2]), sys.argv[3])
	else:
		acc.log_finish_event(sys.argv[1], int(sys.argv[2]))

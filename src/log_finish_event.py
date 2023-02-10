#!/usr/bin/env python

import sys
import accounting_client
import enstore_functions2

if __name__ == "__main__":   # pragma: no cover

	if len(sys.argv) < 3:
		# quit with error yet quiet
		sys.exit(1)

	acc = accounting_client.accClient((enstore_functions2.default_host(),
					   enstore_functions2.default_port()))
	if len(sys.argv) > 3:
		acc.log_finish_event(sys.argv[1], int(sys.argv[2]), sys.argv[3])
	else:
		acc.log_finish_event(sys.argv[1], int(sys.argv[2]))

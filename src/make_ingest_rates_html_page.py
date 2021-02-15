#!/usr/bin/env python
# $Id$
import enstore_files
import enstore_functions
import os

html_dir = enstore_functions.get_html_dir()
filepath = os.path.join(html_dir, "all_sg_burn_rates.html")
html_file = enstore_files.EnstoreIngestRatesFile(filepath)
html_file.open()
html_file.write()
html_file.close()
html_file.install()

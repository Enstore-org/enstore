#!/usr/bin/env python
# $Id$
import enstore_files
import enstore_functions
import os

html_dir = enstore_functions.get_html_dir()
filepath=os.path.join(html_dir,"all_sg_burn_rates.html")
file=enstore_files.EnstoreIngestRatesFile(filepath)
file.open()
file.write()
file.close()
file.install()

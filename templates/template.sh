#!/bin/sh
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi
set -u  # force better programming and ability to use check for not set

# bin/template.sh  $Revision$
# Description...

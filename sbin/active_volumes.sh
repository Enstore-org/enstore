#!/bin/sh
# generate the list of active volumes per library manager
if [ "${1:-}" = "-x" ] ; then set -xv; shift; fi

# this is cheap argument processing. Must be in this order when checking. I want it cheap!
if [ "${1:-}" = "-v" -o "${1:-}" = "--verbose" ] ; then VERBOSE=1; shift; else VERBOSE=0; fi

if [ -r /usr/local/etc/setups.sh ]; then
    set +u
    . /usr/local/etc/setups.sh
    set -u
else
   echo `date` ERROR: setups.sh not found
   exit 1
fi

rm -f /tmp/enstore_libs.0
enstore conf --list-lib | awk '{print $1}' | grep -v null | \
grep -v disk | grep "\." > /tmp/enstore_libs.0

echo "Content-type: text/html

<head>
<title> Active Volumes List </title>
</head>
<body bgcolor='#ffffd0'>
<font size=5 color='blue'>Active Volumes `date`</font>
<pre>
"
for lib in $(cat /tmp/enstore_libs.0)
do
    rm -f /tmp/etl.1
    echo $lib
    enstore lib --vols $lib | tee /tmp/etl.1
    echo
    echo
done

echo "
</pre>
<hr>
<hr><a href='http://www.fnal.gov/pub/disclaim.html'>Legal Notices</a><hr>
</body>
</html>"
rm -f /tmp/etl.1



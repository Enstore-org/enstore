

set path=(`dropit -d' ' /enstore`)
setenv PYTHONPATH `dropit -d':' -p"$PYTHONPATH" /enstore`

unalias encp
unalias pnfs
set path=(`dropit -d' ' /enstore`)
setenv PYTHONPATH `dropit -d':' -p"$PYTHONPATH" /enstore`

unsetenv PYTHONLIB
unsetenv PYTHONINC
unsetenv PYTHONMOD

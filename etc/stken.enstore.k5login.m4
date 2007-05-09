enstore/cd/ccfsrv2.fnal.gov@FNAL.GOV
enstore/cd/stkensrv3.fnal.gov@FNAL.GOV
ifelse(eval(index(HOSTNAME, `stkendca') == 0 || index(HOSTNAME, `fndca') == 0), 1,
enstore/cd/fndca2a.fnal.gov@FNAL.GOV
enstore/cd/fndca3a.fnal.gov@FNAL.GOV
enstore/cd/stkendca2a.fnal.gov@FNAL.GOV
enstore/cd/stkendca3a.fnal.gov@FNAL.GOV,
enstore/cd/cmspnfs1.fnal.gov@FNAL.GOV
enstore/cd/cmspnfs2.fnal.gov@FNAL.GOV
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1,
enstore/cd/cmsdcdr1.fnal.gov@FNAL.GOV
enstore/cd/cmsdcdr2.fnal.gov@FNAL.GOV
enstore/cd/cmssrv28.fnal.gov@FNAL.GOV
enstore/cd/cmsdcmon1.fnal.gov@FNAL.GOV
enstore/cd/stkendca3a.fnal.gov@FNAL.GOV,
enstore/cd/eagpnfs1.fnal.gov@FNAL.GOV
enstore/cd/stkensrv0.fnal.gov@FNAL.GOV
enstore/cd/stkensrv1.fnal.gov@FNAL.GOV
enstore/cd/stkensrv2.fnal.gov@FNAL.GOV
enstore/cd/stkensrv4.fnal.gov@FNAL.GOV
enstore/cd/stkensrv5.fnal.gov@FNAL.GOV
enstore/cd/stkensrv6.fnal.gov@FNAL.GOV
enstore/cd/stkensrv7.fnal.gov@FNAL.GOV
enstore/cd/stkensrv8.fnal.gov@FNAL.GOV
enstore/cd/stkenscan1.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr5a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr6a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr7a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr8a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr9a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr10a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr11a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr12a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr13a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr14a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr15a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr16a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr17a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr20a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr21a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr22a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr23a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr24a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr25a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr26a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr27a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr28a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr29a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr30a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr31a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr32a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr33a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr34a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr35a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr36a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr40a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr41a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr103a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr106a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr107a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr108a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr109a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr111a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr112a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr113a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr114a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr115a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr117a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr118a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr119a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr120a.fnal.gov@FNAL.GOV
enstore/cd/stkenmvr121a.fnal.gov@FNAL.GOV
enstore/cd/stkendm1a.fnal.gov@FNAL.GOV
enstore/cd/stkendm2a.fnal.gov@FNAL.GOV
enstore/cd/stkendm3a.fnal.gov@FNAL.GOV
enstore/cd/stkendm4a.fnal.gov@FNAL.GOV))
ifelse(HOSTNAME, `stkensrv1',
enstore/cd/fndca3a.fnal.gov@FNAL.GOV, `dnl')
ifelse(HOSTNAME, `stkensrv1',
enstore/cd/stkendca3a.fnal.gov@FNAL.GOV, `dnl')
ifelse(HOSTNAME, `stkensrv2',
enstore/cd/d0ensrv4.fnal.gov@FNAL.GOV, `dnl')
ifelse(HOSTNAME, `stkensrv2',
enstore/cd/cdfensrv4.fnal.gov@FNAL.GOV, `dnl')
aik@FNAL.GOV
baisley@FNAL.GOV
bakken@FNAL.GOV
berg@FNAL.GOV
berman@FNAL.GOV
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `fagan@FNAL.GOV', `dnl')
ifelse(eval(index(HOSTNAME, `stkendca') == 0 || index(HOSTNAME, `fndca') == 0), 1,
`fuhrmann@FNAL.GOV', `dnl')
george@FNAL.GOV
huangch@FNAL.GOV
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `ifisk@FNAL.GOV', `dnl')
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `jlkaiser@FNAL.GOV', `dnl')
jonest@FNAL.GOV
kennedy@FNAL.GOV
kschu@FNAL.GOV
ifelse(eval(index(HOSTNAME, `stkendca3a') == 0 || index(HOSTNAME, `fndca3a') == 0), 1,
`kurt@FNAL.GOV', `dnl')
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `lisa@FNAL.GOV', `dnl')
litvinse@FNAL.GOV
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `merina@FNAL.GOV', `dnl')
mircea@FNAL.GOV
moibenko@FNAL.GOV
oleynik@FNAL.GOV
petravic@FNAL.GOV
podstvkv@FNAL.GOV
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `ptader@FNAL.GOV', `dnl')
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `richt@FNAL.GOV', `dnl')
stan@FNAL.GOV
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `tdh@FNAL.GOV', `dnl')
timur@FNAL.GOV
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `tmesser@FNAL.GOV', `dnl')
ifelse(HOSTNAME, `stkensrv4', `vsergeev@FNAL.GOV', `dnl')
ifelse(eval(index(HOSTNAME, `cmspnfs') == 0), 1, `yujun@FNAL.GOV', `dnl')
zalokar@FNAL.GOV

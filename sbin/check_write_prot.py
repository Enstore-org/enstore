#!/usr/bin/env python
import time
import string
import os
import volume_clerk_client
import option

log_dir = "/diska/enstore-log"

date = do = time.strftime("%Y-%m-%d", time.localtime(time.time()))
data_file = "/tmp/override_ro_mount-%s" % (date,)
log_file = os.path.join(log_dir, string.join(("LOG", date), '-'))
mail_to = "moibenko@fnal.gov"
mail_subject = "Write tab flip required"

intf = option.Interface()
vcc = volume_clerk_client.VolumeClerkClient(
    (intf.config_host, intf.config_port))
cmd = "egrep 'override_ro_mount' %s | egrep 'write protection' | awk '{print $8, $11, $13}'  > %s" % (
    log_file, data_file)
os.system(cmd)

df = open(data_file, "r")
ofn = "/tmp/report-%s" % (date,)
of = open(ofn, "w")
lcnt = 0
of.write("VOLUME\tSTATE\t\tWRITE PROTECTION\tOVERRIDE SWITCH\n")
of.write("=================================================================\n")
while True:
    l = df.readline()
    if not l:
        break
    vol, write_prot, override = l.split()
    if override == '1' and write_prot != '1':
        vol_info = vcc.inquire_vol(vol)
        lo = "%s\t%s\t%s\t\t\t%s\n" % (
            vol, vol_info['system_inhibit'][1], "OFF", "ON")
        of.write(lo)
        lcnt = lcnt + 1
df.close()
of.close()
if lcnt != 0:
    cmd = '/usr/bin/Mail -s "%s" %s < %s' % (mail_subject, mail_to, ofn)
    os.system(cmd)
os.system("rm -f %s %s" % (data_file, ofn))

###############################################################################
#
# $Id$
#
###############################################################################
#!/bin/sh
rm -f  $ENSTORE_DIR/bin/encp;
ln -s $ENSTORE_DIR/src/encp.py $ENSTORE_DIR/bin/encp;
rm -f $ENSTORE_DIR/bin/enstore;
ln -s $ENSTORE_DIR/src/enstore_admin.py $ENSTORE_DIR/bin/enstore; 
rm -f $ENSTORE_DIR/bin/enmv;
ln -s $ENSTORE_DIR/src/enmv.py $ENSTORE_DIR/bin/enmv; 
rm -f $ENSTORE_DIR/sbin/enstoreCut;
ln -s $ENSTORE_DIR/sbin/encpCut $ENSTORE_DIR/sbin/enstoreCut; 
rm -f $ENSTORE_DIR/sbin/enstore_up_down;
ln -s $ENSTORE_DIR/src/enstore_up_down.py $ENSTORE_DIR/sbin/enstore_up_down; 
rm -f $ENSTORE_DIR/sbin/enmonitor; 
ln -s $ENSTORE_DIR/src/monitor_client.py  $ENSTORE_DIR/sbin/enmonitor; 
rm -f $ENSTORE_DIR/bin/pnfs
ln -s $ENSTORE_DIR/src/pnfs.py $ENSTORE_DIR/bin/pnfs; 
rm $ENSTORE_DIR/bin/entv 
ln -s $ENSTORE_DIR/src/entv.py $ENSTORE_DIR/bin/entv; 
rm -f $ENSTORE_DIR/bin/volume_assert 
ln -s $ENSTORE_DIR/src/volume_assert.py   $ENSTORE_DIR/bin/volume_assert; 
rm -f $ENSTORE_DIR/bin/ensync 
ln -s $ENSTORE_DIR/src/ensync.py   $ENSTORE_DIR/bin/ensync; 
rm -f $ENSTORE_DIR/bin/get;
ln -s $ENSTORE_DIR/src/get.py   $ENSTORE_DIR/bin/get; 
rm -f $ENSTORE_DIR/sbin/quickquota
ln -s $ENSTORE_DIR/src/quickquota.py   $ENSTORE_DIR/sbin/quickquota; 
rm -f $ENSTORE_DIR/bin/migrate
ln -s $ENSTORE_DIR/src/gmigrate.py   $ENSTORE_DIR/bin/migrate; 
rm -f $ENSTORE_DIR/bin/duplicate
ln -s $ENSTORE_DIR/src/duplicate.py   $ENSTORE_DIR/bin/duplicate; 
###
### Enstore server links.
###
rm -f $ENSTORE_DIR/sbin/configuration_server 
ln -s $ENSTORE_DIR/src/configuration_server.py $ENSTORE_DIR/sbin/configuration_server; 
rm $ENSTORE_DIR/sbin/log_server
ln -s $ENSTORE_DIR/src/log_server.py   $ENSTORE_DIR/sbin/log_server; 
rm -f $ENSTORE_DIR/sbin/alarm_server
ln -s $ENSTORE_DIR/src/alarm_server.py   $ENSTORE_DIR/sbin/alarm_server; 
rm -f $ENSTORE_DIR/sbin/inquisitor
ln -s $ENSTORE_DIR/src/inquisitor.py   $ENSTORE_DIR/sbin/inquisitor; 
rm -f $ENSTORE_DIR/sbin/ratekeeper
ln -s $ENSTORE_DIR/src/ratekeeper.py   $ENSTORE_DIR/sbin/ratekeeper; 
rm $ENSTORE_DIR/sbin/event_relay
ln -s $ENSTORE_DIR/src/event_relay.py   $ENSTORE_DIR/sbin/event_relay; 
rm -f $ENSTORE_DIR/sbin/info_server 
ln -s $ENSTORE_DIR/src/info_server.py   $ENSTORE_DIR/sbin/info_server; 
rm -f $ENSTORE_DIR/sbin/file_clerk
ln -s $ENSTORE_DIR/src/file_clerk.py   $ENSTORE_DIR/sbin/file_clerk; 
rm -f $ENSTORE_DIR/sbin/volume_clerk
ln -s $ENSTORE_DIR/src/volume_clerk.py   $ENSTORE_DIR/sbin/volume_clerk; 
rm -f $ENSTORE_DIR/sbin/accounting_server
ln -s $ENSTORE_DIR/src/accounting_server.py   $ENSTORE_DIR/sbin/accounting_server; 
rm -f $ENSTORE_DIR/sbin/drivestat_server 
ln -s $ENSTORE_DIR/src/drivestat_server.py   $ENSTORE_DIR/sbin/drivestat_server; 
rm -f $ENSTORE_DIR/sbin/library_manager
ln -s $ENSTORE_DIR/src/library_manager.py   $ENSTORE_DIR/sbin/library_manager; 
rm -f $ENSTORE_DIR/sbin/media_changer 
ln -s $ENSTORE_DIR/src/media_changer.py   $ENSTORE_DIR/sbin/media_changer; 
rm -f $ENSTORE_DIR/sbin/mover
ln -s $ENSTORE_DIR/src/mover.py   $ENSTORE_DIR/sbin/mover; 
rm -f $ENSTORE_DIR/sbin/monitor_server 
ln -s $ENSTORE_DIR/src/monitor_server.py   $ENSTORE_DIR/sbin/monitor_server; 

      host		filesystem		Threshold	Action
#---   ----		----------		----------	----------------------
CC     hppc             ./enserver_alive~config       =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)
CC     hppc             ./enserver_alive~log_server   =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)
CC     hppc             ./enserver_alive~volume_clerk =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)
CC     hppc             ./enserver_alive~file_clerk   =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)
CC     hppc             ./enserver_alive~hppcdisk.library_manager =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)
CC     hppc             ./enserver_alive~hppcdisk.media_changer =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)
CC     hppc             ./enserver_alive~hppcdisk.move =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)
CC     hppc             ./enserver_alive~inquisitor   =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)
CC     hppc             ./enserver_alive~admin_clerk  =1        envars(),mail(berman@fnal.gov,NO_ENSERVER,10s)
                                                      =2        envars(),write($ensstartfail),mail(berman@fnal.gov,START_FAILED,10s)

P envars <<EOF
$ensname = "$cmdline";
$ensname =~ s/\s*\S+\s+(\S+)/$1/;
$enstime = "$year-$month-$day $hour:$min";
$ensstartfail = "Problem re-starting $ensname on $host."
EOF

M NO_ENSERVER<<EOF
Enstore $ensname was not running on $host at $enstime
and was restarted.
EOF

M START_FAILED<<EOF
ecmd start failed for $ensname on $host at $enstime.
EOF

#-----------------------------------------------------------
#    WWW - Interface
#-----------------------------------------------------------


P www! <<EOF


#  give all managers
@managers=("Enstore");

#  assign hosts to managers
@hosts=("hppc"
	);

#  specify URL for all status images
@status_images=("patrol_0.gif",
                "patrol_1.gif",
                "patrol_2.gif",
                "patrol_3.gif",
                "patrol_4.gif"
	);

#  assign messages to status images
@status_messages=("No problems detected, but history is available.",
                  "No problems detected.",
                  "Problems detected.",
                  "Problems detected. Check the ressources !",
                  "Bad problems detected. Host status is critical !"
	);

#  define spaces of time in hours of history window
@history_block=(0,3,6,24);

#  define interval in minutes of automatic update
$update_period=7;
 
#  define ping command for optional RCP job
$command_ping="/usr/etc/ping -c 1 -i 20";
 
EOF

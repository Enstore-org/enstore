       host		filesystem		Threshold	Action
#---   ----		----------		----------	----------------------
O      mailfrom         '"Patrol" <patrol@hppc.fnal.gov>'
CC     hppc             ./enalarm                     =1        envars(),write("$enalarms"),mail(berman@fnal.gov,ALARMS,10s)

P envars <<EOF
$ensname = "$cmdline";
$ensname =~ s/\s*\S+\s+(\S+)/$1/;
$enstime = "$year-$month-$day $hour:$min";
$patrol_file = "`./enget_patrol_file`";
$group = "Enstore";
$severity = "2";
$enalarms = `cat $patrol_file`;
EOF

M ALARMS<<EOF
$enalarms
EOF

#-----------------------------------------------------------
#    WWW - Interface
#-----------------------------------------------------------


P www! <<EOF


#  give all managers
@managers=("Enstore");

#  assign hosts to managers
@hosts=("hppc "
	);

#  specify URL for all status images
#@status_images=("patrol_0.gif",
#                "patrol_1.gif",
#                "patrol_2.gif",
#                "patrol_3.gif",
#                "patrol_4.gif"
@status_images=("patrol_4.gif",
                "patrol_4.gif",
                "patrol_3.gif",
                "patrol_2.gif",
                "patrol_1.gif",
                "patrol_0.gif"
	);

#  assign messages to status images
#@status_messages=("No problems detected, but history is available.",
#                  "No problems detected.",
#                  "Problems detected.",
#                  "Problems detected. Check the resources !",
#                  "Bad problems detected. Host status is critical !"
@status_messages=("Bad problems detected. Host status is critical !",
                  "Bad problems detected. Host status is critical !",
                  "Problems detected. Check the resources !",
                  "Problems detected.",
                  "No problems detected.",
                  "No problems detected, but history is available."
	);

#  define spaces of time in hours of history window
@history_block=(0,3,6,24);

#  define interval in minutes of automatic update
$update_period=7;
 
#  define ping command for optional RCP job
$command_ping="/usr/etc/ping -c 1 -i 20";
 
EOF

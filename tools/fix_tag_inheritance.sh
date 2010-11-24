#! /bin/bash

# $Id$

#Script to retroactively add a full tag chain to an already existing PNFS
# directory tree.
#
# fix_tag_inheritance.sh [-s] <tag_name> <owner_name> <owner_group> [directory]
#
# If the -s switch is present, then the script will set all previously existing
# owner names and groups.  By default it only sets the owner and group of
# tags that are created.
#
#It can be used in two ways:
# 1)
#   fix_tag_inheritance.sh cacheClass enstore enstore /pnfs/mist/mist2b/a/
# or 
# 2)
#   find /pnfs/mist/mist2b/a/ -xdev -mount -mindepth 1 -type d > /tmp/test_tag_paths
#   echo -n "default" > /pnfs/mist/mist2b/a/.(tag)(cacheClass)
#   cat /tmp/test_tag_paths | fix_tag_inheritance.sh cacheClass enstore enstore
#where /pnfs/mist/mist2b/a/ is a sample starting directory.  For option #1,
# the script recursively fixes underlying directories.  For option #2,
# the "top" level directory needs to be created first.
#
# This script can be be run multiple times over the same input.
# This script must be run on the machine that PNFS is running on.
# This script does not depend upon any enstore command.

debug=0

##
## Start of functions.
##

print_help()
{
  echo "Usage: `basename $0` [-s] <new_tag_name> <new_owner> <new_group> [target_dir]"
  echo "       `basename $0` -h"
  echo
  echo "       -h   prints this help message"
  echo "       -s   modify user and group ownersip of existing tags too"
  echo
  echo "       If target_dir is specified, then all directories recursively"
  echo "       are checked.  If target_dir is not specified, then the list"
  echo "       of directories is read from standard in without recursion."
  echo
  echo "       The new_owner value is a UID or username that should be set"
  echo "       as the new owner of any created tags.  The new_group value is"
  echo "       the GID or group name that should be set for any new tags."
  echo "       It is acceptable to set these to empty strings, in this case"
  echo "       the values are left as root (0)."
}

#If standard error is closed, try sending it somewhere the user might still
# see it.
write_error()
{
  echo $* 1>&2
  if [ $? -ne 0 ]; then
    echo $* > /dev/tty
  fi
}

#Get the parent id.
# Echo the parent PNFS ID to stdout and return 0, if found.  Otherwise,
# return 1.
get_parent_id()
{
  id=$1
  dir=$2

  parent_id=`cat "$dir/.(parent)($id)"`
  if [ $? -ne 0 ]; then
    message="Unable to find parent id of $id."
    write_error $message
    return 1
  fi

  echo $parent_id
  return 0
}

#Get the first tag id for the supplied directory.
# Echo the PNFS ID of the first tag belonging to the directory to stdout
# and return 0, if found.  Otherwise, return 1.
get_first_tag()
{
  dir=$1  #Name of the directory to return the first tag PNFS ID.
  parent_dir=`dirname "$dir"`
  fname=`basename "$dir"`
  dir_id=`cat "$parent_dir/.(id)($fname)"`  
  if [ $? -ne 0 ]; then
     message="Unable to find PNFS ID: $dir"
     write_error $message
     return 1
  fi
  dir_showid=`cat "$dir/.(showid)($dir_id)"`
  if [ $? -ne 0 ]; then
     message="Unable to find $dir/.(showid)($dir_id)"
     write_error $message
     return 1
  fi
  first_tag_id=`echo "$dir_showid" | awk '$1 == "Tag" && $2 == ":" {print $3}'`
  echo "$first_tag_id"
  return 0
}

#Find that pnfsid of the target tag name.
# Echo the PNFS ID of the tag to stdout and return 0, if found.  Otherwise,
# return 1.
find_tag_id()
{
  next_tag_id=$1  #First tag PNFS ID in the directory chain.
  target_tag_name=$2  #Name of the target tag.
  dir=$3
  
  if [ -z "$next_tag_id" ]; then
    message="No first tag id."
    write_error $message
    return 1
  fi
  if [ -z "$target_tag_name" ]; then
    message="No tag name."
    write_error $message
    return 1
  fi

  current_tag_name="dummy123"
  while [ "$current_tag_name" != "$target_tag_name" \
          -a "$next_tag_id" != "000000000000000000000000" ]; do
    tag_showid=`cat "$dir/.(showid)($next_tag_id)"`
    if [ $? -ne 0 ]; then
      message="Failed to obtain showid information: $dir/.(showid)($next_tag_id)"
      write_error $message
      return 1
    fi
    current_tag_name=`echo "$tag_showid" | awk '$1 == "Name" && $2 == ":" {print $3}'`
    current_tag_id=`echo "$tag_showid" | awk '$1 == "ID" && $2 == ":" {print $3}'`
    next_tag_id=`echo "$tag_showid" | awk '$1 == "next" && $2 == "ID" && $3 == ":" {print $4}'`
  done

  if [ "$current_tag_name" = "$target_tag_name" ]; then
    echo $current_tag_id
    return 0
  else
    return 1
  fi
}


verify_tag()
{
  first_tag_id=$1  #Return value from get_first_tag().
  tag_name=$2  #Tag name from the command line.
  tag_owner=$3 #Tag owner from the command line.
  tag_group=$4 #Tag group from the command line.
  dir=$5  #Current directory being processed.

  #Determine if the directory has a target tag.
  dir_tag_id=`find_tag_id "$first_tag_id" "$tag_name" "$dir"`
  if [ $? -ne 0 ]; then
    return 1
  fi
    
  if [ -n "$set_owner" -a -n "$dir_tag_id" ]; then
    #Set the default owner of the tag.
    correct_tag_owner $tag_name "$tag_owner" "$tag_group" "$dir"
    if [ $? -ne 0 ]; then
      #Use correct_tag_owner()'s error output.
      return 1
    fi
  fi

  echo $dir_tag_id
  return 0
}

create_tag()
{
    first_tag_id=$1  #Return value from get_first_tag().
    tag_name=$2  #Tag name from the command line.
    tag_owner=$3 #Tag owner from the command line.
    tag_group=$4 #Tag group from the command line.
    dir=$5  #Current directory being processed.

    # -n suppresses trailing newline
    echo -n "default" > "$dir/.(tag)($tag_name)"
    if [ $? -ne 0 ]; then
      message="Did not make: $dir/.(tag)($tag_name)"
      write_error $message
      return 1
    fi

    count=1
    while [ "$first_tag_id" = "000000000000000000000000" -a $count -lt $LOOPS ]; do
      #We just inserted the first tag for this directory.  Need to re-grab
      # the top id.
      first_tag_id=`get_first_tag "$dir"`
      if [ $? -ne 0 ]; then

        message="Did not find any tags: $dir"
	write_error $message
        return 1
      elif [ "$first_tag_id" = "000000000000000000000000" ]; then
        #Need to wait for PNFS and the local file cache to give the new
        # correct value.
        message="Retry count at $count"
	write_error $message
        count=`expr $count + 1`
	sleep 1
      fi
    done

    count=1
    rc=-1
    while [ $rc -ne 0 -a $count -lt $LOOPS ]; do
      #Get the tag id now that the tag exists.
      dir_tag_id=`find_tag_id "$first_tag_id" "$tag_name" "$dir"`
      rc=$?
      if [ $rc -ne 0 ]; then
        #Need to wait for PNFS and the local file cache to give the new
        # correct value.
        message="Retry count at $count"
	write_error $message
        count=`expr $count + 1`
	sleep 1
      fi
    done
    if [ $rc -ne 0 ]; then
        message="Did not find $dir/.(tag)($tag_name) id."
	write_error $message
        return 1
    fi

    #Set the default owner of the tag.
    correct_tag_owner $tag_name "$tag_owner" "$tag_group" "$dir"
    if [ $? -ne 0 ]; then
      #Use correct_tag_owner()'s error output.
      return 1
    fi

    echo $dir_tag_id
    return 0
}

#Check if the tag already exists; if not create it.
# Echo the PNFS ID of the tag to stdout and return 0, if found.  Otherwise,
# return 1.
#WARNING: Do not use this function inside of make_or_repair_tag().
verify_or_create_tag()
{
  first_tag_id=$1  #Return value from get_first_tag().
  tag_name=$2  #Tag name from the command line.
  tag_owner=$3 #New tag owner from command line.
  tag_group=$4 #New tag group from command line.
  dir=$5  #Current directory being processed.

  dir_tag_id=`verify_tag $first_tag_id $tag_name "$tag_owner" "$tag_group" "$dir"`
  if [ $? -ne 0 ]; then
    dir_tag_id=`create_tag $first_tag_id $tag_name "$tag_owner" "$tag_group" "$dir"`
    if [ $? -ne 0 ]; then
        message="Did not find $dir/.(tag)($tag_name) id."
	write_error $message
        return 1
    else
      echo $dir_tag_id
      echo "Made $dir_tag_id $tag_name tag in $dir directory."
      return 0
    fi
  else
    echo $dir_tag_id
    echo "Found $dir_tag_id $tag_name tag in $dir directory."
    return 0
  fi
}

#Set the tag file's owner and group.
correct_tag_owner()
{
  tag_name=$1   #Tag name from the command line.
  tag_owner=$2  #New tag owner from command line.
  tag_group=$3  #New tag group from command line.
  dir=$4  #Current directory being processed.

  if [ -n "$tag_group" ]; then
    #Attempt to set the file's group.  Running as root, this should never
    # fail.  If this is not set then leave the default alone.
    count=1
    rc=-1
    while [ $rc -ne 0 -a $count -lt $LOOPS ]; do
      chgrp $3 "$dir/.(tag)($tag_name)"
      rc=$?
      if [ $rc -ne 0 ]; then
        #Need to wait for PNFS and the local file cache to give the new
        # correct value.
        message="Retry count at $count"
	write_error $message
        count=`expr $count + 1`
        sleep 1
      fi
    done
    if [ $rc -ne 0 ]; then
      write_error "Did not change tag group."
      return 1
    fi
  fi

  if [ -n "$tag_owner" ]; then
    #Attempt to set the file's owner.  Running as root, this should never fail.
    count=1
    rc=-1
    while [ $rc -ne 0 -a $count -lt $LOOPS ]; do
      chown $2 "$dir/.(tag)($tag_name)"
      rc=$?
      if [ $rc -ne 0 ]; then
        #Need to wait for PNFS and the local file cache to give the new
        # correct value.
        message="Retry count at $count"
	write_error $message
        count=`expr $count + 1`
        sleep 1
      fi
    done
    if [ $rc -ne 0 ]; then
      write_error "Did not change tag owner."
      return 1
    fi
  fi

  return 0
}

#Check if the tag already exists; if not create it.  Then make the parent
# directory's tag of the same name the parent of this directory's tag.
# Return 0 on success, 1 on failure.
make_or_repair_tag()
{
  tag_name=$1   #Tag name from the command line.
  tag_owner=$2  #New tag owner from command line.
  tag_group=$3  #New tag group from command line.
  dir=$4  #Current directory being processed.

  parent_dir=`dirname "$dir"`

  #Veify that we did not just cross into another PNFS database.
  dir_database=`cat "$dir/.(get)(database)"`
  if [ $? -ne 0 ]; then
    message="Unable to obtain $dir/.(get)(database)"
    write_error $message
    return 1
  fi
  parent_dir_database=`cat "$parent_dir/.(get)(database)"`
  if [ $? -ne 0 ]; then
    message="Unable to obtain $parent_dir/.(get)(database)"
    write_error $message
    return 1
  fi
  if [ "$dir_database" != "$parent_dir_database" ]; then
    echo "Found new mountpoint $dir.  Not changing parent ID value."
    #Fork off a new process to walk through the new PNFS database.
    $0 $set_owner $tag_name "$tag_owner" "$tag_group" "$dir" &
    #We need to save the PID.  If the shell doesn't see $! in the code, it
    # doesn't need to save the exit status.
    ps -lf -p $! | grep $! >> $saved_pids
    return 0
  fi

  first_tag_id=`get_first_tag "$dir"`
  parent_first_tag_id=`get_first_tag "$parent_dir"`

  #Check for the parent directory having a target tag first.  It should
  # already exist.  If this fails there is likely some major issue.
  parent_dir_tag_id=`find_tag_id "$parent_first_tag_id" "$tag_name" "$dir"`
  if [ $? -ne 0 ]; then
    message="Did not find parent $tag_name id: $parent_dir/.(tag)($tag_name)"
    write_error $message
    return 1
  fi

  #Determine if the directory has a target tag.  If not, create it.
  dir_tag_id=`verify_tag $first_tag_id $tag_name "$tag_owner" "$tag_group" "$dir"`
  if [ $? -ne 0 ]; then
    dir_tag_id=`create_tag $first_tag_id $tag_name "$tag_owner" "$tag_group" "$dir"`
    if [ $? -ne 0 ]; then
      message="Did not find $dir/.(tag)($tag_name) id."
      write_error $message
      return 1
    fi
  fi

  tag_parent_id=`get_parent_id "$dir_tag_id" "$dir"`
  if [ $? -ne 0 ]; then
    ## A serious error occured.
    message="Can not find parent of $dir_tag_id."
    write_error $message
    return 1
  elif [ "$tag_parent_id" = "000000000000000000000000" ]; then
    ## We need to update the parent id of the tag.
    
    #Give debugging output if needed.
    if [ $debug -ne 0 ]; then
      echo $pnfs/tools/sclient chparent $shmkey $dir_tag_id $parent_dir_tag_id
    fi

    #This is the scary part where we change the parent id value.
    count=1
    rc=-1
    while [ $rc -ne 0 -a $count -lt $LOOPS ]; do
      $pnfs/tools/sclient chparent "$shmkey" "$dir_tag_id" "$parent_dir_tag_id"
      rc=$?
      if [ $rc -ne 0 ]; then
        #Need to wait for PNFS and the local file cache to give the new
        # correct value.
        message="Retry count at $count"
	write_error $message
        count=`expr $count + 1`
        sleep 1
      fi
    done
    if [ $rc -ne 0 ]; then
        message="'$pnfs/tools/sclient chparent $shmkey $dir_tag_id $parent_dir_tag_id' failed"
	write_error $message
    fi

    #This is another scary part where we turn the pseudo primary tag back into
    # an inherited tag.
    count=1
    rc=-1
    while [ $rc -ne 0 -a $count -lt $LOOPS ]; do
      rm "$dir/.(tag)($tag_name)" 2> /dev/null
      rc=$?
      if [ $rc -ne 0 ]; then
        #If the tag is already an inherited tag we will get a error that we can
        # ignore.  We test for this by seeing if we can read the tag back
        # without error.
	cat "$dir/.(tag)($tag_name)" > /dev/null 2>&1
	if [ $? -ne 0 ]; then
	  grep "$tag_name" "$dir/.(tags)()" > /dev/null 2>&1
          if [ $? -ne 0 ]; then
	    message="Can not find $dir/.(tag)($tag_name)"
	    write_error $message
	    return 1
	  else
            #The "0 0 10" are magic values from the PNFS developers.
	    $pnfs/tools/sclient writedata "$shmkey" "$dir_tag_id" 0 0 10
            if [ $? -ne 0 ]; then
              mesage="Can not repair $dir/.(tag)($tag_name)"
	      write_error $message
	      return 1
            fi
	    #Deleting the tag again should restore inheritance.
            #
            #Need to wait for PNFS and the local file cache to give the new
            # correct value.
            message="Retry count at $count"
	    write_error $message
            count=`expr $count + 1`
            sleep 1
          fi
        else
          #We cat-ed the tag file.  It already is inherited.
          rc=0
	fi
      fi
    done
    if [ $rc -ne 0 ]; then
      message="Did not repair $dir/.(tag)($tag_name)"
      write_error $message
      return 1
    fi


    echo "Set $parent_dir_tag_id as parent of $dir_tag_id in $dir."
  elif [ "$tag_parent_id" != "$parent_dir_tag_id" ]; then
    message="Found mismatched parent tag IDs for $dir_tag_id: $tag_parent_id != $parent_dir_tag_id"
    write_error $message
    return 1
  elif [ "$tag_parent_id" = "$parent_dir_tag_id" ]; then
    echo "Determined $parent_dir_tag_id already set as parent of $dir_tag_id in $dir."
  else
    message="Aborting from unknown error."
    write_error $message
    return 1
  fi


  return 0
}

#Take a tag_name and directory and recursively search 
fix_directory_tree()
{
  tag_name=$1  #the target tag name
  if [ -z "$tag_name" ]; then
    message="No tag name specified."
    write_error $message
    return 1
  fi

  tag_owner=$2  #the target tag new owner
  tag_group=$3  #the target tag new group

  #Determine the source of the list of directories to check.
  my_tty=`tty`
  if [ $? -eq 0 -o -n "$2" ]; then
    #Stdin is attached to a terminal.  (Or the user explicitly put a directory
    # on the command line; it is possible for the first clause to be false
    # and the second clause to be true for cron environments.)

    dir=$4  #target directory
    if [ -z "$dir" ]; then
      print_help
      return 1
    fi
    if [ ! -d "$dir" ]; then
       message="Directory not found: $dir"
       write_error $message
       return 1
    fi

    #Look at the top directory supplied by the user.  If it has the requested
    # tag, do nothing.  If not, create it.
    top_tag_id=`get_first_tag "$dir"`
    verify_or_create_tag $top_tag_id "$tag_name" "$tag_owner" "$tag_group" "$dir" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      message="Aborting from failure [2]: $dir"
      write_error $message
      return 1
    fi

    #List all dirs underneath $dir.  This excludes $dir which has just been
    # done. This find includes directories belonging to different PNFS
    # databases, but not those sub-directories.  We count the lines of output
    # in case we find an empty PNFS database and return success anyway;
    # otherwise egrep finds no matches and returns false (1).
    (line_count=`find $dir -mindepth 1 -type d -print \( -exec diff -s "{}/.(get)(database)" "{}/../.(get)(database)" \; -o -prune \) | egrep -v "are identical|differ" | tee "$temp_dir_file" | wc -l`;
     rc=$?;
     if [ $line_count -eq 0 ]; then
       exit 0;
     else 
       exit $rc;
     fi;) &
    #We need to save the PID.  If the shell doesn't see $! in the code, it
    # doesn't need to save the exit status.
    ps -lf -p $! | grep $! >> $saved_pids

    # Re-assign standard in to be this "file."  This avoids the problem with 
    # bash creating a sub-shell using a pipe to read from a file in a loop.
    exec < "$temp_dir_file"
  elif [ $? -eq 1 ]; then
    #Stdin is most likely attached to a pipe.
    #
    #This method assumes that the "top" directory(ies) already have the
    # tag value already set.
  
    temp_dir_file=""  # cat reads from stdin with ""
  else
    #Something really bad happened.
    message="Something really bad happened."
    write_error $message
    return 1
  fi

  #Loop over all sub-directories creating or repairing the target tags as
  # necessary.
  count=0
  while read next_dir; do
    make_or_repair_tag $tag_name "$tag_owner" "$tag_group" "$next_dir"
    rc_temp=$?
#false; rc_temp=$?  #Usefull for debugging.
    if [ $rc_temp -ne 0 ]; then
      message="Aborting from failure [1]: $next_dir"
      write_error $message
      jobs_to_kill=`jobs -p`
      if [ -n "$jobs_to_kill" ]; then
	  kill "$jobs_to_kill"
      fi
      false
      break
    fi

    count=`expr $count + 1`
    true
  done  #ignore error when find closes pipe
  rc=$?

  return $rc 
}

#Send SIGTERM then SIGKILL to every child process.
kill_children()
{
  to_kill=`jobs -p`
  if [ -n "$to_kill" ]; then
    kill $to_kill 2> /dev/null
    sleep 3
    to_kill2=`jobs -p`
    if [ -n "$to_kill2" ]; then
      kill -9 $to_kill2 2> /dev/null  #Make sure of it.
    fi
  fi

  return
}

#Wait for all child processes to finish.
collect_children()
{
  rc=0
  rc2=0

  #Start by grabbing the processes we know we want.
  if [ -r "$saved_pids" ]; then
    #Find out if any child processes in sub-directory databases had an error.
    while read process_description; do
      next_job=`echo $process_description | awk '{print $4}'`
      if [ -z $next_job ]; then
        #Avoid calling wait without an argument.
        continue
      fi
      wait $next_job 2> /dev/null
      temp_rc=$?
      if [ $temp_rc -gt 0 -a $temp_rc -le 126 ]; then
        message="Process \"$process_description\" returned error $temp_rc."
        write_error $message
        rc=`expr $rc + $temp_rc`
      elif [ $temp_rc -eq 127 ]; then
	message="PID $next_job does not exist.  \"$process_description\""
        write_error $message
        jobs -p | grep $next_job > /dev/null 2> /dev/null
        if [ $? -ne 0 ]; then
          #The grep did not find the PID in 'jobs' output and 'wait' failed
          # to find the process to return the exit status.
          #
	  #If we do not get here, the grep did find the PID in the 'jobs'
          # output.  The 'wait' command failed to find it.  This should never
          # happen.  Let's fake success in that case.
	  rc2=$temp_rc
        fi
      elif [ $temp_rc -ge 128 ]; then
        message="Process \"$process_description\" was terminated by signal SIG$(kill -l $temp_rc)."
	write_error $message
        rc2=$temp_rc
      else
        rc=`expr $rc + $temp_rc`
      fi
    done < "$saved_pids"
  fi

  #Grab anything else.  Should never need this.
  for pid in `jobs -p`; do
    wait $pid 2>/dev/null
    temp_rc=$?
    if [ $temp_rc -gt 0 -a $temp_rc -le 126 ]; then
      message="Process $pid exited with $temp_rc."
      write_error $message
      rc=`expr $rc + $temp_rc`
    elif [ $temp_rc -eq 127 ]; then
      message="PID $pid does not exist."
      write_error $message
      #If the PID is reported by 'jobs', but wait does not find it, there
      # is something wrong.  This should never happend.  Let's fake success
      # in this case.
      #rc2=$temp_rc
    elif [ $temp_rc -ge 128 ]; then
      message="Process $pid was terminated by signal SIG$(kill -l $temp_rc)."
      write_error $message
      rc2=$temp_rc
    else
      rc=`expr $rc + $temp_rc`
    fi
  done
  
  if [ $rc2 -ne 0 ]; then
    return $rc2
  fi
  return $rc
}

##
## End of functions.
##

#First some sanity checks.
if [ `id -u` -ne 0 ]; then
  message="Must be root."
  write_error $message
  exit 1
fi
if [ ! -r /usr/etc/pnfsSetup ]; then
  message="Unable to find /usr/etc/pnfsSetup"
  write_error $message
  exit 1
fi
source /usr/etc/pnfsSetup

#Maximum number of times to try commands that access PNFS.  Sometimes it
# gives errors when it should not.
LOOPS=4

#Determine if -s was included on the command line.
set_owner=""
while getopts sh name
do
    case $name in
    s)   set_owner="-s";;
    h)   print_help
         exit 0;;
    ?)   print_help
         exit 1;;
    esac
done
shift $(($OPTIND - 1))

tag_name=${1:-""}
owner_name=${2:-""}
owner_group=${3:-""}
directory=${4:-""}
if [ -z "$tag_name" -o -z "$directory" ]; then
  print_help
  exit 1
fi

#Make sure we cancel this before normal executing ends.  Save the starting
# point, since the single quoted trap command is processed at trap execution.
starting_point=$4
trap 'save_rc=$?;
      kill_children;
      collect_children;
      rm -f "$saved_pids" "$temp_dir_file";
      message="Exiting PID $$ for $starting_point early";
      if [ $save_rc -ge 128 ]; then
        message="$message from signal SIG$(kill -l $save_rc) $save_rc.";
      else
        message="${message}.";
      fi;
      write_error $message;
      trap "" HUP INT QUIT ABRT ALRM TERM PIPE EXIT;
      exit $save_rc;' HUP INT QUIT ABRT ALRM TERM PIPE EXIT

#Store the list of backgrounded process IDs.
saved_pids=/tmp/saved_pids_$$
rm -f "$saved_pids"
touch "$saved_pids"
if [ $? -ne 0 ]; then
  message="Failed to make temporary file: $saved_pids"
  write_error $message
  exit 1
fi

#Create the named pipe that will handle the list of directories created
# in the sub-process to be processed in this process.
temp_dir_file=/tmp/find_all_dirs_$$
mkfifo "$temp_dir_file"
if [ $? -ne 0 ]; then
  message="Failed to make temporary FIFO: $temp_dir_file"
  write_error $message
  exit 1
fi

#Do all the real directory walking here.
fix_directory_tree "$tag_name" "$owner_name" "$owner_group" "$directory"
rc=$?

#Find out if any child processes in sub-directory databases had an error.
number_of_saved_pids=`wc -l "$saved_pids" | awk '{print $1}'`
echo "Done Walking '$tag_name $directory' with $rc and $number_of_saved_pids process(es) to check." 2>/dev/null
collect_children
temp_rc=$?
if [ $temp_rc -lt 127 ]; then
  rc=`expr $rc + $temp_rc`
else
  #Signal received (128+) or the process no longer exists (127). 
  rc=$temp_rc
fi

#Cleanup temporary files.
rm -f "$saved_pids" "$temp_dir_file" 
#Clear the trap.  The processes are done and the temporary files are gone.
trap "" HUP INT QUIT ABRT ALRM TERM PIPE EXIT

#We have a successful update?
echo "Exiting '$tag_name $directory' $$ with $rc." 2> /dev/null
exit $rc

#! /bin/bash

# $Id$

#Script to retroactively add a full tag chain to an already existing PNFS
# directory tree.
#
#It can be used in two ways:
# 1)
#   fix_tag_inheritance.sh cacheClass /pnfs/mist/mist2b/a/
# or 
# 2)
#   find -P /pnfs/mist/mist2b/a/ -xdev -mount -mindepth 1 -type d > /tmp/test_tag_paths
#   echo -n "default" > /pnfs/mist/mist2b/a/.(tag)(cacheClass)
#   cat /tmp/test_tag_paths | fix_tag_inheritance.sh cacheClass
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
  echo Usage: `basename $0` "<new_tag_name> [target dir]"
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
    echo "Unable to find parent id of $id." 1>&2
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
     echo "Unable to find PNFS ID: $dir" 1>&2
     return 1
  fi
  dir_showid=`cat "$dir/.(showid)($dir_id)"`
  if [ $? -ne 0 ]; then
     echo "Unable to find $dir/.(showid)($dir_id)" 1>&2
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
  
  if [ -z "$next_tag_id" ]; then
    echo "No first tag id." 1>&2
    return 1
  fi
  if [ -z "$target_tag_name" ]; then
    echo "No tag name." 1>&2
    return 1
  fi

  current_tag_name="dummy123"
  while [ "$current_tag_name" != "$target_tag_name" \
          -a "$next_tag_id" != "000000000000000000000000" ]; do
    tag_showid=`cat "$dir/.(showid)($next_tag_id)"`
    if [ $? -ne 0 ]; then
      echo "Failed to obtain showid information: $dir/.(showid)($next_tag_id)" 1>&2
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

#Check if the tag already exists; if not create it.
# Echo the PNFS ID of the tag to stdout and return 0, if found.  Otherwise,
# return 1.
verify_or_create_tag()
{
  first_tag_id=$1  #Return value from get_first_tag().
  tag_name=$2  #Tag name from the command line.
  dir=$3  #Current directory being processed.

  #Determine if the directory has a target tag.
  dir_tag_id=`find_tag_id "$first_tag_id" "$tag_name"`
  if [ $? -ne 0 ]; then
    # -n suppresses trailing newline
    echo -n "default" > "$dir/.(tag)($tag_name)"
    if [ $? -ne 0 ]; then
      echo "Did not make: $dir/.(tag)($tag_name)" 1>&2
      return 1
    fi

    count=1
    while [ "$first_tag_id" = "000000000000000000000000" -a $count -lt 4 ]; do
      #We just inserted the first tag for this directory.  Need to re-grab
      # the top id.
      first_tag_id=`get_first_tag "$dir"`
      if [ $? -ne 0 ]; then

        echo "Did not find any tags: $dir" 1>&2
        return 1
      elif [ "$first_tag_id" = "000000000000000000000000" ]; then
        #Need to wait for PNFS and the local file cache to give the new
        # correct value.
        echo "Retry count at $count" 1>&2
        count=`expr $count + 1`
	sleep 1
      fi
    done

    count=1
    rc=-1
    while [ $rc -ne 0 -a $count -lt 4 ]; do
      #Get the tag id now that the tag exists.
      dir_tag_id=`find_tag_id "$first_tag_id" "$tag_name"`
      rc=$?
      if [ $rc -ne 0 ]; then
        #Need to wait for PNFS and the local file cache to give the new
        # correct value.
        echo "Retry count at $count" 1>&2
        count=`expr $count + 1`
	sleep 1
      fi
    done
    if [ $rc -ne 0 ]; then
        echo "Did not find $dir/.(tag)($tag_name) id." 1>&2
        return 1
    else
      echo "Made $dir_tag_id $tag_name tag in $dir directory." >> /dev/tty
    fi
  else
    echo "Found $dir_tag_id $tag_name tag in $dir directory." >> /dev/tty
  fi

  echo $dir_tag_id
  return 0
}

#Check if the tag already exists; if not create it.  Then make the parent
# directory's tag of the same name the parent of this directory's tag.
# Return 0 on success, 1 on failure.
make_or_repair_tag()
{
  tag_name=$1
  dir=$2

  parent_dir=`dirname "$dir"`

  first_tag_id=`get_first_tag "$dir"`
  parent_first_tag_id=`get_first_tag "$parent_dir"`

  #Check for the parent directory having a target tag first.  It should
  # already exist.  If this fails there is likely some major issue.
  parent_dir_tag_id=`find_tag_id "$parent_first_tag_id" "$tag_name"`
  if [ $? -ne 0 ]; then
    echo "Did not find parent $tag_name id: $parent_dir/.(tag)($tag_name)" 1>&2
    return 1
  fi

  #Determine if the directory has a target tag.  If not, create it.
  dir_tag_id=`verify_or_create_tag "$first_tag_id" "$tag_name" "$dir"`
  if [ $? -ne 0 ]; then
    #Error already echoed.
    return 1
  fi

  #Veify that we did not just cross into another PNFS database.
  dir_database=`cat "$dir/.(get)(database)"`
  if [ $? -ne 0 ]; then
    echo "Unable to obtain $dir/.(get)(database)" 1>&2
    return 1
  fi
  parent_dir_database=`cat "$parent_dir/.(get)(database)"`
  if [ $? -ne 0 ]; then
    echo "Unable to obtain $parent_dir/.(get)(database)" 1>&2
    return 1
  fi
  if [ "$dir_database" != "$parent_dir_database" ]; then
    echo "Found new mountpoint.  Not changing parent value." > /dev/tty
    return 0
  fi

  tag_parent_id=`get_parent_id "$dir_tag_id" "$dir"`
  if [ $? -ne 0 ]; then
    ## A serious error occured.
    echo "Can not find parent of $dir_tag_id." 1>&2
    return 1
  elif [ "$tag_parent_id" = "000000000000000000000000" ]; then
    ## We need to update the parent id of the tag.
    
    #Give debugging output if needed.
    if [ $debug -ne 0 ]; then
      echo $pnfs/tools/sclient chparent $shmkey $dir_tag_id $parent_dir_tag_id
    fi
    echo "Setting $parent_dir_tag_id as parent of $dir_tag_id." > /dev/tty

    #This is the scary part where we change the parent id value.
    $pnfs/tools/sclient chparent "$shmkey" "$dir_tag_id" "$parent_dir_tag_id"
    if [ $? -ne 0 ]; then
      echo "'$pnfs/tools/sclient chparent $shmkey $dir_tag_id $parent_dir_tag_id'" failed 1>&2
      return 1
    fi 

    #This is another scary part where we turn the pseudo primary tag back into
    # an inherited tag.
    rm "$dir/.(tag)($tag_name)" 2> /dev/null
    if [ $? -ne 0 ]; then
      #If the tag is already an inherited tag we will get a error that we can
      # ignore.  We test for this by seeing if we can read the tag back without
      # error.
      cat "$dir/.(tag)($tag_name)" > /dev/null 2>&1
      if [ $? -ne 0 ]; then
        echo "Can not find $dir/.(tag)($tag_name)" 1>&2
        return 1
      fi
    fi
  elif [ "$tag_parent_id" != "$parent_dir_tag_id" ]; then
    echo "Found mismatched parent tag IDs for $dir_tag_id: $tag_parent_id != $parent_dir_tag_id" 1>&2
    return 1
  elif [ "$tag_parent_id" = "$parent_dir_tag_id" ]; then
    echo "$parent_dir_tag_id already set as parent of $dir_tag_id." > /dev/tty
  else
    echo "Unknown error.  Abborting" 1>&2
    return 1
  fi
  return 0
}

##
## End of functions.
##


#First some sanity checks.
if [ `id -u` -ne 0 ]; then
  echo "Must be root." 1>&2
  exit 1
fi
if [ ! -r /usr/etc/pnfsSetup ]; then
  echo "Unable to find /usr/etc/pnfsSetup" 1>&2
  exit 1
fi
source /usr/etc/pnfsSetup

tag_name=$1  #the target tag name
if [ -z "$tag_name" ]; then
  echo "No tag name specified." 1>&2
  exit 1
fi

#Determine the source of the list of directories to check.
my_tty=`tty`
if [ $? -eq 0 -o -n "$2" ]; then
  #Stdin is attached to a terminal.  (Or the user explicitly put a directory
  # on the command line; it is possible for the first clause to be false
  # and the second clause to be true for cron environments.)

  dir=$2  #target directory
  if [ -z "$dir" ]; then
    print_help
    exit 1
  fi
  if [ ! -d "$dir" ]; then
     echo "Directory not found: $dir" 1>&2
     exit 1
  fi

  #Look at the top directory supplied by the user.  If it has the requested
  # tag, do nothing.  If not, create it.
  top_tag_id=`get_first_tag "$dir"`
  verify_or_create_tag $top_tag_id "$tag_name" "$dir" > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "Abborting from failure: $dir" 1>&2
    exit 1
  fi

  temp_dir_file=/tmp/find_all_dirs_$$
  trap "rm -f $temp_dir_file" 0
  #List all dirs underneath $dir.  This excludes $dir which has just been done.
  find -P $dir -xdev -mindepth 1 -type d > "$temp_dir_file"

elif [ $? -eq 1 ]; then
  #Stdin is most likely attached to a pipe.
  #
  #This method assumes that the "top" direcotry(ies) already have the
  # tag value already set.
  
  temp_dir_file=""  # cat reads from stdin with ""
else
  #Something really bad happened.
  echo "Something really bad happened." 1>&2
  exit 1
fi

#Loop over all sub-directories creating or repairing the target tags as
# necessary.
cat $temp_dir_file |
while read next_dir; do
  make_or_repair_tag $tag_name $next_dir
  if [ $? -ne 0 ]; then
    echo "Abborting from failure: $next_dir" 1>&2
    exit 1
  fi
done

#We have a successful update.
exit 0

#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import string

# extract wrapper from volume family
# volume family has the following structure:
# strage_group.file_family.wrapper
def extract_wrapper(volume_family):
    vf = string.split(volume_family,'.')
    if len(vf) == 3: wrapper = vf[2]
    else: wrapper = 'none'
    return wrapper

# extract file_family from volume family
# volume family has the following structure:
# strage_group.file_family.wrapper
def extract_file_family(volume_family):
    vf = string.split(volume_family,'.')
    if len(vf) > 2:
        file_family = vf[1]
        if len(vf) == 3:
            file_family = string.join((file_family, vf[2]), '.')
    else: file_family = 'none'
    return file_family

# extract storage_group from volume family
# volume family has the following structure:
# strage_group.file_family.wrapper
def extract_storage_group(volume_family):
    vf = string.split(volume_family,'.')
    if len(vf) > 0:
        storage_group = vf[0]
    else: storage_group = 'none'
    return storage_group

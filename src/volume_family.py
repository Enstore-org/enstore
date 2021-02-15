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
    vf = string.split(volume_family, '.')
    if len(vf) == 3:
        wrapper = vf[2]
    else:
        wrapper = 'none'
    return wrapper

# extract file_family from volume family
# volume family has the following structure:
# strage_group.file_family.wrapper


def extract_file_family(volume_family):
    vf = string.split(volume_family, '.')
    if len(vf) > 2:
        file_family = vf[1]

        # if len(vf) == 3:
        #    file_family = string.join((file_family, vf[2]), '.')
    else:
        file_family = 'none'
    return file_family

# extract storage_group from volume family
# volume family has the following structure:
# strage_group.file_family.wrapper


def extract_storage_group(volume_family):
    vf = string.split(volume_family, '.')
    if len(vf) > 0:
        storage_group = vf[0]
    else:
        storage_group = 'none'
    return storage_group

# combine volume family


def make_volume_family(storage_group, file_family, wrapper):
    return string.join((storage_group, file_family, wrapper), '.')


# compare 2 volume families
# volume family syntax:
# storage_group.file_family.wrapper
def match_volume_families(a, b):
    a = string.split(a, '.')
    b = string.split(b, '.')
    if len(a) != len(b):
        min_len = min(len(a), len(b))
        a = a[:min_len]
        b = b[:min_len]
    return a == b

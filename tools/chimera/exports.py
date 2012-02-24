#!/usr/bin/env python
##############################################################################
#
# $Id$
#
# script that generates exports file in current directory from file entries
# in /pnfs/fs/admin/etc/export
#
##############################################################################
import os
import socket

PATH="/pnfs/fs/admin/etc/exports"

if __name__ == "__main__":
    our_host_name=socket.gethostname()
    export_file = open("exports","w")
    export_file.write("/ localhost(rw)\n")
    content={}
    trusted={}
    for direntry in os.listdir(os.path.abspath(os.path.join(PATH,"trusted"))):
        try:
            name, dummy, ip= socket.gethostbyaddr(direntry)
            trusted[name]=1
        except:
            continue


    for direntry in os.listdir(os.path.abspath(PATH)):
        path_to_file=os.path.join(PATH,direntry)
        if os.path.isdir(path_to_file): continue
        if os.path.islink(path_to_file): continue
        try:
            name, dummy, ip= socket.gethostbyaddr(direntry)
            f=open(path_to_file,"r")
            for l in f:
                if not l: continue
                parts=l.split()
                mp=parts[0].strip()
                if mp=="/admin" : continue
                if not content.has_key(mp):
                    content[mp] = []
                content[mp].append(name)
        except:
            continue
    sortedkeys = content.keys()[:]
    sortedkeys.sort()
    for key in sortedkeys:
        value=content[key]
        sortedvalues = value[:]
        sortedvalues.sort()
        sortedvalues_set=set(sortedvalues)
        sortedvalues=list(sortedvalues_set)
        sortedvalues.sort()
        if key == "/fs" :
            export_file.write("/pnfs%s"%(key,))
        else:
            export_file.write("/pnfs/fs/usr%s"%(key,))

        hasUs=False
        for v in sortedvalues:
            if v == our_host_name:
                hasUs=True
            if trusted.has_key(v):
                export_file.write(" %s(rw,no_root_squash)"%(v,))
            else:
                export_file.write(" %s(rw)"%(v,))
        if not hasUs:
            export_file.write(" %s(rw,no_root_squash)"%(our_host_name,))
        export_file.write("\n");

    for key in sortedkeys:
        value=content[key]
        sortedvalues = value[:]
        sortedvalues.sort()
        sortedvalues_set=set(sortedvalues)
        sortedvalues=list(sortedvalues_set)
        sortedvalues.sort()
        if key == "/fs" : continue
        export_file.write("%s"%(key,))
        hasUs=False
        for v in sortedvalues:
            if v == our_host_name:
                hasUs=True
            if trusted.has_key(v):
                export_file.write(" %s(rw,no_root_squash)"%(v,))
            else:
                export_file.write(" %s(rw)"%(v,))
        if not hasUs:
            export_file.write(" %s(rw,no_root_squash)"%(our_host_name,))
        export_file.write("\n");

    export_file.close()



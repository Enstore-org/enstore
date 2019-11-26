#!/usr/bin/env python
## Get volumes information from IBM TS4500 library using IBM CLI,
## Get volumes information using enstore media changer, specified in command line.
## Compare received data, save the result.
## Send e-mail if data did not compare.

import sys
import os
import errno
import time
import subprocess
import tempfile

def shell_command(command):
    pipeObj = subprocess.Popen(command,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True,
                               close_fds=True)
    if pipeObj == None:
        return None
    # get stdout and stderr
    result = pipeObj.communicate()
    del(pipeObj)
    return result 

def shell_command2(command):
    pipeObj = subprocess.Popen(command,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True,
                               close_fds=True)
    if pipeObj == None:
        return None
    # get stdout and stderr
    result = pipeObj.communicate()
    rc = [pipeObj.returncode]
    del(pipeObj)
    for r in result:
        rc.append(r)
    return tuple(rc) 

def compare_ibm_enstore(ibm_report, enstore_report):
    rc = shell_command('grep -v Volume %s | sed "s/,//g" | sort -k 1'%(ibm_report,))
    if not rc:
        return
    vols_in_ibm_report = []
    lines = rc[0].split('\n')
    for l in lines:
        if l:
            e = l.split()
            vols_in_ibm_report.append((e[0].strip(), e[2].strip(),e[4].strip().split('(')[0]))
    #rc = shell_command('grep -v volume %s | sort -k 1'%(enstore_report,))
    rc = shell_command('grep -v volume %s | grep -v empty | grep -v busy | sort -k 1'%(enstore_report,))
    if not rc:
        return
    vols_in_enstore_report = []
    lines = rc[0].split('\n')
    for l in lines:
        if l:
            e = l.split()
            vol = e[0].strip()
            addr = e[2].strip()
            if addr == 'drive':
                pos = 'Drive'
                addr = e[3].strip()
            else:
                pos = 'Slot'
            #if  vol in ('empty', 'busy'): 
            #    continue
            vols_in_enstore_report.append((vol, addr, pos))
    cmp_len = min(len(vols_in_enstore_report), len(vols_in_ibm_report))
    i = 0
    result_file = enstore_report.replace('media_changer-vol-list', 'comparison')
    did_not_compare = []
    not_found_in_ibm_report = []
    for_the_rest = None
    pos_changed = []
    rc = (True, None)
    for i in range(cmp_len):
        # Find volume in vols_in_ibm_report
        found_in_ibm_report = False
        for j in range(cmp_len):
            if vols_in_enstore_report[i][0] == vols_in_ibm_report[j][0]:
                found_in_ibm_report = True
                break
        else:
            not_found_in_ibm_report.append(vols_in_enstore_report[i])
        if found_in_ibm_report and vols_in_enstore_report[i] != vols_in_ibm_report[j]:
            if ((vols_in_enstore_report[i][0] == vols_in_ibm_report[j][0]) and 
                (vols_in_enstore_report[i][2] != vols_in_ibm_report[j][2])):
                pos_changed.append((vols_in_ibm_report[i], vols_in_enstore_report[i]))
            else:
                did_not_compare.append((vols_in_ibm_report[i], vols_in_enstore_report[i]))
    #print('POS CHANGED', pos_changed)
    excess_volumes = []    
    if len(vols_in_enstore_report) != len(vols_in_ibm_report):
        if len(vols_in_enstore_report) > len(vols_in_ibm_report):
            for_the_rest = vols_in_enstore_report
            check_in = vols_in_ibm_report
            b_len = len(vols_in_ibm_report)
            rest = len(vols_in_enstore_report) - len(vols_in_ibm_report)
            rest_starting_index = len(vols_in_ibm_report)
            rest_finishing_index = len(vols_in_enstore_report) - 1
            msg_header = 'Excess volumes in Estore report'
        else:
            for_the_rest = vols_in_ibm_report
            check_in = vols_in_enstore_report
            b_len = len(vols_in_enstore_report)
            rest = len(vols_in_ibm_report) - len(vols_in_enstore_report)
            rest_starting_index = len(vols_in_enstore_report)
            rest_finishing_index = len(vols_in_ibm_report)
            msg_header = 'Excess volumes in IBM report'
        i = rest_starting_index
        for i in  xrange(rest_starting_index, rest_finishing_index):
            for j in range(b_len):
                if for_the_rest[i][0] == check_in[j][0]:
                    break
            else:
                excess_volumes.append(for_the_rest[i])
            
    with open(result_file, 'w') as f:
        f.write('Resulst for %s %s\n'%(ibm_report, enstore_report))
        if len(did_not_compare) != 0:
            rc = (False, None)
            f.write('Did not compare\n')
            f.write('\tIBM\t\t\tEnstore\n')
            for e in  did_not_compare:
                f.write('%s \t%s\n'%(e[0],e[1]))
        if len(not_found_in_ibm_report) != 0:
            rc = (False, None)
            f.write('Not found in IBM report\n')
            for e in  not_found_in_ibm_report:
                f.write('%s\n'%(e,))
        if excess_volumes:
            rc = (False, None)
            f.write('%s\n'%(msg_header,))
            for i in range(len(excess_volumes)):
                f.write('%s\n'%(excess_volumes[i],))
                i += 1
    # Compare with last report
    # Find the last resulting file
    li = result_file.rfind('comparison-')
    if li > 0:
        li = li+len('comparison')
        search_for = '%s*'%(result_file[:li],)
        ret = shell_command('ls -ltr %s | tail -2'%(search_for,))
        if not ret:
            return(False, None)
        f_l = ret[0].split('\n')
        if len(f_l) == 3:
            # 2 files and empty string
            prev_file = f_l[0].split(' ')[-1]
            prev_file_1 = tempfile.mktemp(".txt")
            cur_file_1 = tempfile.mktemp(".txt")
            ret = shell_command('tail -n +2 %s > %s'%(prev_file, prev_file_1))
            ret = shell_command('tail -n +2 %s > %s'%(result_file, cur_file_1))
            cmp_cmd = 'diff -q %s %s'%(prev_file_1, cur_file_1)
            ret = shell_command2('%s'%(cmp_cmd,))
            os.system('rm %s %s'%(prev_file_1, cur_file_1))
            if ret[0] != 0:
                rc = (False, None)
            else:
                rc = (True, None)
    if not rc[0]:
        rc = (False, 'IBM and Estore reports do not match. See \n%s \n%s \n%s\n'%(ibm_report, enstore_report, result_file))
    return rc

def sendmail(subject, message):
    fn = '/tmp/volunes_info_r'
    with open(fn, 'w') as f:
        f.write('%s\n'%(message,))
    cmd = '/usr/bin/Mail -s "%s" %s < %s'%(subject, os.environ['ENSTORE_MAIL'], fn)
    #cmd = '/usr/bin/Mail -s "%s" %s < %s'%(subject, 'moibenko@fnal.gov', fn)
    res = shell_command('%s'%(cmd,))

def usage(arg):
    print('usage: %s <media_changer> <dst_dir>'%(arg[0],))

def main():
    #remote_host = 'stkensrv3n'
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)
    name = sys.argv[1]
    dst_base_dir=os.path.join(sys.argv[2], sys.argv[1])

    if not os.path.exists(dst_base_dir):
        try:
            os.makedirs(dst_base_dir)
        except Exception as e:
            print(e)
            sys.exit(1)
    t = time.strftime('%Y-%m-%d-%H:%M', time.localtime())
    t1 = time.strftime('%Y-%m-%d', time.localtime())
    dst_dir = '%s/%s'%(dst_base_dir, t1)
    if not os.path.exists(dst_dir):
        try:
            os.makedirs(dst_dir)
        except Exception as e:
            print(e)
            sys.exit(1)
    #os.chdir('/home/enstore/moibenko/TS4500')
    #cmd='enrsh -n %s locate TS4500CLI.jar'%(remote_host,)
    cmd='locate TS4500CLI.jar'
    res= shell_command('%s'%(cmd,))
    if len(res[0]) == 0:
        print('can not find CLI')
        sys.exit(1)
    cli=res[0].strip()
    fn ='%s.IBMCLI'%(name,)  
    #cmd= 'enrsh -n %s locate %s'%(remote_host, fn)
    cmd= 'locate %s'%(fn)
    res = shell_command('%s'%(cmd,))
    if len(res[0]) == 0:
        print('Cofiguration not found')
        sys.exit(1)

    #cmd='enrsh -n %s cat %s'%(remote_host, res[0],)
    cmd='cat %s'%(res[0],)
    res=shell_command('%s'%(cmd,))
    if res[0]:
        try:
            exec(res[0])
            ibm_cli_host =  IBMCLIHOST
            ibm_cli_u = IBMCLIU
            ibm_cli_pw = IBMCLIPW
        except Exception as e:
            print('Exception %s'%(e,))

    of = os.path.join(dst_dir, '%s-Cartridges-%s'%(name, t))
    cmd='java -jar %s -ip %s -u %s -p %s --viewDataCartridges'% (
        cli, ibm_cli_host, ibm_cli_u, ibm_cli_pw)
    #res = shell_command('enrsh -n %s %s'%(remote_host,cmd,))
    res = shell_command('%s'%(cmd,))
    with open(of, 'w') as f:
        f.write(res[0])

    # now get list from the media changer
    of1 = os.path.join(dst_dir, '%s-vol-list-%s'%(name, t))
    cmd='enstore med --list-vol %s > %s'%(name, of1)
    res = shell_command('%s'%(cmd,))

    rc = compare_ibm_enstore(of, of1)
    if not rc[0]:
        sendmail('Mismatch in %s, reported by IBM CLI and Enstore'%(sys.argv[1],), rc[1])

def test():
    of = '/home/enstore/TS4500G2_cartridges/2019-11-15/TS4500G2.media_changer-Cartridges-2019-11-15-11:12'
    of1 = '/home/enstore/TS4500G2_cartridges/2019-11-15/TS4500G2.media_changer-vol-list-2019-11-15-11:12'
    rc = compare_ibm_enstore(of, of1)
    if not rc[0]:
        sendmail('Mismatch in %s, reported by IBM CLI and Enstore'%(sys.argv[1],), rc[1])
    print(rc)

main()

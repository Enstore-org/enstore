#!/usr/bin/env python
import sys
import os
import string
import time
import getopt
import configuration_client

options = ['log_file=','date=']
MIN_FSIZE=100. # Minimal file size in MBytes to filter log messages

def usage():
    print "Usage:",sys.argv[0], "[--log_file=logfile] [--date=`date '+%Y-%m-%d'`] [file_size(in MBytes)]"
    


class Rtester:
    def __init__(self,configfile):
        # create configuration client
        port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
        port = string.atoi(port)
        if port:
            # we have a port
            host = os.environ.get('ENSTORE_CONFIG_HOST', 0)
            if host:
                # we have a host
                self.csc = configuration_client.ConfigurationClient((host, port))
            else:
                print "Cannot find config host"
                sys.exit(-1)
        else:
            print "Cannot find config port"
            sys.exit(-1)
        # get rates info
        self.rates = self.csc.get_uncached("rates")
        if not self.rates:
            print "rates information unavailabe: exit"
            sys.exit(-1)
        

    def min_rate(self,drive_type):
        for key in self.rates.keys():
            #print "MR",drive_type,key
            if string.find(drive_type,key) != -1:
                break
        else: return 0.
        return self.rates[key]['min_rate']

    def get_movers(self):
        lb_managers = self.csc.get_library_managers({}).keys()
        self.movers = []
        for lb in lb_managers:
            mvrs = self.csc.get_movers(lb+".library_manager")
            for mv in mvrs:
                self.movers.append(mv['mover'])
                

    def find_drive_type(self, mover):
        import udp_client
        # get the mover
        mv = self.csc.get_uncached(mover)
        u = udp_client.UDPClient()
        # communicate with mover
        # set mover to the draining state
        rc = u.send({"work" : "status"}, (mv['hostip'], mv['port']),
                    300, 10)
        #print "MOVER STATUS",rc
        drive_type = None
        if (rc.get('work_ticket',None) and  rc['work_ticket'].get('mover',None)):
           drive_type = rc['work_ticket']['mover'].get('product_id',None)
        return drive_type

        
        
    def check_mover(self, mover, drive_type, avg_rate):
        print "check_mover",mover,drive_type,avg_rate
        # find what are test tape media and test script for a given drive type
        for key in self.rates.keys():
            if string.find(drive_type,key) != -1:
                break
        else:
            print "Drive Type mismatch:%s"%(drive_type,)
            return

        # get the mover
        mv = self.csc.get_uncached(mover)
    
        if mv and mv.has_key('test_me'):
            # send command
            cmdline="%s %s %s %s"%(self.rates[key]['test_script'],
                                   mover,
                                   self.rates[key]['test_tape'],
                                   self.rates[key]['media'])
        
            cmd = 'rsh %s '%(mv['hostip'],)+'"sh -c \'%s'%(cmdline,)+'\'"'
            ret = os.popen(cmd).readlines()
            for item in ret:
                if string.find(item,'Transfer rates') != -1:
                    print "Transfer rates test completed"
                    print item
                    strs = string.split(item, " ")
                    w_rate = string.atof(strs[9])
                    r_rate = string.atof(strs[12])
                    if w_rate >= self.min_rate(drive_type):
                        print "Write Rate is OK"
                    else:
                        msg = "Write rate is SLOW"
                        if w_rate > avg_rate:
                            msg = msg+", but more than average"
                        print msg
                    if r_rate >= self.min_rate(drive_type):
                        print "Read Rate is OK"
                    else:
                        msg = "Read rate is SLOW"
                        if r_rate > avg_rate:
                            msg = msg+", but more than average"
                        print msg
                    
                    break
            #os.system(cmd)
        return None
        
    
if __name__ == "__main__":    
    movers = {}
    log_file=''
    date=''

    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    if optlist:
        if len(optlist) != 1:
            usage()
        else:
            if optlist[0][0] == '--log_file':
                log_file = optlist[0][1]
            elif optlist[0][0] == '--date':
                date = optlist[0][1]

    tst = Rtester(os.path.join(os.environ.get('ENSTORE_DIR', 0),'etc','rates.conf'))
    tst.get_movers()
    #print "MVRS",tst.movers
    if not log_file:
        # get log_server entry
        log_server_info = tst.csc.get_uncached("log_server")
        if not date:
            date=time.strftime("%Y-%m-%d",time.localtime(time.time()))
        log_file=log_server_info['log_file_path']+"LOG-"+date

    if args:
        min_fsize = string.atoi(args[0])
    else:
        min_fsize = MIN_FSIZE
    
    #print "CONFIG_DICT",tst.configdict
    #print "DATE",date
    #print "LOG FILE", log_file
    f = open(log_file,'r')
    line=f.readline()
    while line:
        if (string.find(line," ENCP") != -1 and
            (string.find(line,"read_from_hsm") != -1 or
             string.find(line,"write_to_hsm") != -1)):
            #print line
            w = string.split(line, " ")
            # remove all empty entries
            words = []
            for i in w:
                if i: words.append(i)
            del(w)
            if words[4] != 'E':
                try:
                    stat = {'time': words[0],
                            'work': words[7],
                            'size': string.atof(words[11]),
                            'rate': string.atof(string.split(words[19],'(')[1]),
                            'mover':string.split(words[21],'=')[1],
                            'drive_type':string.split(words[22],'=')[1],
                            'drive_sn':string.split(words[23],'=')[1],
                            'vendor':string.split(words[24],'=')[1],
                            }
                    #print "STAT",stat
                    #print "SIZE",stat['size']/1024./1024.
                    if stat['size']/1024./1024. >= MIN_FSIZE:
                        # add to mover list entry
                        if not movers.has_key(stat['mover']):
                            movers[stat['mover']] = {'rates':[], 'stat':stat}
                        movers[stat['mover']]['rates'].append(stat['rate'])
                        # calculate the average rate
                        avg = 0.
                        for i in range(0,len(movers[stat['mover']]['rates'])):
                            avg = avg + movers[stat['mover']]['rates'][i]
                        avg = avg / (len(movers[stat['mover']]['rates'])*1.)
                        movers[stat['mover']]['average'] = avg
                except IndexError:
                    print "WRONG MESSAGE FORMAT"
        line=f.readline()

    #print "MOVERS",movers
    checked_movers = []
    # go through list of suspected movers and test them
    for key in movers.keys():
        #print "AVG,MIN",movers[key]['average'],tst.min_rate(movers[key]['stat']['drive_type'])
        if movers[key]['average'] < tst.min_rate(movers[key]['stat']['drive_type']):
            print "SLOW"
            ret = tst.check_mover(movers[key]['stat']['mover'],
                                  movers[key]['stat']['drive_type'],
                                  movers[key]['average'])
        else:
            print "DRIVE RATE IS O'K",movers[key]['stat']['mover'],

        checked_movers.append(movers[key]['stat']['mover'])

    # see if all movers have been checked
    not_checked_movers = []
    for mv in tst.movers:
        if not (mv in checked_movers):
            not_checked_movers.append(mv)

    for mv in not_checked_movers:
        drive_type = tst.find_drive_type(mv)
        #print "DRIVE TYPE", drive_type
        if drive_type:
            try:
                avg = movers[key]['average']
            except NameError:
                avg = 0.         # check unconditionally 
            ret = tst.check_mover(mv,
                                  drive_type,avg)
        
    sys.exit(0)

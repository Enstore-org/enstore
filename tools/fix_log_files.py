#!/usr/bin/env python
"""
Find missed messages in log server out put file and fill enstore log.
"""
import sys
import time
import socket
import os

import udp_common

LOG_MESSAGE_KEYWORD =  "Intermediate queue is full "

class LogFixer:
    def __init__(self, input_file, output_dir):
        self.log_dict ={}
        self.input_file = input_file
        self.input_fn = open(input_file, 'r')
        self.output_dir = output_dir
        self.total_entries_processed = 0L
        self.useful_entries_processed = 0L
        self.outfiles = []

        
    def extract_log_message(self, message):
        # The message looks like:
        # 2014-10-16 11:47:23 (\'("(\'131.225.13.15-33976-1413478036.907071-2936-47120465922576\', 12L, {\'work\': \'alive\'})", 3783201541L)\', (\'131.225.13.15\', 33976))'
        rc = None
        if not LOG_MESSAGE_KEYWORD in message:
            return rc
        log_message_dated = message.split(LOG_MESSAGE_KEYWORD)[1]
        d, t, msg = log_message_dated.split(' ', 2)
        try:
            rq, client_addr = udp_common.r_eval(msg, check=True)
            #print "RQ", rq
            #print "CA", client_addr
            #idn, number, ticket = udp_common.r_eval(request, check=False)
        except Exception, detail:
            #print "DET", detail
            return None
        if rq:
            try:
                #print "RQQ", type(rq), rq
                req, inCRC = udp_common.r_eval(rq, check=True)
                #idn, number, ticket = udp_common.r_eval(rq, check=False)
            except Exception, detail:
                #print "RQ", detail
                return None
        if req:
            try:
                idn, number, ticket = udp_common.r_eval(req, check=False)
            except Exception, detail:
                #print "RQ1", detail
                return None
        #print "TICK", type(ticket), ticket
        if 'message' in ticket and 'work' in ticket and ticket['work'] == 'log_message':
            tm = time.strptime(" ".join((d, t)), "%Y-%m-%d %H:%M:%S")
            rc = tm, ticket, socket.gethostbyaddr(client_addr[0])[0]

        return rc

    def extract_time(self, id):
        # id nas the following format:
        #
        args = id.split("-")
        #print "ARGS", args
        return float(args[2])

    def udate_log_dict(self, msg):
        ts = msg[0]
        if ts.tm_year not in self.log_dict:
            self.log_dict[ts.tm_year] = {} # year
        if ts.tm_mon not in self.log_dict[ts.tm_year]:
            self.log_dict[ts.tm_year][ts.tm_mon] = {}
        if ts.tm_mday not in self.log_dict[ts.tm_year][ts.tm_mon]:
            self.log_dict[ts.tm_year][ts.tm_mon][ts.tm_mday] = []

        tod = time.strftime("%H:%M:%S", ts)
        self.log_dict[ts.tm_year][ts.tm_mon][ts.tm_mday].append((tod, msg[1]['message'], msg[2]))
       
    def process_input_file(self):
        while 1:
            l = self.input_fn.readline()
            if l:
                msg = self.extract_log_message(l)
                self.total_entries_processed += 1
                if msg:
                    if "STOPPED HERE" in msg:
                        # reset everything"
                        del(self.log_dict)
                        self.log_dict = {}
                        self.total_entries_processed = 0
                        self.useful_entries_processed += 1
                    else:
                        #print "MMMMMMMMMM", msg
                        self.udate_log_dict(msg)
                        self.useful_entries_processed += 1
            else:
                break
        
            
        self.input_fn.close()
        fn = open(self.input_file, 'a')
        
        fn.write("STOPPED HERE\n")
        fn.close()

    def generate_output(self):
        if not os.path.exists(self.output_dir):
            try:
                os.makedirs(self.output_dir)
            except:
                print "Can not create output directory %s"%(self.output_dir,)
                sys.exit(1)
        for year in self.log_dict:
            for month in self.log_dict[year]:
                for day in self.log_dict[year][month]:
                    fn = "LOG-%d-%02d-%02d.appendix"%(int(year), int(month), int(day))
                    output_path = os.path.join(self.output_dir, fn)
                    print "create", output_path
                    of = open(output_path, 'w')
                    self.outfiles.append(output_path)
                    for entry in self.log_dict[year][month][day]:
                        of.write("%s %s %s\n"%(entry[0], entry[2], entry[1]))
                    of.close()
        print "OUTFILES", self.outfiles

    def merge_logs(self):
        for f in self.outfiles:
            d = os.path.dirname(f)
            lf = f.split('.')[0]
            cmd = "cat %s %s | sort > %s.common"%(lf, f, lf)
            os.system(cmd)
    
        
                                 
        

if __name__ == "__main__" :
    infile = open(sys.argv[1], 'r')

    fixer = LogFixer(sys.argv[1], sys.argv[2])
    t = time.time()
    fixer.process_input_file()
    print "Total Entries %s. Useful entries %s. Processing time %s"%(fixer.total_entries_processed,
                                                                     fixer.useful_entries_processed,
                                                                     time.time() - t)
    fixer.generate_output()
    fixer.merge_logs()
    
    

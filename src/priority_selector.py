#!/usr/bin/env python

###############################################################################
# $Id$
#
# system imports
from __future__ import print_function
import sys
import os
import stat
import errno
import string
import re
# import pcre has been deprecated
import copy
import traceback
import e_errors
import Trace

MAX_REG_PRIORITY = 1000001


class PriSelector:

    def read_config(self):
        Trace.log(e_errors.INFO, "(Re)loading priority")
        self.exists = 0

        dict = self.csc.get('priority', {})
        if dict['status'][0] == e_errors.OK:
            prioritydict = dict.get(self.library_manager, {})
        else:
            prioritydict = {}
        if prioritydict:
            self.exists = 1
        self.prioritydict = prioritydict
        self.base_dict = prioritydict.get('basepri', {})
        self.adm_dict = prioritydict.get('adminpri', {})
        self.base_pri_keys = self.base_dict.keys()
        self.admin_pri_keys = self.adm_dict.keys()
        self.base_pri_keys.sort()
        self.base_pri_keys.reverse()
        self.admin_pri_keys.sort()
        self.admin_pri_keys.reverse()
        return (e_errors.OK, None)

    def __init__(self, csc, library_manager, max_reg_pri=MAX_REG_PRIORITY):
        self.max_reg_pri = max_reg_pri
        self.library_manager = library_manager
        self.csc = csc
        self.read_config()

    def ticket_match(self, ticket, pri_key, conf_key):
        pattern = "^%s" % (self.prioritydict[pri_key][conf_key],)
        item = '%s' % (ticket.get(conf_key, 'Unknown'),)
        try:
            if re.search(pattern, item):
                return 1
            else:
                return 0
        except BaseException:
            Trace.log(e_errors.ERROR, "parse errorr")
            Trace.handle_error()
            return 0
        #pcre is deprecated
        # except pcre.error, detail:
        #    Trace.log(e_errors.ERROR,"parse errorr %s" % (detail, ))
        #    return 0

    def priority(self, ticket):
        # self.read_config()
        if not self.exists:  # no priority configuration info
            return ticket['encp']['basepri'], ticket['encp']['adminpri']
        # make a "flat" copy of ticket
        # use deepcopy
        flat_ticket = copy.deepcopy(ticket)
        # flat_ticket.update(ticket)
        # before making a ticket remove ['vc']['wrapper'] as it will interfere
        # with 'wrapper' (see ticket structure)
        if 'wrapper' in flat_ticket['vc']:
            del(flat_ticket['vc']['wrapper'])
        for key in flat_ticket.keys():
            if isinstance(flat_ticket[key], type({})):
                for k in flat_ticket[key].keys():
                    if k == 'machine':
                        flat_ticket['host'] = flat_ticket[key][k][1]
                    else:
                        flat_ticket[k] = flat_ticket[key][k]
                del(flat_ticket[key])

        cur_pri = flat_ticket['basepri']
        cur_adm_pri = flat_ticket.get('adminpri', -1)
        daq_enabled = flat_ticket.get('encp_daq', None)
        # regular priority
        self.prioritydict = self.base_dict
        pri_keys = self.base_pri_keys
        for pri_key in pri_keys:
            conf_keys = self.prioritydict[pri_key].keys()
            nkeys = len(conf_keys)
            nmatches = 0
            for conf_key in conf_keys:
                # try to match a ticket
                if not self.ticket_match(flat_ticket, pri_key, conf_key):
                    break
                nmatches = nmatches + 1
            if nmatches == nkeys:
                if (pri_key <= self.max_reg_pri):
                    if pri_key + cur_pri <= self.max_reg_pri:
                        cur_pri = pri_key + cur_pri
                else:
                    cur_pri = pri_key + cur_pri
                break
        # admin priority
        self.prioritydict = self.adm_dict
        pri_keys = self.admin_pri_keys
        for pri_key in pri_keys:
            conf_keys = self.prioritydict[pri_key].keys()
            nkeys = len(conf_keys)
            nmatches = 0
            for conf_key in conf_keys:
                # try to match a ticket
                if not self.ticket_match(flat_ticket, pri_key, conf_key):
                    break
                nmatches = nmatches + 1
            if nmatches == nkeys:
                if (pri_key <= self.max_reg_pri):
                    if pri_key + cur_adm_pri <= self.max_reg_pri:
                        cur_adm_pri = pri_key + cur_adm_pri
                else:
                    cur_adm_pri = pri_key + cur_adm_pri
                break

        if cur_pri >= self.max_reg_pri:
            if daq_enabled:
                cur_adm_pri = cur_pri / self.max_reg_pri + cur_adm_pri
            cur_pri = self.max_reg_pri
        return cur_pri, cur_adm_pri


if __name__ == "__main__":
    import configuration_client
    def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
                string.atoi(os.environ['ENSTORE_CONFIG_PORT']))
    csc = configuration_client.ConfigurationClient(def_addr)
    ps = PriSelector(csc, 'mam.library_manager')
    # ps.read_config()
    ticket = {'unique_id': 'happy.fnal.gov-959786955.526691-14962', 'at_the_top': 2,
              'encp': {'delayed_dismount': 1, 'basepri': 1, 'adminpri': -1, 'curpri': 1,
                       'agetime': 0, 'delpri': 0}, 'fc': {'address': ('131.225.84.122', 7501),
                                                          'size': 5158,
                                                          'external_label': 'null02'},
              'vc': {'library': 'happynull', 'file_family_width': 2,
                     'volume_family': 'D0.alex.null', 'address': ('131.225.84.122', 7502),
                     'wrapper': 'null', 'file_family': 'alex.null',
                     'at_mover': ('unmounted', 'none'), 'storage_group': 'D0'},
              'times': {'t0': 959786955.184, 'in_queue': 2.62227797508,
                        'job_queued': 959786955.542, 'lm_dequeued': 959786958.164},
              'wrapper': {'minor': 0, 'inode': 0,
                          'fullname': '/home/moibenko/enstore2/src/alarm.py',
                          'size_bytes': 5158, 'gname': 'hppc', 'mode': 33261,
                          'gid': 5440, 'mtime': 959786955, 'sanity_size': 65536,
                          'machine': ('Linux',
                                      'happy.fnal.gov',
                                      '2.2.15',
                                      '#4 SMP Tue May 30 13:35:20 CDT 2000', 'i686'),
                          'uname': 'moibenko',
                          'pstat': (16893, 70397816, 3, 1, 6849, 5440, 512, 959704674, 959704674, 959704674),
                          'uid': 6849, 'pnfsFilename': '/pnfs/rip6/happy/NULL/d2/alarm_client.py',
                          'rminor': 0, 'rmajor': 0, 'type': 'null', 'major': 0},
              'lm': {'address': ('131.225.84.122', 7503)}, 'callback_addr': ('131.225.84.122', 7600),
              'work': 'write_to_hsm', 'retry': 2, 'status': ('ok', None)}
    pri, adm_pri = ps.priority(ticket)
    print("END")
    print("Priority:", pri, adm_pri)

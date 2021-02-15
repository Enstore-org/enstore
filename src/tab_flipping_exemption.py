#!/usr/bin/env python

from __future__ import print_function
import option
import configuration_client
import pg
import log_client
import sys
import string
import os
import pwd
import Trace
import e_errors

MY_NAME = "TAB_FLIPPING_EXEMPTION"
MY_SERVER = None

# take a query object and pretty print the results


def show_query_result(res, msg=None):

    # determine the format
    width = []
    fields = res.listfields()
    w = len(fields)
    for i in range(w):
        width.append(len(fields[i]))

    result = res.getresult()

    if not result:
        return

    for r in result:
        for i in range(w):
            l1 = len(str(r[i]))
            if l1 > width[i]:
                width[i] = l1

    format = []
    for i in range(w):
        format.append("%%%ds " % (width[i]))

    if msg:
        ll = len(msg)
        print("=" * ll)
        print(msg)
        print("=" * ll)
        print()

    # determine the length of the separation line
    ll = 0
    for i in range(w):
        ll = ll + width[i]
    ll = ll + 2 * (w - 1)

    # print the header
    for i in range(w):
        print(format[i] % (fields[i]), end=' ')
    print()
    print("-" * ll)

    # print the rows
    for r in result:
        for i in range(w - 1):
            print(format[i] % (r[i]), end=' ')
        if r[w - 1] is None:
            print(format[w - 1] % (0))
        else:
            print(format[w - 1] % (r[w - 1]))
    print()
    print()

# try to identify the user using LOGNAME and kerberos principal


def whoami():
    # find the principal of kerberos ticket
    # taking only from stdout, ignoring stderr
    kmsg = os.popen3("klist")[1].readlines()
    kk = 'unknown'
    for i in kmsg:
        if i[:18] == "Default principal:":
            kk = string.split(i)[2]
            break
    logname = 'unknown'
    # try to find the real user name through $LOGNAME
    if 'LOGNAME' in os.environ:
        logname = os.environ['LOGNAME']
    else:
        # if failed, use effective user name
        logname = pwd.getpwuid(os.getuid())[0]

    return "%s(%s)" % (logname, kk)

# handles everthing with quota


class TabFlippingExemption:
    def __init__(self, db):
        self.db = db
        self.uname = whoami()

    # informational log any way, stick user identity before the msg
    def log(self, m):
        Trace.log(e_errors.INFO, self.uname + ' ' + m)

    # show [storage_group [, file_family]]
    def show(self, sg=None, ff=None):
        if sg:
            if ff:
                q = "select * from no_flipping_file_family \
					where storage_group = '%s' and \
						file_family = '%s';" % (
                    sg, ff)
                show_query_result(self.db.query(q), "Exempted storage group")
            else:
                q = "select * from no_flipping_storage_group \
					where storage_group = '%s';" % (sg)
                show_query_result(self.db.query(q), "Exempted storage group")
                q = "select * from no_flipping_file_family \
					where storage_group = '%s';" % (sg)
                show_query_result(self.db.query(q), "Exempted file family")
        else:
            q = "select * from no_flipping_storage_group;"
            show_query_result(self.db.query(q), "Exempted storage group")
            q = "select * from no_flipping_file_family;"
            show_query_result(self.db.query(q), "Exempted file family")

    # add storage [, file_family]
    def add(self, sg, ff=None):
        if sg:
            if ff:
                q = "insert into no_flipping_file_family \
					(storage_group, file_family) \
					values ('%s', '%s');" % (sg, ff)
                self.log("ADD %s %s" % (sg, ff))
            else:
                q = "insert into no_flipping_storage_group \
					(storage_group) values ('%s');" % (
                    sg)
                self.log("ADD %s" % (sg))
            try:
                self.db.query(q)
            except BaseException:
                exc_type, exc_value = sys.exc_info()[:2]
                msg = str(exc_type) + ' ' + str(exc_value) + ' query:' + q
                Trace.log(e_errors.ERROR, msg)
        self.show()

    def delete(self, sg, ff=None):
        if sg:
            if ff:
                q = "delete from no_flipping_file_family \
					where storage_group = '%s' and \
					file_family = '%s';" % (sg, ff)
                self.log("DELETE %s %s" % (sg, ff))
            else:
                q = "delete from no_flipping_storage_group \
					where storage_group = '%s';" % (sg)
                self.log("DELETE %s" % (sg))
            try:
                self.db.query(q)
            except BaseException:
                exc_type, exc_value = sys.exc_info()[:2]
                msg = str(exc_type) + ' ' + str(exc_value) + ' query:' + q
        self.show()


class Interface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):
        self.show = None
        self.storage_group = None
        self.file_family = None
        self.add = None
        self.delete = None

        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.quota_options)

    quota_options = {
        option.SHOW: {
            option.HELP_STRING: "show tab flipping exemption",
            option.VALUE_TYPE: option.STRING,
            option.VALUE_USAGE: option.OPTIONAL,
            option.VALUE_LABEL: "storage_group",
            option.DEFAULT_VALUE: "-1",
            option.USER_LEVEL: option.ADMIN,
            option.EXTRA_VALUES: [{
                option.VALUE_NAME: "file_family",
                option.VALUE_TYPE: option.STRING,
                option.VALUE_USAGE: option.OPTIONAL,
                option.DEFAULT_TYPE: None,
                option.DEFAULT_VALUE: None
            }]},
        option.ADD: {
            option.HELP_STRING: "add exemption for storage_group [, file_family] ",
            option.VALUE_TYPE: option.STRING,
            option.VALUE_USAGE: option.REQUIRED,
            option.VALUE_LABEL: "storage_group",
            option.USER_LEVEL: option.ADMIN,
            option.DEFAULT_VALUE: None,
            option.EXTRA_VALUES: [{
                option.VALUE_NAME: "file_family",
                option.VALUE_TYPE: option.STRING,
                option.VALUE_USAGE: option.OPTIONAL,
                option.DEFAULT_TYPE: None,
                option.DEFAULT_VALUE: None
            }]},
        option.DELETE: {
            option.HELP_STRING: "delete exemption for storage_group [, file_family]",
            option.VALUE_TYPE: option.STRING,
            option.VALUE_USAGE: option.OPTIONAL,
            option.VALUE_LABEL: "storage_group",
            option.DEFAULT_VALUE: None,
            option.USER_LEVEL: option.ADMIN,
            option.EXTRA_VALUES: [{
                option.VALUE_NAME: "file_family",
                option.VALUE_TYPE: option.STRING,
                option.VALUE_USAGE: option.OPTIONAL,
                option.DEFAULT_TYPE: None,
                option.DEFAULT_VALUE: None
            }]},
    }


def do_work(intf):
    # get database
    csc = configuration_client.ConfigurationClient(
        (intf.config_host, intf.config_port))
    dbInfo = csc.get('database')
    try:
        db = pg.DB(
            host=dbInfo['dbhost'],
            port=dbInfo['dbport'],
            dbname=dbInfo['dbname'],
            user=dbInfo['dbuser'])
    except BaseException:
        exc_type, exc_value = sys.exc_info()[:2]
        print(str(exc_type) + ' ' + str(exc_value))
        sys.exit(0)
    tfe = TabFlippingExemption(db)
    logc = log_client.LoggerClient(csc)
    Trace.init(string.upper(MY_NAME))

    if intf.show:
        if intf.show == "-1":
            tfe.show(None, intf.file_family)
        else:
            tfe.show(intf.show, intf.file_family)
    elif intf.add:
        tfe.add(intf.add, intf.file_family)
    elif intf.delete:
        tfe.delete(intf.delete, intf.file_family)


if __name__ == '__main__':
    intf = Interface(user_mode=0)
    do_work(intf)

#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import string
import os
import getopt
import sys

# enstore imports
#import setpath
import Trace
import e_errors
import hostaddr


def getenv(var, default=None):
    val = os.environ.get(var)
    if val is None:
        used_default = 1
        val = default
    else:
        used_default = 0
    return val, used_default


DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '7500'

used_default_config_host = 0
used_default_config_port = 0


def default_host():
    val, used_default = getenv('ENSTORE_CONFIG_HOST', default=DEFAULT_HOST)
    if used_default:
        global used_default_config_host
        used_default_config_host = 1
    return val


def default_port():
    val, used_default = getenv('ENSTORE_CONFIG_PORT', default=DEFAULT_PORT)
    val = int(val)
    if used_default:
        global used_default_config_port
        used_default_config_port = 1
    return val


def default_file():
    return "/pnfs/enstore/.(config)(flags)/enstore.conf"


def log_using_default(var, default):
    Trace.log(e_errors.INFO,
              "%s not set in environment or command line - reverting to %s"
              % (var, default))


def check_for_config_defaults():
    # check if we are using the default host and port.  if this is true
    # then nothing was set in the environment or passed on the command
    # line. warn the user.
    if used_default_config_host:
        log_using_default('CONFIG HOST', DEFAULT_HOST)
    if used_default_config_port:
        log_using_default('CONFIG PORT', DEFAULT_PORT)


def str_to_tuple(s):
    # convert the string of the form "(val1, val2)" to a tuple of the form
    # (val1, val2) by doing the following -
    #              remove all surrounding whitespace
    #              remove the first char : the '(' char
    #              remove the last char  : the ')' char
    #              split into two based on ',' char
    tmp = string.strip(s)
    tmp = tmp[1:-1]
    return tuple(string.split(tmp, ",", 1))


def underscore_to_dash(s):
    # accept - rather than _ in arguments - but only in the keywords, not
    # the values!
    if s[:2] != '--':
        return s
    t = '--'
    eq = 0
    for c in s[2:]:
        if c == '=':
            eq = 1
        if c == '_' and not eq:
            c = '-'
        t = t + c
    return t


def parse_range(s):
    if ',' in s:
        s = string.split(s, ',')
    else:
        s = [s]
    r = []
    for t in s:
        if '-' in t:
            lo, hi = string.split(t, '-')
            lo, hi = int(lo), int(hi)
            r.extend(range(lo, hi + 1))
        else:
            r.append(int(t))
    return r


class Interface:
    def __init__(self, host=default_host(), port=default_port()):
        # make pychecker happy
        self.csc = None
        self.test_mode = None
        if self.__dict__.get("do_parse", 1):
            if host == 'localhost':
                self.check_host(hostaddr.gethostinfo()[0])
            else:
                self.check_host(host)
            self.check_port(port)
            self.parse_options()

    def check_host(self, host):
        self.config_host = hostaddr.name_to_address(host)

    def check_port(self, port):
        # bomb out if port isn't numeric
        if isinstance(port, type('string')):
            self.config_port = int(port)
        else:
            self.config_port = port

    def charopts(self):
        return [""]

    def help_options(self):
        return ["help", "usage-line"]

    def config_options(self):
        return ["config-host=", "config-port="]

    def alive_rcv_options(self):
        return ["timeout=", "retries="]

    def alive_options(self):
        return ["alive"] + self.alive_rcv_options()

    def trace_options(self):
        return ["do-print=", "dont-print=", "do-log=",
                "dont-log=", "do-alarm=", "dont-alarm="]

    def format_options(self, opts, prefix):
        # put the options in alphabetical order and add a "--" to the front of
        # each
        opts.sort()
        nopts = ""
        for opt in opts:
            nopts = nopts + prefix + "--" + opt
        return nopts

    def missing_parameter(self, param):
        Trace.trace(13, "ERROR: missing parameter %s" % (param,))
        try:
            sys.stderr.write("ERROR: missing parameter %s\n" % (param,))
            sys.stderr.flush()
        except IOError:
            pass

    def parameters(self):
        return " "

    def help_prefix(self):
        return sys.argv[0] + " [opts] "

    def help_suffix(self):
        return "\n\n\t where 'opts' are:\n"

    def help_line(self):
        return self.help_prefix() + self.parameters() + self.help_suffix() + self.format_options(
            self.options(), "\n\t\t")

    def print_help(self):
        try:
            sys.stderr.write("USAGE: %s\n" % (self.help_line(),))
            sys.stderr.flush()
        except IOError:
            pass

    def print_usage_line(self, opts=None):
        if opts is None:
            opts = self.options()
        try:
            sys.stderr.write(self.get_usage_line(opts))
            sys.stderr.flush()
        except IOError:
            pass

    def get_usage_line(self, opts=None):
        if opts is None:
            opts = self.options()
        return "[" + self.format_options(opts, " ") + "] " + self.parameters()

    def parse_config_host(self, value):
        try:
            self.csc.config_host = value
            self.csc.check_host(self.csc.config_host)
        except AttributeError:
            self.config_host = value
            self.check_host(self.config_host)

    def parse_config_port(self, value):
        try:
            self.csc.check_port(value)
        except AttributeError:
            self.check_port(value)

    def strip(self, value):
        return value

    # This is a dummy options(), the derived class should supply a real
    # one
    def options(self):
        return []

    def parse_options(self):
        self.options_list = []
        try:
            argv = map(underscore_to_dash, sys.argv[1:])
            optlist, self.args = getopt.getopt(argv, self.charopts(),
                                               self.options())
        except getopt.error as detail:
            Trace.trace(9, "ERROR: getopt error %s" % (detail,))
            try:
                sys.stderr.write("error: %s\n" % (detail,))
                sys.stderr.flush()
            except IOError:
                pass
            self.print_help()
            sys.exit(1)
        for (opt, value) in optlist:
            # keep a list of the options entered without the leading "--"
            self.options_list.append(string.replace(opt, "-", ""))

            value = self.strip(value)
            Trace.trace(10, "opt = %s, value = %s" % (opt, value))
            if opt == "--add":
                self.add = value
            elif opt == "--age-time":
                self.age_time = int(value)
            elif opt == "--alive":
                self.alive = 1
            elif opt == "--all":
                self.all = 1
            elif opt == "--array-size":
                self.array_size = int(value)
            elif opt == "--backup":
                self.backup = 1
            elif opt == "--bfid":
                self.bfid = value
            elif opt == "--bfids":
                # recycle it for dul purpose
                if value:
                    self.bfids = value
                else:
                    self.bfids = 1
            elif opt == "--bytes":
                if not self.test_mode:
                    try:
                        sys.stderr.write(
                            "bytecount may only be specified in test mode\n")
                        sys.stderr.flush()
                    except IOError:
                        pass
                    sys.exit(-1)
                if value[-1] == 'L':
                    value = value[:-1]
                self.bytes = long(value)
            elif opt == "--buffer-size":
                self.buffer_size = int(value)
            elif opt == "--caption-title":
                self.caption_title = value
            elif opt == "--change-priority":
                self.change_priority = 1
            elif opt == "--check":
                self.check = value
            elif opt == "--clean-drive":
                self.clean_drive = 1
            elif opt == "--clear":
                self.clear = value
            elif opt == "--config-file":
                self.config_file = value
            elif opt == "--config-host":
                self.parse_config_host(value)
            elif opt == "--config-port":
                self.parse_config_port(value)
            elif opt == "--data-access-layer":
                self.data_access_layer = 1
            elif opt == "--decr-file-count":
                self.decr_file_count = value
            elif opt == "--delayed-dismount":
                self.delayed_dismount = int(value)
            elif opt == "--delete":
                # recycle it for dual purpose
                if value:
                    self.delete = value
                else:
                    self.delete = 1
            elif opt == "--delete-work":
                self.work_to_delete = value
                self.delete_work = 1
            elif opt == "--deleted":
                self.deleted = value
            elif opt == "--delpri":
                self.delpri = int(value)
            elif opt == "--description":
                self.description = value
            elif opt == "--destroy":
                self.rmvol = value
            elif opt == "--direct-io":
                self.direct_io = 1
            elif opt == "--dismount":
                self.dismount = 1
            elif opt == "--do-alarm":
                self.do_alarm = parse_range(value)
            elif opt == "--do-log":
                self.do_log = parse_range(value)
            elif opt == "--do-print":
                self.do_print = parse_range(value)
            elif opt == "--dont-alarm":
                self.dont_alarm = parse_range(value)
            elif opt == "--dont-log":
                self.dont_log = parse_range(value)
            elif opt == "--dont-print":
                self.dont_print = parse_range(value)
            elif opt == "--down":
                self.down = value
            elif opt == "--dump":
                self.dump = 1
            elif opt == "--mover-dump":
                self.mover_dump = 1
            elif opt == "--ecrc":
                self.ecrc = 1
            elif opt == "--ephemeral":
                self.output_file_family = "ephemeral"
            elif opt == "--export":
                if value:
                    self.export = value
                else:
                    self._export = 1
            elif opt == "--file-family":
                self.output_file_family = value
            elif opt == "--force":
                self.force = 1
            elif opt == "--get-bfid":
                self.get_bfid = 1
            elif opt == "--get-cache":
                self.get_cache = 1
            elif opt == "--get-crcs":
                self.get_crcs = value
            elif opt == "--get-update-interval":
                self.get_update_interval = 1
            elif opt == "--get-last-logfile-name":
                self.get_last_logfile_name = 1
            elif opt == "--get-logfile-name":
                self.get_logfile_name = 1
            elif opt == "--get-logfiles":
                self.get_logfiles = value
            elif opt == "--get-max-encp-lines":
                self.get_max_encp_lines = 1
            elif opt == "--get-patrol-file":
                self.get_patrol_file = 1
            elif opt == "--get-queue":
                self.get_queue = value
            elif opt == "--print-queue":
                self.print_queue = value
            elif opt == "--get-refresh":
                self.get_refresh = 1
            elif opt == "--get-suspect-vols":
                self.get_susp_vols = 1
            elif opt == "--get-work":
                self.get_work = 1
            elif opt == "--get-work-sorted":
                self.get_work_sorted = 1
            elif opt == "--help":
                self.print_help()
                sys.exit(0)
            elif opt == "--hostip":
                self.hostip = value
            elif opt == "--html":
                self.html = 1
            elif opt == "--html-dir":
                self.html_dir = value
            elif opt == "--html-file":
                self.html_file = value
            elif opt == "--html-gen-host":
                self.html_gen_host = value
            elif opt == "--idle":
                self.stop_draining = 1
            elif opt == "--import":
                if value:
                    self._import = value
                else:
                    self._import = 1
            elif opt == "--input-dir":
                self.input_dir = value
            elif opt == "--update-interval":
                self.update_interval = int(value)
            elif opt == "--interval":
                self.interval = int(value)
            elif opt == "--keep":
                self.keep = 1
            elif opt == "--keep-dir":
                self.keep_dir = value
            elif opt == "--list":
                self.list = value
            elif opt == "--ls-active":
                self.list_active = value
            elif opt == "--load":
                self.load = 1
            elif opt == "--logfile-dir":
                self.logfile_dir = value
            elif opt == "--client-name":
                self.client_name = value
            elif opt == "--max-encp-lines":
                self.max_encp_lines = int(value)
            elif opt == "--max-retry":
                self.max_retry = int(value)
            elif opt == "--max-resubmit":
                self.max_resubmit = int(value)
            elif opt == "--max-work":
                self.max_work = int(value)
            elif opt == "--mc":
                self.mcs = string.split(value, ",")
            elif opt == "--message":
                self.message = value
            elif opt == "--mmap-io":
                self.mmap_io = 1
            elif opt == "--mmap-size":
                self.mmap_size = long(value)
            elif opt == "--modify":
                self.modify = value
            elif opt == "--mount":
                self.mount = 1
            elif opt == "--new-library":
                self.new_library = value
            elif opt == "--next":
                self.next = 1
            elif opt == "--no-access":
                self.no_access = value
            elif opt == "--no-crc":
                self.chk_crc = 0
            elif opt == "--nocheck":
                self.nocheck = 1
            elif opt == "--nooutage":
                self.nooutage = value
            elif opt == "--offline":
                self.start_draining = 1
            elif opt == "--down":
                self.start_draining = 1
            elif opt == "--outage":
                self.outage = value
            elif opt == "--override":
                self.override = value
            elif opt == "--nooverride":
                self.nooverride = value
            elif opt == "--saagStatus":
                self.saagStatus = value
            elif opt == "--output":
                self.output = value
            elif opt == "--output-dir":
                self.output_dir = value
            elif opt == "--pnfs-mount-point":
                self.pnfs_mount_point = value
            elif opt == "--prefix":
                self.prefix = value
            elif opt == "--priority":
                self.priority = int(value)
            elif opt == "--put-cache":
                self.put_cache = 1
            elif opt == "--raise":
                self.alarm = 1
            elif opt == "--read-only":
                self.read_only = value
            elif opt == "--recursive":
                self.restore_dir = 1
            elif opt == "--refresh":
                self.refresh = int(value)
            elif opt == "--reset-lib":
                self.lm_to_clear = value
            elif opt == "--resolve":
                self.resolve = value
            elif opt == "--restore":
                self.restore = value
            elif opt == "--restore-all":
                self.restore_all = 1
            elif opt == "--rm-active-vol":
                self.active_volume = value
                self.rm_active_vol = 1
            elif opt == "--rm-suspect-vol":
                self.suspect_volume = value
                self.rm_suspect_vol = 1
            elif opt == "--root-error":
                self.root_error = value
            elif opt == "--set-crcs":
                self.set_crcs = value
            elif opt == "--severity":
                self.severity = value
            elif opt == "--shortcut":
                self.shortcut = 1
            elif opt == "--show":
                self.show = 1
            elif opt == "--storage-info":
                self.storage_info = value
            elif opt == "--start-draining":
                self.start_draining = value
            elif opt == "--start-time":
                self.start_time = value
            elif opt == "--status":
                self.status = 1
            elif opt == "--stop-draining":
                self.stop_draining = 1
            elif opt == "--online":
                self.stop_draining = 1
            elif opt == "--up":
                self.stop_draining = 1
                self.up = value
            elif opt == "--warm-restart":
                self.warm_restart = 1
                self.up = value
            elif opt == "--stop-time":
                self.stop_time = value
            elif opt == "--storage-groups":
                self.storage_groups = 1
            elif opt == "--summary":
                self.summary = 1
            elif opt == "--threaded":
                self.threaded_exfer = 1
            elif opt == "--threaded-impl":
                self.threaded_impl = value
            elif opt == "--time":
                self.time = value
            elif opt == "--timeout":
                self.alive_rcv_timeout = int(value)
            elif opt == "--title":
                self.title = value
            elif opt == "--title-gif":
                self.title_gif = value
            elif opt == "--update":
                self.update = 1
            elif opt == "--update-and-exit":
                self.update_and_exit = 1
            elif opt == "--url":
                self.url = value
            elif opt == "--encp":
                self.encp = 1
            elif opt == "--sg":
                self.sg = 1
            elif opt == "--usage-line":
                self.print_usage_line()
                sys.exit(0)
            elif opt == "--verbose":
                if value == "":
                    self.verbose = self.verbose | 1
                else:
                    self.verbose = self.verbose | int(value)
            elif opt == "--vol":
                self.vol = value
            elif opt == "--vols":
                self.vols = 1
            elif opt == "--VOL1OK":
                self.vol1ok = 1
            elif opt == "--web-host":
                self.web_host = value
            elif opt == "--subscribe":
                self.subscribe = 1
            elif opt == "--no-mail":
                self.no_mail = 1
            elif opt == "--dbHome":
                self.dbHome = value
            elif opt == "--jouHome":
                self.jouHome = value
            elif opt == "--sendto":    # for mailing
                self.sendto = string.split(value)
            elif opt == "--notify":    # for mailing (notification)
                self.notify = string.split(value)
            elif opt == "--skip-pnfs":  # for super_remove
                self.skip_pnfs = 1
            elif opt == "--dont-ask":  # for super_remove
                self.dont_ask = 1
            elif opt == "--recycle":   # for recycling a tape
                self.recycle = value
            elif opt == "--dont-try-this-at-home-erase":
                self.dont_try_this_at_home_erase = value
            elif opt == "--pnfs-is-automounted":
                self.pnfs_is_automounted = 1
            elif opt == "--ignore-storage-group":
                self.ignore_storage_group = value
            elif opt == "--clear-ignored-storage-group":
                self.clear_ignored_storage_group = value
            elif opt == "--clear-all-ignored-storage-groups":
                self.clear_all_ignored_storage_groups = 1
            elif opt == "--list-ignored-storage-groups":
                self.list_ignored_storage_groups = 1

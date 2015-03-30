#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

#system imports
import pprint
import math
import os
import socket
import string
import sys
import time
import stat
import types
import threading
import re
import resource
import traceback
import copy

#enstore imports
import Trace
import mover_client
import configuration_client
import enstore_constants
import enstore_functions2
import e_errors

#Set up paths to find our private copy of tcl/tk 8.3

#Get the environmental variables.
ENSTORE_DIR = os.environ.get("ENSTORE_DIR", None)
ENTV_DIR = os.environ.get("ENTV_DIR", None)
PYTHONLIB = os.environ.get("PYTHONLIB", None)
#IMAGE_DIR = None
_TKINTER_SO = None

def set_tcltk_library(tcltk_dir):
    global _TKINTER_SO

    #Determine the tcl directory.
    if not os.environ.has_key("TCL_LIBRARY"):
        for fname in os.listdir(tcltk_dir):
            if fname.startswith('tcl'):
                temp_dir_tcl = os.path.join(tcltk_dir, fname)
                os.environ["TCL_LIBRARY"] = temp_dir_tcl
                break
        else:
            return

    #Find the tk directory.
    if not os.environ.has_key("TK_LIBRARY"):
        for fname in os.listdir(tcltk_dir):
            if fname.startswith('tk'):
                temp_dir_tk = os.path.join(tcltk_dir, fname)
                os.environ["TK_LIBRARY"] = temp_dir_tk
                break
        else:
            return

    #Find the _tkinter.so directory.
    temp_dir_lib = os.path.join(tcltk_dir, sys.platform)
    if os.path.exists(temp_dir_lib):
        _TKINTER_SO = temp_dir_lib
        #Modify the search path for the _tkinter.so library.
        sys.path.insert(0, _TKINTER_SO)

"""
#Determine the expected location of the local copy of Tcl/Tk.
try:
    #Determine the expected location of the local copy of Tcl/Tk.
    if ENSTORE_DIR:
        TCLTK_DIR = os.path.join(ENSTORE_DIR, 'etc', 'TclTk')
        #IMAGE_DIR = os.path.join(ENSTORE_DIR, 'etc', 'Images')

        #Specify this python location first.  The local location should
        # be found first and this is used only as a last resort.
        temp_dir = os.path.join(PYTHONLIB, "lib-dynload")
        if(os.path.exists(temp_dir)):
            #Modify the search path for the _tkinter.so library.
            sys.path.insert(0, temp_dir)

        set_tcltk_library(TCLTK_DIR)

    elif ENTV_DIR:
        TCLTK_DIR = ENTV_DIR
        #IMAGE_DIR = os.path.join(ENTV_DIR, 'Images')

        set_tcltk_library(TCLTK_DIR)
except Tkinter.TclError:
    pass
"""

"""
#Make sure the environment has at least one copy of TCL/TK.
if not os.environ.get("TCL_LIBRARY", None):
    try:
        sys.stderr.write("Tcl library not found.\n")
    except IOError:
        pass
    sys.exit(1)
if not os.environ.get("TK_LIBRARY", None):
    try:
        sys.stderr.write("Tk library not found.\n")
    except IOError:
        pass
    sys.exit(1)

if not IMAGE_DIR:
    try:
        sys.stderr.write("IMAGE_DIR is not set.\n")
    except IOError:
        pass
    sys.exit(1)
"""

#print "_tkinter.so =", _TKINTER_SO
#print "TCL_LIBRARY =", os.environ.get('TCL_LIBRARY', "")
#print "TK_LIBRARY =", os.environ.get('TK_LIBRARY', "")

try:
    import Tkinter
    import tkFont  #Starts a thread not reported by threading.enumerate().
except ImportError:
    try:
        sys.stderr.write("%s\n" % (str(sys.exc_info()[1]),))
    except IOError:
        pass
    sys.exit(1)

#########################################################################

#A lock to allow only one thread at a time access the display class instance.
display_lock = threading.Lock()
startup_lock = threading.Lock()
thread_lock  = threading.Lock()
clients_lock = threading.Lock()

#Wrapper for logging when a thread grabs a lock.
def acquire(lock, lock_name="<generic_lock>", blocking=1):
    do_print = 0
    if Trace.print_levels.has_key(LOCK_LEVEL):
        do_print = 1

    if do_print:
        Trace.trace(LOCK_LEVEL,
                    "%s acquiring %s" % (threading.current_thread().getName(),
                                         lock_name))
    rtn = lock.acquire(blocking)

    if do_print:
        Trace.trace(LOCK_LEVEL,
                    "%s acquired %s" % (threading.current_thread().getName(),
                                        lock_name))
    return rtn

#Wrapper for logging when a thread gives up a lock.
def release(lock, lock_name="<generic_lock>"):
    do_print = 0
    if Trace.print_levels.has_key(LOCK_LEVEL):
        do_print = 1

    if do_print:
        Trace.trace(LOCK_LEVEL,
                    "%s releasing %s" % (threading.current_thread().getName(),
                                         lock_name))
    rtn = lock.release()

    if do_print:
        Trace.trace(LOCK_LEVEL,
                    "%s released %s" % (threading.current_thread().getName(),
                                        lock_name))
    return rtn

#########################################################################

CIRCULAR, LINEAR = range(2)

ANIMATE = 1
STILL = 0

CLIENT_COLOR = 1
LIBRARY_COLOR = 2

COUNTDOWN = 2
WAITING = 1
CONNECTED = 0

MMPC = 20.0     # Max Movers Per Column
MIPC = 20       # Max Items Per Column

REINIT_TIME = 3600000  #in milliseconds (1 hour)
ANIMATE_TIME = 42      #in milliseconds (~1/42nd of second)
UPDATE_TIME = 1000     #in milliseconds (1 second)
MESSAGES_TIME = 250    #in milliseconds (1/4th of second)
JOIN_TIME = 10000      #in milliseconds (10 seconds)
OFFLINE_REASON_TIME = 300000   #in milliseconds (5 minutes)
OFFLINE_REASON_INITIAL_TIME = 5000  #in milliseconds (5 seconds)
MOVER_DISPLAY_TIME = 5000  #in milliseconds (5 seconds)

YELLOW_WAIT_TIME_IN_SECONDS = 5.0  #status wait time before yellow background

status_request_threads = []
offline_reason_thread = None

#To prevent instantiating a slew of Inquisitors.
inqc_dict_cache = {}

st = 0
#pt = 0

#Cache the default configuration server client.
__csc = None
__cscs = {}

#Trace.message level for generating a messages file for replaying later.
MESSAGES_LEVEL = 10
#Trace.message level for fixing lock deadlocks.
LOCK_LEVEL = 9

#########################################################################

class Queue:
    def __init__(self):
        self.queue = {}
        self.lock = threading.Lock()

        #These are the timestamps that something was put in or taken out
        # of the queue.
        self.get_time = time.time()
        self.put_time = time.time()

    def get_queue_keys(self):
        self.lock.acquire()

        try:
            key_list = self.queue.keys()
        except KeyError:
            key_list = []
        except (KeyboardInterrupt, SystemExit):
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        self.lock.release()

        return key_list

    def len_queue(self, tid=None):
        self.lock.acquire()

        try:
            number = len(self.queue[tid])
        except KeyError:
            number = 0
        except (KeyboardInterrupt, SystemExit):
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        self.lock.release()

        return number

    def last_get_time(self):
        self.lock.acquire()

        get_time = copy.copy(self.get_time)

        self.lock.release()

        return get_time


    def clear_queue(self, tid = None):
        self.lock.acquire()

        try:
            del self.queue[tid][:]
        except KeyError:
            pass
        except (KeyboardInterrupt, SystemExit):
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        self.lock.release()

    def clear_queues(self):
        self.lock.acquire()

        try:
            for queue in self.queue.values():
                del queue[:]
        except KeyError:
            pass
        except (KeyboardInterrupt, SystemExit):
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        try:
            self.queue = {}
        except (KeyboardInterrupt, SystemExit):
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        self.lock.release()

    def put_queue(self, queue_item, tid=None):
        self.lock.acquire()

        try:
            if self.queue.get(tid, None) == None:
                self.queue[tid] = []

            self.queue[tid].append({'item' : queue_item, 'tid' : tid})

        except (KeyboardInterrupt, SystemExit):
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        self.put_time = time.time()  #Update the time this was done.

        self.lock.release()

    #Similar to put_queue, but puts the item at the beginning, not at the end.
    def insert_queue(self, queue_item, tid=None):
        self.lock.acquire()

        try:
            if self.queue.get(tid, None) == None:
                self.queue[tid] = []

            self.queue[tid].insert(0, {'item' : queue_item, 'tid' : tid})

        except (KeyboardInterrupt, SystemExit):
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        self.put_time = time.time()  #Update the time this was done.

        self.lock.release()

    #Wait for an inserted queue item, at the beginning of the queue, to be
    # processed before continueing.  Do not call this function from the
    # window's mainloop thread.
    def wait_for_queue_item(self, queue_item, tid=None):
        now=time.time()
        put_time = None
        while time.time() < now + 10:  #Give it 10 seconds.
             self.lock.acquire()

             #Get this value while we have the lock on the first pass.
             if put_time == None:
                 put_time = self.put_time

             try:
                 temp = self.queue[tid][0]
             except (KeyError, IndexError):
                 #The queue is now empty.

                 if self.get_time > put_time:
                     #If we get here, we know the display thread has started
                     # processing at least one additional queued item.
                     self.lock.release()
                     return
                 else:
                     #Insert a dummy message to make sure that we are done
                     # processing the update we care about.
                     self.queue[tid].insert(0, {'item' : "", 'tid' : tid})

             except (KeyboardInterrupt, SystemExit):
                 self.lock.release()
                 raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
             except:
                 self.lock.release()
                 raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

             #Grab a copy of this before releasing the lock.
             get_time = self.get_time

             self.lock.release()
             if temp['item'] == queue_item:
                 #The item is still in the queue.
                 time.sleep(.1)
                 continue
             elif get_time <= put_time:
                 #If we get here, we know the inserted item has been
                 # pulled from the queue, but it is not done processing.
                 time.sleep(.1)
                 continue
             else:
                 #The item is done getting processed, this thread can continue.
                 return

    def get_queue(self, tid=None):
        self.lock.acquire()

        try:
            temp = self.queue[tid][0]
            del self.queue[tid][0]
        except (KeyError, IndexError):
            temp = {'item' : None}
        except (KeyboardInterrupt, SystemExit):
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        self.get_time = time.time()  #Update the time this was done.

        self.lock.release()

        return temp['item']

#These are the queue classes.  The message_queue contains items received by
# entv and need to be processed to update the display.  The request_queue
# is the list of Enstore servers that the display/main thread has decided
# that the netork thread needs to query their status.
message_queue = Queue()
request_queue = Queue()

#Older python versions don't have a sum() function.  Define one for them.
#if sys.version_info < (2, 3, 0):
#    def local_sum(sequence, start = 0):
#        import operator
#        return reduce(operator.add, sequence, start)
#
#    sum = local_sum

def scale_to_display(x, y, w, h):
    """Convert coordinates on unit circle to Tk display coordinates for
    a window of size w, h"""
    return int((x+1)*(w/2)), int((1-y)*(h/2))

def HMS(s):
    """Convert the number of seconds to H:M:S"""
    h = s / 3600
    s = s - (h*3600)
    m = s / 60
    s = s - (m*60)
    return "%02d:%02d:%02d" % (h, m, s)

_font_cache = {}

#def get_font(height_wanted, family='arial', fit_string="", width_wanted=0):
def get_font(height_wanted, family='Helvetica', fit_string="", width_wanted=0):
#def get_font(height_wanted, family='Courier', fit_string="", width_wanted=0):

    height_wanted = int(height_wanted)

    f = _font_cache.get((height_wanted, width_wanted, len(fit_string), family))
    if f:
        return f
        #Why do this?  MWZ
        if width_wanted and f.measure(fit_string) > width_wanted:
            pass
        else:
            return f

    size = height_wanted
    while size >= 0:
        ### Unfortuneatly, it has been observed that, either the Font
        ### constructor or the metrics() is able to hang.  Thus, the use
        ### of alarm() in entv.py to mitigate this behavior.
        f = tkFont.Font(size=size, family=family)
        metrics = f.metrics()  #f.metrics returns something like:
        # {'ascent': 11, 'linespace': 15, 'descent': 4, 'fixed': 1}
        height = metrics['ascent']
        width = f.measure(fit_string)
        if height <= height_wanted and width_wanted and width <= width_wanted:
            #good, we found it
            break
        elif height < height_wanted and not width_wanted:
            break
        else:
            size = size - 1 #Try a little bit smaller...

    _font_cache[(height_wanted, width_wanted, len(fit_string), family)] = f
    return f

def fit_string(font, the_string, width_wanted):

    #Get the list in index ranges to check.
    search = range(len(the_string) + 1)
    search.reverse()

    #Start at the end of the string and move toward the beginning.  Return
    # only the portion of the string that will fit.
    for i in search:
        width = font.measure(the_string[:i])
        if width <= width_wanted:
            return the_string[:i]

    return ""


def rgbtohex(r,g,b):
    r=hex(r)[2:]
    g=hex(g)[2:]
    b=hex(b)[2:]
    if len(r)==1:
        r='0'+r
    if len(g)==1:
        g='0'+g
    if len(b)==1:
        b='0'+b
    return "#"+r+g+b

def hextorgb(hexcolor):
    if type(hexcolor) != types.StringType:
        return 0, 0, 0

    #make sure the string is long enough
    if len(hexcolor) < 7:
        hexcolor = hexcolor + "0"*(7-len(hexcolor))

    red = int("0x" + hexcolor[1:3], 16)
    green = int("0x" + hexcolor[3:5], 16)
    blue = int("0x" + hexcolor[5:7], 16)

    return red, green, blue

def invert_color(hexcolor):
    red, green, blue = hextorgb(hexcolor)

    return rgbtohex(255 - red, 255 - green, 255 - blue)

color_dict = {
    #client colors
    'client_wait_color' :   rgbtohex(150, 150, 150),  # grey
    'client_active_color' : rgbtohex(0, 255, 0), # green
    'client_outline_color' : rgbtohex(0, 0, 0), # black
    'client_font_color' : rgbtohex(0, 0, 0), # black
    #mover colors
    'mover_color':          rgbtohex(0, 0, 0), # black
    'mover_error_color':    rgbtohex(255, 0, 0), # red
    'mover_unknown_color':    rgbtohex(255, 255, 0), # yellow
    'mover_offline_color':  rgbtohex(169, 169, 169), # grey
    'mover_stable_color':   rgbtohex(0, 0, 0), # black
    'mover_label_color':    rgbtohex(255, 50, 50), # maroon
    'percent_color':        rgbtohex(0, 255, 0), # green
    'progress_bar_color':   rgbtohex(255, 255, 0), # yellow
    'progress_bg_color':    rgbtohex(255, 0, 255), # magenta
    'progress_alt_bar_color':rgbtohex(255, 192, 0), # orange
    'state_stable_color':   rgbtohex(255, 192, 0), # orange
    'state_idle_color':     rgbtohex(191, 239, 255), # lightblue
    'state_error_color':    rgbtohex(0, 0, 0), # black
    'state_offline_color':  rgbtohex(0, 0, 0), # black
    'state_unknown_color':    rgbtohex(0, 0, 0), # black
    'state_active_color':    rgbtohex(0, 255, 0), # green
    'timer_color':          rgbtohex(255, 255, 255), # white
    'timer_longtime_color': rgbtohex(255, 0, 0), # red
    'timer_unknown_color': rgbtohex(0, 0, 0), # black
    #volume colors
    'tape_stable_color':    rgbtohex(0, 165, 255), # (royal?) blue
    'label_stable_color':   rgbtohex(255, 255, 255), # white (tape)
}


def colors(what_color): # function that controls colors
    return color_dict.get(what_color, rgbtohex(0,0,0))

def endswith(s1,s2):
    return s1[-len(s2):] == s2

def normalize_name(hostname):
    ## Clean off any leading or trailing garbage
    while hostname and hostname[0] not in string.letters+string.digits:
        hostname = hostname[1:]
    while hostname and hostname[-1] not in string.letters+string.digits:
        hostname = hostname[:-1]

    ## Empty string?
    if not hostname:
        return '???'

    ## If it's numeric, try to look it up
    if hostname[0] in string.digits:
        try:
            hostname = socket.gethostbyaddr(hostname)[0]
        except (socket.error, IndexError):
            Trace.trace(1, "Can't resolve address %s." % (hostname,))

    ## If it ends with .fnal.gov, cut that part out
    if endswith(hostname, '.fnal.gov'):
        hostname = hostname[:-9]
    return hostname

"""
_image_cache = {} #key is filename, value is (modtime, img)

def find_image(name):
    #Look in IMAGE_DIR for a file of the given name.  Cache already loaded
    # image, but check modification time for file changes
    img_mtime, img = _image_cache.get(name, (0, None))
    filename = os.path.join(IMAGE_DIR, name)
    if img: #already cached, is it still valid?
        try:
            statinfo = os.stat(filename)
            file_mtime = statinfo[stat.ST_MTIME]
            if file_mtime > img_mtime: #need to reload
                del _image_cache[name]
                img = None
        except OSError:
            del _image_cache[name]
            img = None
    if not img: # Need to load it
        try:
            statinfo = os.stat(filename)
            file_mtime = statinfo[stat.ST_MTIME]
            img = Tkinter.PhotoImage(file=filename)
            _image_cache[name] = file_mtime, img #keep track of image and modification time
        except OSError:
            img = None
    return img
"""

class XY:
    def __init__(self, x, y):
        self.x = x
        self.y = y

def split_geometry(geometry):
    if not geometry:
        return (None, None, None, None)

    window_width = int(re.search("^[0-9]+", geometry).group(0))
    window_height = re.search("[x][0-9]+", geometry).group(0)
    window_height = int(window_height.replace("x", " "))
    x_position = re.search("[+][-]{0,1}[0-9]+[+]", geometry).group(0)
    x_position = int(x_position.replace("+", ""))
    y_position = re.search("[+][-]{0,1}[0-9]+$", geometry).group(0)
    y_position = int(y_position.replace("+", ""))

    return window_width, window_height, x_position, y_position

#########################################################################
# These two functions can only be called from one location in the code
# due to use of the global variables.

last_animate = time.time()
def animate_time():
    global st
    global last_animate

    sum_time = sum(resource.getrusage(resource.RUSAGE_SELF)[:2])
    cpu = min(1000, int(1000 * (sum_time - st)) * 2)
    st = sum_time

    now = time.time()
    rtn = max(1, int(ANIMATE_TIME + cpu - ((now - last_animate) * 1000)))
    last_animate = now

    return rtn

last_process = time.time()
def process_time():
    global last_process

    now = time.time()

    #Find the fractional part of the current time and obtain the current
    # multiple of MESSAGES_TIME.
    part, whole = math.modf((math.modf(now)[0] * 1000) / MESSAGES_TIME)
    #Taking just fractional part, determine how many milliseconds it is
    # to the next multiple of MESSAGES_TIME.
    rtn = max(1, int((MESSAGES_TIME - (part * MESSAGES_TIME))))

    last_process = now

    return rtn

#########################################################################

#Get the list of all known Enstore systems from the default configuration
# servers 'known_config_servers' section.
def get_all_systems(csc=None):

    #Get the list of all config servers and remove the 'status' element.
    if csc:
        use_csc = csc
    else:
        #Be patient with lost configuration_servers.
        use_csc = None
        sleep_time = 1 #Seconds.
        while use_csc == None:
            #The defaults are 3 seconds and 3 tries.
            use_csc = get_csc()
            if use_csc == None:
                Trace.trace(0,
                            "Failed to find configuration server.  Retrying.")
                time.sleep(sleep_time)
                sleep_time = sleep_time * 2
                sleep_time = min(sleep_time, 60) #max sixty seconds
    known_config_servers = copy.copy(use_csc.get('known_config_servers', {}))
    if known_config_servers['status'][0] == e_errors.OK:
        del known_config_servers['status']
    else:
        return None

    #Create the configuration client objects.
    config_servers = {}
    for system_name, csc_addr in known_config_servers.items():
        new_csc = configuration_client.ConfigurationClient(csc_addr)
        config_servers[system_name] = new_csc

    #Special section for test systems that are not in their own
    # config file's 'known_config_servers' section.
    config_host = enstore_functions2.default_host()
    for system_name, csc_addr in known_config_servers.items():
        if config_host == csc_addr[0]:
            break
        elif csc and csc.server_address == csc_addr:
            break
    else:
        i = 0
        while i < 3:
            try:
                ip = socket.gethostbyname(config_host)
                addr_info = socket.gethostbyaddr(ip)
                if addr_info[1] != []:
                    short_name = addr_info[1][0]
                else:
                    short_name = addr_info[0].split(".")[0]

                #Create the new configuration client object and put it in
                # the list.
                address = (ip, enstore_functions2.default_port())
                new_csc = configuration_client.ConfigurationClient(address)
                new_csc.new_config_obj.enable_caching()
                config_servers[short_name] = new_csc

                break
            except socket.error:
                if csc:
                    break
                else:
                    time.sleep(1)
                    i = i + 1
                    continue
        else:
            if not csc:
                #If we were passed a csc, then we don't need to freak out
                # that the default has failed.  We got far enough to decide
                # on another configuration value.
                try:
                    sys.stderr.write("Unable to obtain ip information.  Aborting.\n")
                    sys.stderr.flush()
                except IOError:
                    pass
                sys.exit(1)

    #Special section to add the a passed in csc to the config_servers list.
    for system_name, current_csc in config_servers.items():
        if csc.server_address == current_csc.server_address:
            break
    else:
        config_servers[csc.server_address[0]] = csc

    return config_servers

#Get the ConfigurationClient object for the default enstore system.
def get_csc(system_name = None, timeout = 3, retry = 3):
    #Normally, system name is an Enstore system name key from the
    # known_config_servers section of the configuration file.  It also can
    # be a hostname of the configuration server.

    global __csc
    global __cscs

    #If we have a configuration client cached, return that.
    if not system_name and __csc:
        return __csc
    elif system_name in __cscs.keys():
        return __cscs[system_name]

    #Get the defaults from the environmental variables.
    default_config_host = enstore_functions2.default_host()
    default_config_port = enstore_functions2.default_port()
    #Specifiy the order of things to try.
    try_system_name_order = [default_config_host,
                             system_name]

    for use_config_host in try_system_name_order:

        # get a configuration server
        try:
            csc = configuration_client.ConfigurationClient(
                (use_config_host, default_config_port))
        except (socket.error,), msg:
            Trace.trace(0, "Error contacting configuration server: %s\n" %
                        msg.args[1],
                        out_fp=sys.stderr)
            return None

        #Enable cache of the configuration file information retreieved from
        # the configuration server.
        csc.new_config_obj.enable_caching()

        #Cache the entire configuration.
        rtn_ticket = csc.dump_and_save(timeout = timeout, retry = retry)

        #Cache the default configuration server.
        if e_errors.is_ok(rtn_ticket):
            if system_name == use_config_host:
                #Default configuratoin server is not responding, but we
                # found the configuration server for the requested Enstore
                # system.
                __cscs[system_name] = csc
                return __cscs[system_name]
            elif system_name:
                #We found the default configuration server, now find the
                # requested configuration server.
                config_servers = get_all_systems(csc)
                for sys_name, current_csc in config_servers.items():
                    #Lets save all known configuration servers.  There are not
                    # that many and we may need to know this at some point
                    # in the future.
                    __cscs[sys_name] = current_csc

                #Return the one we are looking for.
                for sys_name, current_csc in config_servers.items():
                    if sys_name == system_name:
                        return current_csc
                    elif current_csc.server_address[0] == system_name:
                        return current_csc
                else:
                    message = "Unable to find Enstore system for %s in " \
                              "configuration." % (system_name,)
                    Trace.trace(0, message, out_fp=sys.stderr)
                    return None
            elif not system_name:
                __csc = csc
                return __csc

            #The configuration server is down.
    else:
        #Totally unable to find a configuration server.
        if system_name:
            message = "Unable to find Enstore system for %s." % (system_name,)
        else:
            message = "Unable to find Enstore system."
        Trace.trace(0, message, out_fp=sys.stderr)
        return None

#########################################################################
# Most of the functions will be handled by the mover.
# its  functions include:
#     draw_mover() - draws the mover background and the label
#     draw_state() - draws the current state image or text
#     draw_timer() - draws the time-in-state timer
#     draw_volume() - draw the current volume at mover
#     draw_progress() - indicates progress of each data transfer;
#                                     is it almost complete?
#     draw() - draws most features on the movers
#     update_state() - as the state of the movers change, display
#                                  for state will be updated
#     update_timer() - timer associated w/state, will update for each state
#     update_rate() - updates the current rate of active transfer
#     load_tape() - tape gets loaded onto mover:
#                        gray indicates robot recognizes tape and loaded it
#                        orange indicates when mover actually recognizes tape
#     unload_tape() - will unload tape to side of each mover, ready for
#                                 robot to remove f/screen
#     transfer_rate() - rate at which transfer being sent; calculates a rate
#     undraw() - undraws the features fromthe movers
#     position() - calculates the position for each mover
#     reposition() - reposition each feature after screen has been moved
#
#########################################################################
class Mover:
    def __init__(self, name, display, index=0, column=0, row=0, movers=1):
        self.display       = display
        self.name          = name
        self.state         = None
        self.draining = False
        self.index         = index  #Used for CIRCULAR display.
        self.column        = column #Movers may be laid out in multiple columns
        self.row           = row    #Row for column display.
        #Don't save the number of movers as a member.  This value can
        # change as movers are configured and added to the system.

        #Classes that are used.
        self.volume        = None

        #Set geometry of mover.
        self.resize() #Even though this is the initial size, still works.
        self.x, self.y  = self.position(movers)

        #These 4 pieces make up the progress gauge display
        self.progress_bar             = None
        self.progress_alt_bar         = None
        self.progress_bar_bg          = None
        self.progress_percent_display = None
        #These 2 pieces make up the buffer display
        self.buffer_bar               = None
        self.buffer_bar_bg            = None
        # This is the numeric value.  "None" means don't show the progress bar.
        self.percent_done = None
        self.alt_percent_done = None
        self.buffer_size = None

        #These are other display items.
        self.outline            = None
        self.label              = None
        self.timer_display      = None
        self.state_display      = None
        self.state_display_2    = None  #For multiple part icons.
        self.volume_display     = None
        self.volume_bg_display  = None
        self.rate_display       = None
        self.offline_reason_display = None

        # Anything that deals with time
        self.b0                 = 0
        now                     = time.time()
        self.rate               = None  #In bytes per second.
        self.rate_string        = None  #"0 MB/S"
        self.t0                 = 0.0
        self.timer_seconds      = 0
        self.timer_started      = now
        self.timer_string       = '00:00:00'
        self.timer_id           = None

        # Misc. variables.
        self.offline_reason     = None

        #Find out information about the mover.
        try:
            system_name = self.name.split("@")[1]
            csc = get_csc(system_name)
            minfo = csc.get(self.name.split("@")[0] + ".mover")
            #64MB is the default listed in mover.py.
            self.max_buffer = long(minfo.get('max_buffer', 67108864))
            self.library = minfo.get('library', "Unknown")
        except (AttributeError, KeyError):
            self.max_buffer = 67108864L
            self.library = "Unknown"

        self.update_state("Unknown")
        self.animate_timer() #Start the automatic timer updates.

        #Draw the mover.
        self.draw()

    #########################################################################

    def animate_timer(self):

        self.update_timer(time.time())

        #If the mover state is unknown for more than 5 seconds, we need
        # to change the mover's color (if not already done so).
        if self.state in ['Unknown'] and not self.offline_reason and \
               self.state_color != colors('state_unknown_color'):
            if time.time() - self.timer_started >= YELLOW_WAIT_TIME_IN_SECONDS:
                #By passing the empty string, we will change the client
                # color without scheduling an inquisitor request.
                self.update_offline_reason("")

        #Carefull.  As long as draw_timer() gets called after animate_timer
        # in __init__() we are okay.
        #if self.timer_id:
        #    self.display.after_cancel(self.timer_id)
        self.timer_id = self.display.after(UPDATE_TIME, self.animate_timer)

    #########################################################################

    def draw_mover(self):
        x, y                    = self.x, self.y

        #Display the mover rectangle.
        if self.outline:
            self.display.coords(self.outline, x, y,
                                x+self.width, y+self.height)
            self.display.itemconfigure(self.outline, fill=self.mover_color,
                                       outline=self.library_color)
        else:
            self.outline = self.display.create_rectangle(x, y, x+self.width,
                                                         y+self.height,
                                                         fill=self.mover_color,
                                                    outline=self.library_color,
                                                         width=2.0)
        #Display the mover name label.
        if self.label:
            self.display.coords(self.label, x + self.label_offset.x,
                                y + self.label_offset.y)
            self.display.itemconfigure(self.label, fill = self.label_color)
        else:
            self.label = self.display.create_text(x+self.label_offset.x,
                                                  y+self.label_offset.y,
                                               text = self.name.split("@")[0],
                                                  anchor = Tkinter.E,
                                                  font = self.font,
                                                  fill = self.label_color)

    #Used by draw_state().
    def draw_positioning_icon(self):
        x, y = self.x + self.img_offset.x, self.y + self.img_offset.y

        #Path of the first "fast forward" line.
        path = (
            #Top point.
            x,
            y + (self.vol_height * 0.25),
            #Middle point.
            x + (self.vol_height * 0.25),
            y + (self.vol_height * 0.50),
            #Bottom point.
            x,
            y + (self.vol_height * 0.75)
            )
        # path 2 is the same as one just moved to the right three pixels.
        path2 = (
            #Top point.
            path[0] + 3,
            path[1],
            #Middle point.
            path[2] + 3,
            path[3],
            #Bottom point.
            path[4] + 3,
            path[5]
            )

        if self.state == self.old_state:
            self.display.coords(self.state_display, path)
            self.display.coords(self.state_display_2, path2)
        else:
            if self.state_display:
                self.display.delete(self.state_display)
                self.state_display = None
            if self.state_display_2:
                self.display.delete(self.state_display_2)
                self.state_display_2 = None

            self.state_display = self.display.create_line(
                path, width=1, smooth=0, joinstyle = 'miter',
                fill=color_dict['state_active_color'])

            self.state_display_2 = self.display.create_line(
                path2, width=1, smooth=0, joinstyle = 'miter',
                fill=color_dict['state_active_color'])


    #Used by draw_state().
    def draw_active_icon(self):
        if self.rate == None:
            return

        """
        if self.rate < 0:
            #Placeholder for someday having different icons for reads.
            pass
        else:
            #placeholder for someday having different icons for writes.
        """

        x, y = self.x + self.img_offset.x, self.y + self.img_offset.y

        #Circle bounding box.
        coords = (
            #Upper left.
            x,
            y + (self.vol_height * 0.25),
            #Lower right.
            x + (self.vol_height * 0.5),
            y+ (self.vol_height * 0.75)
            )

        #Triangle points.
        coords2 = (
            #Top point.
            x + (self.vol_height * 0.5),
            y + (self.vol_height * 0.25),
            #Middle point.
            x + (self.vol_height * 0.75),
            y + (self.vol_height * 0.5),
            #Bottom point.
            x + (self.vol_height * 0.5),
            y + (self.vol_height * 0.75)
            )

        if self.state == self.old_state:
            self.display.coords(self.state_display, coords)
            self.display.coords(self.state_display_2, coords2)
        else:
            if self.state_display:
                self.display.delete(self.state_display)
                self.state_display = None
            if self.state_display_2:
                self.display.delete(self.state_display_2)
                self.state_display_2 = None

            self.state_display = self.display.create_oval(
                coords, fill=color_dict['state_active_color'])

            self.state_display_2 = self.display.create_polygon(
                coords2, fill=color_dict['state_active_color'])

    #Used by draw_state().
    def draw_drain_icon(self):

        """
        if self.rate < 0:
            #Placeholder for someday having different icons for reads.
            pass
        else:
            #placeholder for someday having different icons for writes.
        """

        x, y = self.x + self.img_offset.x, self.y + self.img_offset.y

        #Circle bounding box.
        coords = (
            #Upper left.
            x,
            y + (self.vol_height * 0.25),
            #Lower right.
            x + (self.vol_height * 0.5),
            y+ (self.vol_height * 0.75)
            )

        #Triangle points.
        coords2 = (
            #Rigth point.
            x + (self.vol_height * 0.75),
            y + (self.vol_height * 0.75),
            #Middle point.
            x + (self.vol_height * 0.25),
            y + (self.vol_height * 1.25),
            #Bottom point.
            x - (self.vol_height * 0.25),
            y + (self.vol_height * 0.75)
            )

        if self.state == self.old_state:
            self.display.coords(self.state_display, coords)
            self.display.coords(self.state_display_2, coords2)
        else:
            if self.state_display:
                self.display.delete(self.state_display)
                self.state_display = None
            if self.state_display_2:
                self.display.delete(self.state_display_2)
                self.state_display_2 = None

            self.state_display = self.display.create_oval(
                coords, fill=color_dict['state_active_color'])

            self.state_display_2 = self.display.create_polygon(
                coords2, fill=color_dict['state_active_color'])


    def draw_state(self):
        x, y = self.x + self.state_offset.x, self.y + self.state_offset.y

        #For small window sizes, the rate display is largely more important.
        # This is simalar to the percent display size drawing restrictions.
        #if self.state == "ACTIVE" and self.display.width <= 470:
        if self.state == "ACTIVE" and self.width < 100:
            self.undraw_state()
            return
        #The rate should only be drawn when in ACTIVE state.  Force an
        # undraw of it if it is in a different state.
        if self.state != "ACTIVE":
            self.undraw_rate()

        #Display the current state.
        #img          = find_image(self.state + '.gif')
        #img = None
        if self.state_display:
            if self.state in ("SEEK"):
                self.draw_positioning_icon()
            elif self.state in ("ACTIVE"):
                self.draw_active_icon()

            #if current state is in text and the new state is in text.
            else:
                if self.display.type(self.state_display) == "text":
                    self.display.coords(self.state_display, x, y)
                    self.display.itemconfigure(self.state_display,
                                           text=fit_string(self.font,
                                                           self.state,
                                                           self.state_width),
                                           anchor=Tkinter.CENTER,
                                           fill=self.state_color)
                else:
                    self.display.delete(self.state_display)
                    if self.state_display_2:
                        self.display.delete(self.state_display_2)
                        self.state_display_2 = None
                    self.state_display = self.display.create_text(x, y,
                        font = self.font,
                        text=fit_string(self.font, self.state, self.state_width),
                        fill=self.state_color, anchor=Tkinter.CENTER)

        #No current state display.
        else:
            if self.state in ("SEEK"):
                self.draw_positioning_icon()
            elif self.state in ("ACTIVE"):
                self.draw_active_icon()
            else:
                self.state_display = self.display.create_text(x, y,
                    font = self.font,
                    text=fit_string(self.font, self.state, self.state_width),
                    fill=self.state_color, anchor=Tkinter.CENTER)

    def draw_timer(self):
        if self.timer_id == None:
            self.timer_id = self.display.after(UPDATE_TIME, self.animate_timer)

        if self.timer_display:
            self.display.itemconfigure(self.timer_display,
                                       text = self.timer_string,
                                       fill = self.timer_color)
        else:
            self.timer_display = self.display.create_text(
                self.x + self.timer_offset.x, self.y + self.timer_offset.y,
                text = self.timer_string, fill = self.timer_color,
                font = self.font, anchor = Tkinter.SE)

    def draw_volume(self):
        x, y = self.x + self.volume_offset.x, self.y + self.volume_offset.y

        #Display the volume.
        if self.volume:

            #If necessary define the font to use.
            if not self.volume_font:
                self.volume_font = get_font(self.vol_height, 'arial',
                                            fit_string=self.volume,
                                            width_wanted=self.vol_width)

            #Draw the volume background.
            if self.volume_bg_display:
                self.display.coords(
                    self.volume_bg_display,
                    x, y, x + self.vol_width, y + self.vol_height)
            else:
                self.volume_bg_display = self.display.create_rectangle(
                    x, y, x + self.vol_width, y + self.vol_height,
                    fill = self.volume_bg_color,)

            #Draw the volume label.
            if self.volume_display:
                self.display.coords(self.volume_display,
                                    x + (self.vol_width / 2.0) + 1,
                                    y + (self.vol_height / 2.0) + 1)
            else:
                self.volume_display = self.display.create_text(
                   x + (self.vol_width / 2.0) + 1,
                   y + (self.vol_height / 2.0) + 1,
                   text = fit_string(self.volume_font,
                                     self.volume, self.vol_width),
                   fill = self.volume_font_color,
                   font = self.volume_font, width = self.vol_width,)

    def draw_background_progress(self):  #, percent_done, alt_percent_done):
        if self.percent_done == None and self.alt_percent_done == None:
            return

        # Redraw the old progress bg gauge.
        if self.progress_bar_bg:
            new_location = self.get_bar_position(100)
            self.display.coords(self.progress_bar_bg,
                                new_location[0], new_location[1],
                                new_location[2], new_location[3])
        else:
            new_location = self.get_bar_position(100)
            #If both are not to be drawn, then this will draw the correct one.
            self.progress_bar_bg = self.display.create_rectangle(
                new_location[0], new_location[1],
                new_location[2], new_location[3],
                fill = colors('progress_bg_color'), outline = "")

    def draw_network_progress(self):  #, percent_done):
        if self.percent_done == None:
            return

        # Redraw the old progress gauge.
        if self.progress_bar:
            new_location = self.get_bar_position(self.percent_done)
            self.display.coords(self.progress_bar,
                                new_location[0], new_location[1],
                                new_location[2], new_location[3])
        else:
            new_location = self.get_bar_position(self.percent_done)
            #If both are not to be drawn, then this will draw the correct one.
            self.progress_bar = self.display.create_rectangle(
                new_location[0], new_location[1],
                new_location[2], new_location[3],
                fill = colors('progress_bar_color'), outline = "")

        #Draw the percent of transfer done next to progress bar.
        if self.width > 100:
            if self.progress_percent_display:
                self.display.itemconfigure(self.progress_percent_display,
                                           text = str(self.percent_done)+"%")
                self.display.lift(self.progress_percent_display)
            else:
                self.progress_percent_display =  self.display.create_text(
                    self.x + self.percent_disp_offset.x,
                    self.y + self.percent_disp_offset.y,
                    text = str(self.percent_done)+"%",
                    fill = colors('percent_color'), font = self.font,
                    anchor = Tkinter.SW)

    def draw_media_progress(self):
        if self.alt_percent_done == None:
            return

        # Redraw the old alternate progress gauge.
        if self.progress_alt_bar:
            new_location = self.get_bar_position(self.alt_percent_done)
            self.display.coords(self.progress_alt_bar,
                                new_location[0], new_location[1],
                                new_location[2], new_location[3])
        else:
            new_location = self.get_bar_position(self.alt_percent_done)
            #If both are not to be drawn, then this will draw the correct one.
            self.progress_alt_bar = self.display.create_rectangle(
                new_location[0], new_location[1],
                new_location[2], new_location[3],
                fill = colors('progress_alt_bar_color'), outline = "")

    def draw_progress(self): #, percent_done, alt_percent_done):
        #(Re)draw the old progress bg gauge.
        self.draw_background_progress()  #percent_done, alt_percent_done)
        #(Re)draw the two progress bars if necessary.
        self.draw_network_progress()  #percent_done)
        self.draw_media_progress()  #alt_percent_done)

        #Raise the correct progress bar to the top of the window stack.
        if self.rate > 0:  #write transfer, make media bar on top.
            try:
                self.display.tag_raise(self.progress_alt_bar,
                                       self.progress_bar)
            except Tkinter.TclError:
                pass #We get here if alt percentage is still None.
        else:              #read transfer, make network bar on top.
            try:
                self.display.tag_raise(self.progress_bar,
                                       self.progress_alt_bar)
            except Tkinter.TclError:
                pass #We get here if percentage is still None.

    def get_bar_position(self, percent):
        offset = (self.bar_width*percent/100.0)
        bar = (self.x + self.progress_bar_offset1.x,
               self.y + self.progress_bar_offset1.y,
               self.x + self.progress_bar_offset2.x + offset,
               self.y + self.progress_bar_offset2.y)
        return bar

    def draw_background_buffer(self):
        if self.buffer_size == None:
            #Don't display the buffer bar.
            #self.undraw_buffer()
            return

        if self.buffer_bar_bg:
            self.display.coords(self.buffer_bar_bg,
                                self.get_buffer_position(self.max_buffer))
        else:
            self.buffer_bar_bg = self.display.create_rectangle(
                self.get_buffer_position(100),
                fill = colors('progress_bg_color'), outline = "")

    def draw_bar_buffer(self):
        if self.buffer_size == None:
            #Don't display the buffer bar.
            #self.undraw_buffer()
            return

        if self.buffer_bar:
            self.display.coords(self.buffer_bar,
                                self.get_buffer_position(self.buffer_size))
        else:
            self.buffer_bar = self.display.create_rectangle(
                self.get_buffer_position(self.buffer_size),
                fill = colors('progress_bar_color'), outline = "")

    def draw_buffer(self):  #, buffer_size):
        self.draw_background_buffer()
        self.draw_bar_buffer()

    def get_buffer_position(self, bufsize):
        #offset = (self.bar_width * bufsize / 100.0)
        offset = (self.bar_width * bufsize / self.max_buffer)
        bar = (self.x + self.buffer_bar_offset1.x,
               self.y + self.buffer_bar_offset1.y,
               self.x + self.buffer_bar_offset2.x + offset,
               self.y + self.buffer_bar_offset2.y)

        return bar

    def draw_buffer_bar(self, percent, color):
        offset = (self.bar_width*percent/100.0)
        bar = self.display.create_rectangle(
            self.x + self.buffer_bar_offset1.x,
            self.y + self.buffer_bar_offset1.y,
            self.x + self.buffer_bar_offset2.x + offset,
            self.y + self.buffer_bar_offset2.y,
            fill=color,
            outline="")
        return bar

    def draw_rate(self):
        #On the off chance that the state still exists on the display
        # when it should not.
        if self.rate_string and self.state != "ACTIVE":
            self.undraw_state()

        if self.draining:
            self.draw_drain_icon()
        #The rate is usualy draw when the mover is in active state.
        # However, it can go into draining state while a transfer is in
        # progress.  The display of the draining state and rate can not
        # occur at the same time.  Give precdence to the state.

        if self.rate_display:
            self.display.itemconfigure(
                self.rate_display,
                text=fit_string(self.font, self.rate_string, self.state_width))
        elif self.rate != None:
            self.rate_display = self.display.create_text(
                self.x + self.rate_offset.x, self.y + self.rate_offset.y,
                #Use the state width since this is displayed in that space.
                text=fit_string(self.font, self.rate_string, self.state_width),
                fill = colors('percent_color'),
                anchor = Tkinter.NE, font = self.font)

    def draw_offline_reason(self):
        if not self.offline_reason:
            if self.offline_reason_display:
                self.undraw_offline_reason()
            return

        #Prepare the text string.  Note: the reason for the "W" is to put in
        # a characters width between the two pieces of text.  Also, protect
        # against the window being so narrow that only the timer string
        # will fit.
        offline_reason_width = \
                max(0, self.width - self.font.measure(self.timer_string + "W"))
        text = fit_string(self.font, self.offline_reason, offline_reason_width)

        if self.offline_reason_display:
            self.display.itemconfigure(
                self.offline_reason_display,
                text=text)
        else:
            self.offline_reason_display = self.display.create_text(
                self.x + self.offline_reason_offset.x,
                self.y + self.offline_reason_offset.y,
                text=text,
                fill = self.state_color,
                anchor = Tkinter.SW, font = self.font)



    def draw(self):

        self.draw_mover()

        self.draw_state()

        self.draw_timer()

        self.draw_progress() #Display the progress bar and percent done.

        self.draw_buffer()

        self.draw_volume()

        self.draw_rate()

        self.draw_offline_reason()

    #########################################################################

    def undraw_mover(self):
        try:
            self.display.delete(self.label)
            self.label = None
        except Tkinter.TclError:
            pass

        try:
            self.display.delete(self.outline)
            self.outline = None
        except Tkinter.TclError:
            pass

    def undraw_timer(self):
        if self.timer_id:
            self.display.after_cancel(self.timer_id)
            self.timer_id = None

        try:
            self.display.delete(self.timer_display)
            self.timer_display = None
        except Tkinter.TclError:
            pass

    def undraw_state(self):
        try:
            self.display.delete(self.state_display)
            self.state_display = None
        except Tkinter.TclError:
            pass

        try:
            self.display.delete(self.state_display_2)
            self.state_display_2 = None
        except Tkinter.TclError:
            pass

    def undraw_progress(self):
        try:
            self.display.delete(self.progress_alt_bar)
            self.progress_alt_bar = None
        except Tkinter.TclError:
            pass

        try:
            self.display.delete(self.progress_bar)
            self.progress_bar = None
        except Tkinter.TclError:
            pass

        try:
            self.display.delete(self.progress_percent_display)
            self.progress_percent_display = None
        except Tkinter.TclError:
            pass

        try:
            self.display.delete(self.progress_bar_bg)
            self.progress_bar_bg = None
        except Tkinter.TclError:
            pass

    def undraw_buffer(self):
        try:
            self.display.delete(self.buffer_bar_bg)
            self.buffer_bar_bg = None
        except Tkinter.TclError:
            pass

        try:
            self.display.delete(self.buffer_bar)
            self.buffer_bar = None
        except Tkinter.TclError:
            pass

    def undraw_volume(self):
        try:
            self.display.delete(self.volume_display)
            self.volume_display = None
        except Tkinter.TclError:
            pass

        try:
            self.display.delete(self.volume_bg_display)
            self.volume_bg_display = None
        except Tkinter.TclError:
            pass

    def undraw_rate(self):
        try:
            self.display.delete(self.rate_display)
            self.rate_display = None
            self.rate = None
        except Tkinter.TclError:
            pass

    def undraw_offline_reason(self):
        try:
            self.display.delete(self.offline_reason_display)
            self.offline_reason_display = None
        except Tkinter.TclError:
            pass

    def undraw(self):
        self.undraw_timer()
        self.undraw_state()
        self.undraw_progress()
        self.undraw_buffer()
        self.undraw_volume()
        self.undraw_mover()
        self.undraw_rate()
        self.undraw_offline_reason()

    #########################################################################

    def update_state(self, state, time_in_state=0):
        if state == self.state:
            return

        self.old_state = self.state  #Remember what state we currently are.
        self.state = state

        #different mover colors
        #mover_error_color   = colors('mover_error_color')
        #mover_offline_color = colors('mover_offline_color')
        #mover_stable_color  = colors('mover_stable_color')
        #state_error_color   = colors('state_error_color')
        #state_offline_color = colors('state_offline_color')
        #state_stable_color  = colors('state_stable_color')
        #state_idle_color    = colors('state_idle_color')
        #tape_stable_color   = colors('tape_stable_color')
        #label_stable_color  = colors('label_stable_color')
        #tape_offline_color  = colors('tape_offline_color')
        #label_offline_color = colors('label_offline_color')
        #mover_label_color   = colors('mover_label_color')

        #These mover colors stick around.
        self.percent_color       = colors('percent_color')
        self.progress_bar_color  = colors('progress_bar_color')
        self.progress_bg_color   = colors('progress_bg_color')
        self.timer_color         = colors('timer_color')
        self.volume_font_color   = colors('label_stable_color')
        self.volume_bg_color     = colors('tape_stable_color')
        self.library_color       = self.display.get_mover_color(self.library)
        if self.state in ['ERROR']:
            self.mover_color = colors('mover_error_color')
            self.state_color = colors('state_error_color')
            self.label_color = colors('state_error_color')
        elif self.state in ['OFFLINE']:
            self.mover_color = colors('mover_offline_color')
            self.state_color = colors('state_offline_color')
            self.label_color = colors('state_offline_color')
            self.draining = False
        else:
            self.mover_color = colors('mover_stable_color')
            if self.state in ['Unknown', 'IDLE']:
                self.state_color = colors('state_idle_color')
            else:
                self.state_color = colors('state_stable_color')
            self.label_color = colors('mover_label_color')

        #Update the time in state counter for the mover.
        now = time.time()
        self.timer_started = now - time_in_state
        self.update_timer(now)
        self.update_timer_color()

        self.draw_mover() #Some state changes change the mover color.
        self.draw_state()

        #Perform some cleanup in case some UDP Mmessages were lost.

        #If the mover should not have a volume, remove it.
        if state in ['IDLE', 'Unknown']:
            msg="need to unload tape because mover state changed to: %s"
            Trace.trace(2, msg % (state,))
            self.unload_tape()

        #If a transfer is not in progress, some things need to be undrawn.
        if (state in ['ERROR', 'IDLE', 'OFFLINE', 'Unknown', 'HAVE_BOUND'
                      'CLEANING', 'SETUP']) \
           or \
           (self.old_state in ['FINISH_WRITE', 'HAVE_BOUND']):

            connection = self.display.connections.get(self.name, None)
            if connection != None:
                message = "Need to disconnect because %s state changed to: %s"
                Trace.trace(2, message % (self.name, state,))
                #Insert into the beginning of the queue.  This avoids
                # deadlocks from trying to call disconnect_command() directly.
                queue_item = "mover %s %s" % (self.name, state)
                message_queue.insert_queue(queue_item,
                                           self.display.system_name)

        #Undraw these objects that correlate only to the ACTIVE/DRAINING
        # state.
        if state in ['ERROR', 'IDLE', 'OFFLINE', 'Unknown', 'HAVE_BOUND',
                     'FINISH_WRITE', 'CLEANING', 'SEEK', 'SETUP',
                     'DISMOUNT_WAIT', 'MOUNT_WAIT']:
            self.update_rate(None)
            self.update_progress(None, None)
            self.update_buffer(None)

        #Query the inquisitor (or mover for 'ERROR') for the reason why
        # the mover is down.
        if state in ['OFFLINE', 'Unknown', 'ERROR']:
            #The reason for the self.old_state test is to prevent looking up
            # offline information when update_state() is called for the first
            # time to set the state to "Unknown".  self.old_state will only
            # be None in this one case.
            if self.old_state != None:
                #passing None forces a request to the inquistor for
                # the scheduled up/down information.
                self.update_offline_reason(None)
        #We know the offline reason needs to be removed.
        elif self.offline_reason_display:
            self.undraw_offline_reason()

    #We don't want to reset the color everytime update_timer() gets called.
    #  So, for those few cases, we have this stand alone function.
    def update_timer_color(self):
        if self.timer_display:
            self.display.itemconfigure(self.timer_display,
                                       fill = self.timer_color)

    def update_timer(self, now):
        seconds = int(now - self.timer_started)
        if seconds == self.timer_seconds:
            return

        self.timer_seconds = seconds
        self.timer_string = HMS(seconds)

        if self.timer_display:
            self.display.itemconfigure(self.timer_display,
                                       text = self.timer_string)

            #Only worry if this for "too long" situation.  When the state
            # changes this will be set back to normal color in update_state().
            if (self.timer_seconds > 3600) and \
               (self.state in ["ACTIVE", "SEEK", "SETUP", "loaded",
                               "MOUNT_WAIT", "DISMOUNT_WAIT", "FINISH_WRITE",
                               "HAVE_BOUND", "DRAINING", "CLEANING"]):
                self.timer_color = colors('timer_longtime_color')
                self.display.itemconfigure(self.timer_display,
                                           fill = self.timer_color)

    def update_rate(self, rate):

        if rate == self.rate:
            return

        old_rate = self.rate

        self.rate = rate

        if rate != None:
            if (old_rate == None or old_rate == 0.0) and rate != 0.0:
                #If we don't have any rate iformation yet, hold off on
                # displaying the ACTIVE state icon.  If we wait until the
                # transfer rate is non-zero we could make a different icon
                # for reading and writing.
                if self.state not in ["ACTIVE"]:
                    #The mover "state" change message was dropped.  We
                    # got a "transfer" message, which means we need to
                    # implicitly put the mover into the ACTIVE state.
                    self.update_state("ACTIVE")
                else:
                    self.draw_state()

            self.rate_string = "%.2f MB/S" % (self.rate / 1048576)
            self.draw_rate()
        else:
            self.rate_string = ""
            self.undraw_rate()

    def update_progress(self, percent_done, alt_percent_done):

        if percent_done == None and alt_percent_done == None:
            self.percent_done = None
            self.alt_percent_done = None
            self.undraw_progress()
            return

        #Only draw the background if it does not exist.
        if not self.progress_bar_bg:
            #It turns out to work best if the entire progress bar area
            # is drawn in this situation.
            self.percent_done = percent_done
            self.alt_percent_done = alt_percent_done
            self.draw_progress()
            return

        if percent_done != self.percent_done:
            self.percent_done = percent_done
            self.draw_network_progress()

        if alt_percent_done != self.alt_percent_done:
            self.alt_percent_done = alt_percent_done
            self.draw_media_progress()

    def update_buffer(self, buffer_size):  #buffer_size in bytes

        if buffer_size == None:
            buffer_size = None
            self.undraw_buffer()
            return

        #Only draw the background if it does not exist.
        if not self.buffer_bar_bg:
            self.draw_background_buffer()

        #This helps prevent potential problems against changes on the fly.
        buffer_size = long(buffer_size)
        if buffer_size > self.max_buffer:
            #In case we don't know the correct max mover size; set it.
            self.max_buffer = buffer_size
        #If the buffer size has changed, update the buffer bar.
        if buffer_size != self.buffer_size:
            self.buffer_size = buffer_size
            self.draw_buffer()

    #offline_reason normally will be a string containing a description of
    # why the mover is down.  It may be set to None, which means that we
    # we are scheduling a request to be sent to inquisitor.  Any other python
    # false value will set the offline_reason to None, without scheduling the
    # request to the inquisitor.
    def update_offline_reason(self, offline_reason):

        if self.state in ['OFFLINE', 'Unknown', 'ERROR']:
            if offline_reason == None: # and not self.offline_reason:
                #To avoid issues when all the movers are restared at once;
                # reschedule the next offline check for 1 second from now
                # to help batch all of them at once.
                self.display.after_cancel(self.display.after_offline_reason_id)
                self.display.after_offline_reason_id = self.display.after(1000,
                                          self.display.check_offline_reason)
                self.offline_reason = None
                return
            elif offline_reason == self.offline_reason:
                return
            elif offline_reason:
                self.offline_reason = offline_reason
            else:
                #offline_reason was passed in as a python false value.
                self.offline_reason = None
        else:
            self.undraw_offline_reason()
            return

        if self.state in ['OFFLINE']:
            self.mover_color = colors('mover_offline_color')
            self.state_color = colors('state_offline_color')
            self.label_color = colors('state_offline_color')
        elif self.state in ["Unknown"] and self.offline_reason:
            self.mover_color = colors('mover_stable_color')
            self.state_color = colors('state_idle_color')
            self.label_color = colors('mover_label_color')
            self.timer_color = colors('timer_color')
        elif self.state in ["Unknown"] and not self.offline_reason:
            self.mover_color = colors('mover_unknown_color')
            self.state_color = colors('state_unknown_color')
            self.label_color = colors('state_unknown_color')
            self.timer_color = colors('timer_unknown_color')
        elif self.state in ['ERROR']:
            self.mover_color = colors('mover_error_color')
            self.state_color = colors('state_error_color')
            self.label_color = colors('state_error_color')

        #Because of changing all the colors, we need to redraw everything
        # and not just the offline_reason().
        self.draw()

    def load_tape(self, volume_name, load_state):
        self.volume = volume_name
        self.update_state(load_state)
        self.draw_volume()

    def unload_tape(self):
        if not self.volume:
            Trace.trace(2, "Mover %s has no volume." % (self.name,))
            return

        self.volume = None
        self.undraw_volume()

    def transfer_rate(self, num_bytes, mover_time = None):
        #keeps track of last number of bytes and time; calculates rate
        # in bytes/second
        num_bytes = long(num_bytes)  #If this throughs an error...
        try:
            el_time = float(mover_time)
        except (ValueError, TypeError):
            el_time = time.time()  #time.time() returns a float.

        try:
            rate = float(num_bytes - self.b0) / float(el_time - self.t0)
        except ZeroDivisionError:
            rate = 0.0
        except TypeError:
            #Something really bad happend.  The code failed to work.
            rate = None

        #self.b0 should always be an integer, and self.t0 should always
        # be a float.
        self.b0 = num_bytes
        self.t0 = el_time
        return rate

    #########################################################################

    def position_circular(self, N, position=None):
        #N = number of movers

        #k is the sequence number of this mover in the display.
        if not position:
            k = self.display.get_mover_index(self.name)
        else:
            #position is a two-tuple; the column number and the row number
            k = (position[0] * MIPC) + position[1]

        #mcc = Mover Column Count
        mcc = self.display.get_mover_column_count()
        #ccc = Cliet Column Count
        ccc = self.display.get_client_column_count()
        #tcc = Total Column Count
        tcc = self.display.get_total_column_count()

        #x_ratio is used to make the "circle" really a vertical oval.
        x_ratio = float(mcc) / float(tcc)
        #x_offset is used to slide the "circle" of movers to the right.
        x_offset = (float(ccc) - 1) / float(tcc)

        if N == 1: ## special positioning for a single mover.
            k = 1
            angle = math.pi / 2
        else:
            angle = (2 * math.pi) / N
        x =  x_ratio * math.sin(angle*k) + x_offset
        y =  .9 * math.cos(angle*k)

        scaled_x, scaled_y = scale_to_display(x, y,
                                              self.display.width,
                                              self.display.height)

        #mcc = Total Column Count
        mcc = self.display.get_mover_column_count()

        return scaled_x, scaled_y

    def position_linear(self, N, position=None):
        __pychecker__ = "no-argsused"
        #N = number of movers

        if not position:
            #position is a two-tuple; the column number and the row number
            position = self.display.get_mover_position(self.name)

        #k = self.index  # k = number of this mover
        mmpc = float(MMPC) #Maximum movers per column

        #total number of columns
        num_cols = self.display.get_total_column_count()
        #total number of rows in the largest column
        num_rows = self.display.get_mover_maximum_column_count()
        #this mover's column
        column = position[0]
        #this mover's row
        #row = self.display.mover_positions[column].get_index(self.name)
        row = position[1]

        #vertical distance seperating the bottom of one mover with the top
        #  of the next.  Use MMPC instead of (MMPC - 1) for the
        #  (self.height * MMPC) term to offset the even columns'
        #  "lowerness" on the screen.
        #Note: The following calculation was divided into even more lines
        #  of code.  The error string:
        #  '%s %s %s is always 1 or ZeroDivisionError'
        #  means the following DIVIDE_VAR_BY_ITSELF.
        #First find the total space between movers (vertically).
        space_between = (self.display.height - (self.height * mmpc))
        #Adjusted space between individulal movers in the display.
        space_between = (space_between / (mmpc - 1.0))

        #Now that the seperation space in pixels between two movers is known,
        # it need to be adjusted for the odd-to-even column offset in the
        # display.
        space = (self.height - space_between) * (((mmpc - 1.0) - num_rows))
        space = (space / (mmpc - 1.0)) + space_between

        #The following offsets the y values for a second column.
        if column % 2: #odds
            y_offset = 0
        else:
            y_offset = self.height / 2.0

        #Calculate the y position for rows with odd and even number of movers.
        #These calculation start in the middle of the window, subtract the
        # first half of them, then add the position that the current mover
        # is in.

        if num_rows % 2: #odd
            y = (self.display.height / 2.0) - \
                ((num_rows - 1) / 2.0 * (space + self.height)) - \
                (self.height / 2.0) + (row * (space + self.height)) + \
                y_offset
        else:    #even
            y = (self.display.height / 2.0) - \
                ((num_rows / 2.0) * (space + self.height)) + \
                (row * (space + self.height)) + \
                y_offset

        #Adding 1 to the column values in the following line,
        # mathematically gives the clients their own column
        column_width = (self.display.width / float(num_cols))
        x =  column_width * (column + \
                             self.display.get_client_column_count())

        return int(x), int(y)

    def position(self, N, position=None):
        layout = self.display.master.layout.get()
        if layout == CIRCULAR:
            return self.position_circular(N, position=position)
        elif layout == LINEAR:
            return self.position_linear(N, position=position)
        else:
            Trace.trace(1, "Unknown layout %s." % (layout,))
            sys.exit(1)

        return -1 #Otherwise pychecker complains.

    def resize(self):

        #Used to calculate width.
        total_columns = self.display.get_total_column_count()

        #Set the mover size in the display.  For the width, add 1 for
        # seperating space.
        self.height = ((self.display.height - 40) / 20)
        self.width = (self.display.width / (total_columns + 1))

        #Font geometry. (state, label, timer)
        self.font = get_font((self.height/3.5),
                             width_wanted=self.max_font_width(),
                             fit_string="DISMOUNT_WAIT")

        #Size of the volume portion of mover display.
        self.vol_height = self.height/2.5  #(self.height / 2.0) - 2.0
        self.volume_font = get_font(self.vol_height,
                                    fit_string="MMMM99",
                                    width_wanted=((self.width/2.5)))
        self.vol_width = max(((self.width)/2.5),
                             self.volume_font.measure("MMMM99"))

        #Set state size of the display.
        self.state_width = self.width - self.vol_width - 6
        self.state_height = self.vol_height

        #These are the new offsets
        self.volume_offset         = XY(2, 2)
        self.volume_label_offset   = XY(
                    self.volume_offset.x+(self.vol_width / 2.0),
                    self.volume_offset.y+(self.vol_height / 2.0))
        self.label_offset          = XY(
            self.width - 2,
            self.height / 2.0)
        self.img_offset            = XY(4 + self.vol_width, 0)
        self.state_offset          = XY(
            ((self.width - self.vol_width - 6) / 2) + (4 + self.vol_width),
            (self.vol_height / 2.0) + 0)
        self.timer_offset          = XY(self.width - 2, self.height - 0)
        self.rate_offset           = XY(self.width - 2, 2) #green

        self.bar_width             = self.width/2.5 #(how long bar should be)
        self.bar_height            = self.height/4
        self.buffer_bar_height     = self.bar_height / 2.0
        self.progress_bar_offset1  = XY(4, self.height - 2 - self.bar_height)
        self.progress_bar_offset2  = XY(4, self.height - 2)#yellow
        self.buffer_bar_offset1 = XY(4, self.height - 4 -
                                     self.buffer_bar_height - self.bar_height)
        self.buffer_bar_offset2 = XY(4, self.height - 4 -
                                     self.bar_height)#magenta
        self.percent_disp_offset   = XY(self.bar_width + 6, self.height)#green
        self.offline_reason_offset = XY(4, self.height - 2)

    def max_font_width(self):
        return (self.width - self.width/3.0) - 10

    def max_label_font_width(self):
        #total number of columns
        num_cols = self.display.get_total_column_count()
        #size of column in pixels
        column_width = (self.display.width / float(num_cols + 1))
        #difference of column width and mover rectangle with fudge factor.
        return (column_width - self.width) - 10

    def reposition(self, N):
        #Undraw the mover before moving it.
        self.undraw()

        self.resize()
        self.x, self.y = self.position(N)

        self.draw()

    #########################################################################

    def handle_status(self, mover, status):
        state = status.get('state','Unknown')
        time_in_state = status.get('time_in_state', '0')
        mover_state = "state %s %s %s" % (mover, state, time_in_state)
        volume = status.get('current_volume', None)
        client = status.get('client', "Unknown")
        connect = "connect %s %s" % (mover, client)
        if not volume:
            return [mover_state]
        if state in ['ACTIVE', 'SEEK', 'SETUP']:
            loaded = "loaded %s %s" % (mover, volume)
            return [loaded, mover_state, connect]
        if state in ['HAVE_BOUND', 'DISMOUNT_WAIT']:
            loaded = "loaded %s %s" % (mover, volume)
            return [loaded, mover_state]
        if state in ['MOUNT_WAIT']:
            loading = "loading %s %s" % (mover, volume)
            return [loading, mover_state, connect]

        return [mover_state]

    def get_mover_client(self):
        name = self.name.split("@")[0] + ".mover"
        system_name = self.name.split("@")[1]
        csc = get_csc(system_name)
        mov = mover_client.MoverClient(csc, name,
                flags=enstore_constants.NO_ALARM | enstore_constants.NO_LOG,)
        return mov

#########################################################################
##
#########################################################################

class Client:

    def __init__(self, name, display):
        self.name               = name
        self.display            = display
        self.mover_names        = {}
        self.last_activity_time = time.time()
        self.waiting            = None   #1 = wait, 0 = connected, None = init
        self.label              = None
        self.outline            = None
        self.font_color         = colors('client_font_color')
        self.color              = None
        self.outline_color      = self.display.get_client_color(self.name)

        self.resize()
        self.position()
        self.update_state(CONNECTED)  #Sets self.waiting.
        self.draw()

    #########################################################################

    def draw(self):
        x, y = self.x, self.y

        if self.outline:
            self.display.coords(self.outline, x,y, x+self.width,y+self.height)
        else:
            self.outline = self.display.create_oval(x, y, x+self.width,
                                                    y+self.height,
                                                    fill=self.color,
                                                    outline=self.outline_color,
                                                    width=1)

        if self.label:
            self.display.coords(self.label,
                                x + (self.width/2), y + (self.height/2))
            self.display.itemconfigure(self.label, font = self.font)
        else:
            self.label = self.display.create_text(x+self.width/2,
                                                  y+self.height/2,
                                                  text=self.name,
                                                  font=self.font,
                                                  fill=self.font_color)

    def undraw(self):
        try:
            self.display.delete(self.outline)
            self.outline = None
        except Tkinter.TclError:
            pass

        try:
            self.display.delete(self.label)
            self.label = None
        except Tkinter.TclError:
            pass

    #########################################################################

    def update_state(self, waiting_state):

        ### color

        if waiting_state != self.waiting:
            self.waiting = waiting_state
            if self.waiting in [WAITING, COUNTDOWN]:
                self.color = colors('client_wait_color')
            else:
                self.color = colors('client_active_color')

            if self.outline:
                self.display.itemconfigure(self.outline, fill = self.color)

    #########################################################################

    def resize(self):
        self.width = self.display.width / \
                     (self.display.get_total_column_count() + 1)
        self.height =  self.display.height/28
        self.font = get_font(self.height/2.5) #'arial')

    def position(self):

        position = self.display.get_client_position(self.name)

        column_width = (self.display.width /
                        self.display.get_total_column_count())
        row_height = self.display.height / MIPC

        x = 0.1 + (position[0] - 1) * column_width
        y = (self.display.height / 2.0) + (row_height * position[1])
        y = y - row_height #adjust one slot for zero index
        y = y + ((self.height / 2.0) * (position[0] % 2))
        if position[0] % 2 == 0: #For even columns
            y = y - (self.height / 4.0)

        self.x, self.y = int(x), int(y)

    def reposition(self):
        self.undraw()
        self.resize()
        self.position()
        self.draw()

#########################################################################
##
#########################################################################

class Connection:
    """ a line connecting a mover and a client"""
    def __init__(self, mover, client, display):
        # we are passing instances of movers and clients
        self.mover              = mover
        self.client             = client
        self.display            = display
        self.rate               = 0 #pixels/second, not MB
        self.dashoffset         = 0
        self.segment_start_time = 0
        self.segment_stop_time  = 0
        self.line               = None
        self.path               = []
        self.color              = None
        self.color_type         = None    #CLIENT_COLOR

        self.update_color(self.display.master.connection_color.get())
        self.position()

    def reinit(self, mover, client):
        if self.mover != mover or self.client != client:
            self.mover = mover
            self.client = client

            self.reposition()

    #########################################################################

    def draw(self):
        if self.line:
            self.display.coords(self.line, tuple(self.path))
            self.display.itemconfigure(self.line, dashoffset = self.dashoffset)
                                       #fill = self.color)
        else:
            self.line = self.display.create_line(self.path, dash='...-',
                                                 width=2, smooth=1,
                                                 dashoffset = self.dashoffset,
                                                 fill=self.color)

    def undraw(self):
        try:
            self.display.delete(self.line)
            self.line = None
        except Tkinter.TclError:
            pass

    def animate(self, now=None):
        if self.rate == None:
            #The transfer is complete don't update in this case.
            return

        if now == None:
            now = time.time()
        if now >= self.segment_stop_time:
            return

        #Despite the local_variables and if statements it is still faster
        # to perform these actions then do the itemconfigure() every time;
        # including when it will have no effect.

        new_offset = int(self.dashoffset + \
                              self.rate * (now - self.segment_start_time))

        if new_offset != self.dashoffset:  #we need to redraw the line
            self.dashoffset = new_offset
            if self.line:
                #Since we only want to change this one aspect of the line
                # don't call draw().  Doing any uncesessary computation
                # is a real performace killer.
                self.display.itemconfigure(self.line,
                                           dashoffset = self.dashoffset)

    #########################################################################

    def update_rate(self, rate):
        now                     = time.time()
        # starting time at this rate
        self.segment_start_time = now
        # let the animation run 5 seconds
        self.segment_stop_time  = now + enstore_constants.MAX_TRANSFER_TIME
        # remember the rate
        self.rate               = rate

    def update_color(self, color_type):
        if color_type == CLIENT_COLOR:
            self.color_type = CLIENT_COLOR
            self.color = self.display.get_client_color(self.client.name)
        else:  #LIBRARY_COLOR
            self.color_type = LIBRARY_COLOR
            self.color = self.display.get_mover_color(self.mover.library)

        if self.line:
            self.display.itemconfigure(self.line, fill = self.color)

    #########################################################################

    #Don't be tempted to have position call other functions to set set.path.
    # This for unknown reasons causes tkinter to render the path in unexpected
    # ways.
    def position(self):
        self.path = [] #remove old path

        #Column positions start counting at 1.  Thus, a three column display
        # has mover columns 1, 2 and 3.
        position = self.display.get_mover_position(self.mover.name)

        column = position[0]
        row = position[1]

        # middle of left side of mover
        mx = self.mover.x
        my = self.mover.y + self.mover.height/2.0 + 1
        self.path.extend([mx,my])

        # past the first column.
        if column > 1:
            pos = self.display.get_mover_coordinates((column - 1, row))
            mx = (pos[0] + self.mover.width + self.mover.x) / 2.0
            my = self.mover.y + self.mover.height/2.0 + 1
            self.path.extend([mx,my])

        values = range(1, column)
        values.reverse()
        for i in values:
            if i % 2 == 0:
                #Go over even numbered columns.
                pos = self.display.get_mover_coordinates((i, row))
                pos1 = self.display.get_mover_coordinates((i + 1, row))

                pos_low = self.display.get_mover_coordinates((i, row - 1))
                if pos_low == None:
                    my = pos[1] - 2
                else:
                    my = (pos_low[1] + self.mover.height + pos[1]) / 2.0

                mx = (pos[0] + self.mover.width + pos1[0]) / 2.0
                self.path.extend([mx,my])

                mx = pos[0] + self.mover.width
                self.path.extend([mx,my])

                mx = pos[0]
                self.path.extend([mx,my])

                if i - 1 >= 1: #Skip this step on the last (leftmost) column.
                    pos_1 = self.display.get_mover_coordinates((i - 1, row))
                    mx = (pos_1[0] + self.mover.width + pos[0]) / 2.0
                    self.path.extend([mx,my])

            else:
                #Go under odd numbered columns.
                pos = self.display.get_mover_coordinates((i, row))
                pos1 = self.display.get_mover_coordinates((i + 1, row))

                pos_high = self.display.get_mover_coordinates((i, row + 1))
                if pos_high == None:
                    my = pos[1] + 2 + self.mover.height
                else:
                    my = (pos[1] + self.mover.height + pos_high[1]) / 2.0

                mx = (pos[0] + self.mover.width + pos1[0]) / 2.0
                self.path.extend([mx,my])

                mx = pos[0] + self.mover.width
                self.path.extend([mx,my])

                mx = pos[0]
                self.path.extend([mx,my])

                if i - 1 >= 1: #Skip this step on the last (leftmost) column.
                    pos_1 = self.display.get_mover_coordinates((i - 1, row))
                    mx = (pos_1[0] + self.mover.width + pos[0]) / 2.0
                    self.path.extend([mx,my])

        #Column positions start counting at 1.  Thus, a three column display
        # has client columns 1, 2 and 3.
        position = self.display.get_client_position(self.client.name)

        if position == None:
            #Is this necessary anymore?
            Trace.trace(0, "An unknown error occured.  Please send the "\
                           "enstore developers the following output.")
            Trace.trace(0, "ERROR: %s %s %s" % (position, self.client.name,
                                                self.mover.name))
            Trace.trace(0, pprint.pformat(self.display.clients))
            for i in range(len(self.display.client_positions.keys())):
                Trace.trace(0, "COLUMN: %s" % (i + 1,))
                Trace.trace(0, pprint.pformat(self.display.client_positions[i + 1].item_positions))

            return

        column = position[0]
        row = position[1]

        #This is more accurate than self.client.width.
        column_width = self.display.width / \
                       self.display.get_total_column_count()

        #For the client side we will use a temporary list and go in the
        # opposite direction that we used for the movers (then reverse it).
        client_path = []

        #middle of right side of client
        cx, cy = (self.client.x + self.client.width,
                  self.client.y + self.client.height/2.0)
        client_path.extend([cx, cy])

        for i in range(column, self.display.get_client_column_count()):
            if (i % 2 == column % 2): #same column height
                cx = column_width * (i)
                cy = self.client.y + (self.client.height / 2.0)
                client_path.extend([cx, cy])

                cx = column_width * (i + 1)
                cy = self.client.y + (self.client.height / 2.0)
                client_path.extend([cx, cy])
            else:                     #different column height
                cx = column_width * (i)
                cy = self.client.y + self.client.height + 2
                client_path.extend([cx, cy])

                cx = column_width * (i + 1)
                cy = self.client.y + self.client.height + 2
                client_path.extend([cx, cy])

        #Add in the two points that make the pretty spline curve between
        # clients and movers.
        x_distance = mx - cx
        client_path.extend([cx + x_distance / 3.0, cy,
                            mx - x_distance / 3.0, my])

        #Take the temporary client path items and in reverse order place them
        # at the end of the self.path point list.
        while len(client_path):
            self.path.extend(client_path[-2:])
            del client_path[-2:]

    def reposition(self):
        self.undraw()
        self.position()
        self.draw()

#########################################################################
##  What does this class do?
#########################################################################

class Title:
    def __init__(self, text, display):
        self.text       = text #this is just a string
        self.display    = display
        self.tk_text    = None #this is a tk Text object
        self.fill       = None #color to draw with
        self.font       = get_font(20)  #, "arial")
        self.length     = 2.5  #animation runs 2.5 seconds
        now             = time.time()
        self.start_time = now
        self.stop_time  = now + self.length

    def draw(self):
        #center this in the entire canvas
        self.tk_text = self.display.create_text(self.display.width/2,
                                                self.display.height/2,
                                                text=self.text, font=self.font,
                                                justify=Tkinter.CENTER)

    def animate(self, now=None):
        if now==None:
            now = time.time()
        if not self.tk_text:
            self.draw()
        elapsed = now - self.start_time
        startrgb = 0,0,0
        endrgb = 173, 216, 230
        currentrgb = [0,0,0]
        for i in range(3):
            currentrgb[i] = int(startrgb[i] + \
                                (endrgb[i]-startrgb[i])*(elapsed/self.length))
        fill=rgbtohex(currentrgb[0], currentrgb[1], currentrgb[2])
        self.display.itemconfigure(self.tk_text, fill=fill)

#########################################################################
##
#########################################################################

class MoverDisplay(Tkinter.Toplevel):
    """  The mover state display """
    ##** means "variable number of keyword arguments" (passed as a dictionary)
    ## mover - instantiated Mover class instance
    def __init__(self, mover, **attributes):
        if not hasattr(self, "state_display"):
            Tkinter.Toplevel.__init__(self)
            self.configure(attributes)
            #Font geometry.
            self.font = get_font(12)  #, 'arial')

        self.init_common(mover)

        self.state_display = None
        self.after_mover_display_id = None

        self.status_text = "" #self.format_mover_status(self.get_mover_status())

        #When the window is closed, we have some things to cleanup.
        self.bind('<Destroy>', self.window_killed)

        self.update_status()

    def reinit(self, mover = None):

        self.after_cancel(self.after_mover_display_id)

        Tkinter.Toplevel.__init__(self)  #Redraw the window.
        self.init_common(mover)  #Set values for this mover's info.
        self.update_status()  #Fill the mover window with the mover info.

    #Tell it to set the remaining configuration values and to apply them.
    def init_common(self, mover=None):
        # The mover class values that are copied are done with copy.copy()
        # to avoid cyclic references.
        if mover:
            self.mover_name = copy.copy(mover.name)  # mover31@stken
            self.title(self.mover_name) #Update the title with mover name

            self.system_name = self.mover_name.split("@")[1]
            self.csc = get_csc(self.system_name)

            short_mover_name = self.mover_name.split("@")[0]
            mover_name = short_mover_name + ".mover" #mover31.mover
            self.mc = mover_client.MoverClient(self.csc, mover_name)

            self.transaction_ids = []

    def window_killed(self, event):
        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        #With the window closed, don't do an update.
        self.after_cancel(self.after_mover_display_id)

        #Clear this to avoid a cyclic reference.
        self.mover_name = ""
        self.system_name = ""

        #Make sure to close any open file desctiptors.
        try:
            del self.csc
        except AttributeError:
            pass
        self.csc = None
        try:
            del self.mc
        except AttributeError:
            pass
        self.mc = None


    def draw(self):

        try:
            self.state_display.configure(text = self.status_text,
                                         foreground = self.state_color,
                                         background = self.mover_color,)
            self.state_display.pack(side=Tkinter.LEFT, expand=Tkinter.YES,
                                    fill=Tkinter.BOTH)
        except (Tkinter.TclError, AttributeError):
            #If the state_display variable does not yet exist; create it.
            self.state_display = Tkinter.Label(master=self,
                                               justify=Tkinter.LEFT,
                                               font = self.font,
                                               text = self.status_text,
                                               foreground = self.state_color,
                                               background = self.mover_color,
                                               anchor=Tkinter.NW)
            self.state_display.pack(side=Tkinter.LEFT, expand=Tkinter.YES,
                                    fill=Tkinter.BOTH)

    def undraw(self):
        try:
            self.state_display.destroy()
            self.state_display = None
        except Tkinter.TclError:
            pass

    #Wrapper for generic_client.send_deferred().
    def send_mover_status_request(self):
        if self.mc:
            txn_id = self.mc.u.send_deferred({'work':"status"},
                                             self.mc.server_address)
            self.transaction_ids.append(txn_id)

    #Wrapper for generic_client.recv_deferred().
    def receive_mover_status_request(self, timeout):
        if self.mc:
            if self.transaction_ids:
                try:
                    status = self.mc.u.recv_deferred(self.transaction_ids,
                                                     timeout)
                except (socket.error, socket.herror, socket.gaierror), msg:
                    status = {'status' : (e_errors.NET_ERROR, str(msg))}
                except (e_errors.EnstoreError), msg:
                    status = {'status' : (msg.type, str(msg))}

                #In case of timeout, set the state to Unknown.
                if status.get('state', None) == None:
                    status['state'] = "Unknown"

                return status

            #Will get here the first time called from update_status().
            return None

        #Should never get here.
        return None

    def format_mover_status(self, status_dict):
        if status_dict == None:
            return ""
        else:
            order = status_dict.keys()
            order.sort()
            msg = ""
            for item in order:
                msg = msg + "%s: %s\n" % (item,
                                          pprint.pformat(status_dict[item]))
            return msg

    def update_status(self):
        ## We want to separate the sending and receiving of these requests
        ## and answers.  Otherwise, while waiting for the response, the
        ## display is not updating, which we want it to continue doing.

        #Get any pending status updates.
        status = self.receive_mover_status_request(0.0)
        if status:
            self.status_text = self.format_mover_status(status)

            if status['state'] in ['ERROR']:
                self.state_color = colors('state_error_color')
                self.mover_color = colors('mover_error_color')
            elif status['state'] in ['OFFLINE']:
                self.state_color = colors('state_offline_color')
                self.mover_color = colors('mover_offline_color')
            elif status['state'] in ['IDLE']:
                self.state_color = colors('state_idle_color')
                self.mover_color = colors('mover_stable_color')
            elif status['state'] in ['Unknown']:
                if hasattr(self, 'state_color'):
                    self.state_color = colors('state_unknown_color')
                    self.mover_color = colors('mover_unknown_color')
                else:
                    #We get here in the first call from __init__().
                    self.state_color = colors('state_idle_color')
                    self.mover_color = colors('mover_stable_color')
            else:
                self.state_color = colors('state_stable_color')
                self.mover_color = colors('mover_stable_color')

            self.draw()

        #Send another status update request.
        self.transactions_ids = []
        self.send_mover_status_request()

        #Reset the time for 5 seconds.
        self.after_mover_display_id = self.after(MOVER_DISPLAY_TIME,
                                                 self.update_status)

#########################################################################
##
#########################################################################

MOVERS="MOVERS"
CLIENTS="CLIENTS"

class Column:

    def __init__(self, number, type):

        self.number = number
        self.type = type #MOVERS or CLIENTS
        self.item_positions = {}
        self.column_limit = None  #MIPC
        self.max_index = 0  #Max index this column has ever seen.  Only
                            # valid for sequential (MOVERS) columns.

    def get_index(self, item_name):
        for index in self.item_positions.keys():
            if self.item_positions[index] == item_name:
                return index

        return None

    def get_name(self, item_index):
        return self.item_positions.get(item_index, None)

    def get_max_limit(self):
        if self.column_limit == None:
            return MIPC
        return self.column_limit

    def set_max_limit(self, limit):
        if type(limit) == types.IntType and limit > 0 and limit <= MIPC:
            if self.column_limit == None or self.column_limit < limit:
                self.column_limit = limit

    def get_max_index(self):
        return self.max_index

    def add_item(self, item_name):

        if len(self.item_positions.keys()) >= self.get_max_limit():
            #Column is full.
            return True

        if self.type == CLIENTS:
            return self.add_alt_item(item_name)
        elif self.type == MOVERS:
            return self.add_seq_item(item_name)

        return True #Failure

    def add_alt_item(self, item_name):
        i = 0

        ## Step through possible positions in order 0, 1, -1, 2, -2, 3, ...
        while self.item_positions.has_key(i):
            if i == 0:
                i = 1
            elif i > 0:
                i = -i
            else:
                i = 1 - i

        if i < -(MIPC / 2) or i > (MIPC / 2):
            return True

        self.item_positions[i] = item_name

        return False

    def add_seq_item(self, item_name):
        i = 0

        while self.item_positions.has_key(i):
            i = i + 1

        if i > MIPC:
            return True

        self.item_positions[i] = item_name
        if i > self.max_index:
            #If this is the highest index seen so far, up the max_index.
            self.max_index = i
        return False

    def del_item(self, index_or_name):
        if type(index_or_name) == types.IntType: #We have an index.
            del self.item_positions[index_or_name]
        else:                                    #We have a name.
            for index in self.item_positions.keys():
                if self.item_positions[index] == index_or_name:
                    del self.item_positions[index]

    def count(self):
        return len(self.item_positions)

    def has_item(self, item_name):
        if item_name in self.item_positions.values():
            return True

        return False

#########################################################################
##
#########################################################################

class Display(Tkinter.Canvas):
    """  The main state display """
    ##** means "variable number of keyword arguments" (passed as a dictionary)
    #entvrc_info is a dictionary of various parameters.
    def __init__(self, entvrc_info, system_name,
                 master = None, mover_display = None,
                 **attributes):
        if not hasattr(self, "master"):
            self.master = master
            reinited = 0
        else:
            reinited = 1

        self.system_name = system_name
        self.library_colors = entvrc_info.get('library_colors', {})
        self.client_colors = entvrc_info.get('client_colors', [])

        #Only call Tkinter.Canvas.__init__() on the first time through.
        if not reinited:
            if master:
                self.master_geometry = self.master.geometry()
                Tkinter.Canvas.__init__(self, master = master)

            else:
                Tkinter.Canvas.__init__(self)

###XXXXXXXXXXXXXXXXXX  --get rid of scrollbars--
##        if canvas_width is None:
##            canvas_width = window_width
##        if canvas_height is None:
##            canvas_height = window_height

##        #Initialzie the window.
##        Tkinter.Canvas.__init__(self, master, width=window_width,
##                                height=window_height,
##                               scrollregion=(0,0,canvas_width,canvas_height))

##        self.scrollX = Tkinter.Scrollbar(self, orient=Tkinter.HORIZONTAL)
##        self.scrollY = Tkinter.Scrollbar(self, orient=Tkinter.VERTICAL)

##        #When the canvas changes size or moves, update the scrollbars
##        self['xscrollcommand']= self.scrollX.set
##        self['yscrollcommand'] = self.scrollY.set

##        #When scrollbar clicked on, move the canvas
##        self.scrollX['command'] = self.xview
##        self.scrollY['command'] = self.yview

##        #pack 'em up
##        self.scrollX.pack(side=Tkinter.BOTTOM, fill=Tkinter.X)
##        self.scrollY.pack(side=Tkinter.RIGHT, fill=Tkinter.Y)
##        self.pack(side=Tkinter.LEFT)
###XXXXXXXXXXXXXXXXXX  --get rid of scrollbars--

        #Various toplevel window attributes.
        self.configure(attributes)

        if self.master:
            #If animation is turned on, set animation on (by default it
            # is set off until here).
            if entvrc_info.get('animate', 1) == ANIMATE:
                try:
                    master.entv_do_animation.set(ANIMATE)
                except AttributeError:
                    #Deal with this if we run enstore_display directly.
                    pass

        self.width  = int(self['width'])
        self.height = int(self['height'])

        self.reinit_display()
        self.clear_display() #C

        self.bind('<Button-1>', self.action)
        self.bind('<Button-3>', self.reinitialize)
        self.bind('<Configure>', self.resize)
        self.bind('<Destroy>', self.window_killed)
        self.bind('<Visibility>', self.visibility)
        self.bind('<Button-2>', self.print_canvas)

        self.after_smooth_animation_id = None
        self.after_clients_id = None
        self.after_reinitialize_id = None
        self.after_process_messages_id = None
        self.after_join_id = None
        self.after_offline_reason_id = None
        self.after_reposition_id = None

        #Set this to the current time so that the alarm signal doesn't
        # get raised to hastily.
        self.last_message_processed = time.time()

        #Clear the window for drawing to the screen.
        #self.pack(expand = 1, fill = Tkinter.BOTH)
        self.update()

        #Force the specific mover display to be reinitialized.
        if mover_display:
            mover_display.reinit(display = self)
            self.mover_display = mover_display
        else:
            self.mover_display = None

        self.startup()

    def reinit_display(self):
        self._reinit = 0
        self.stopped = 0

    def clear_display(self):
        self.movers           = {} ## This is a dictionary keyed by mover name,
                                   ##value is an instance of class Mover
        self.mover_positions  = {} ##key is position index (0,1,-1,2,-2) and
                                   ##value is Mover
        self.clients          = {} ## dictionary, key = client name,
                                   ##value is instance of class Client
        self.client_positions = {} ##key is position index (0,1,-1,2,-2) and
                                   ##value is Client
        self.connections      = {} ##dict. of connections.

    def attempt_reinit(self):
        return self._reinit

    #########################################################################

    def undraw(self):
        for connection in self.connections.values():
            connection.undraw()
        for mover in self.movers.values():
            mover.undraw()
        for client in self.clients.values():
            client.undraw()

    #########################################################################

    def action(self, event):

        Trace.trace(6, "Starting action()")

        x, y = self.canvasx(event.x), self.canvasy(event.y)
        overlapping = self.find_overlapping(x-1, y-1, x+1, y+1)
        Trace.trace(1, "%s %s" % (overlapping, (x, y)))

        #Display detailed mover information.
        for mover in self.movers.values():
            for i in range(len(overlapping)):
                if mover.state_display == overlapping[i]:
                    #If the window already exits; reuse it.
                    if getattr(self, "mover_display", None):
                        self.mover_display.reinit(mover = mover)
                    else:
                        self.mover_display = MoverDisplay(mover)

        #Change the color of the connection.
        for connection in self.connections.values():
            for i in range(len(overlapping)):
                if connection.line == overlapping[i]:
                    self.itemconfigure(connection.line,
                                       fill=invert_color(connection.color))
                else:
                    self.itemconfigure(connection.line, fill=connection.color)

        Trace.trace(6, "Finishing action()")

    def resize(self, event=None):

        Trace.trace(6, "Starting resize()")

        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        Trace.trace(1, "New dimensions: %s" % self.master.wm_geometry())
        self.master_geometry = self.master.geometry()

        try:
            if self.after_smooth_animation_id:
                self.after_cancel(self.after_smooth_animation_id)
            if self.after_clients_id:
                self.after_cancel(self.after_clients_id)
            if self.after_reinitialize_id:
                self.after_cancel(self.after_reinitialize_id)
            if self.after_process_messages_id:
                self.after_cancel(self.after_process_messages_id)
            if self.after_reposition_id:
                self.after_cancel(self.after_reposition_id)
        except AttributeError:
            pass  #Will get here when resize is called during __init__.

        #If the user changed the window size, schedule an update.  We don't
        # want to do this every time the window changes sizes.  When a user
        # changes the size using the mouse MANY window change events are
        # generated.  This helps wait until the last is done.
        self.after_reposition_id = self.after(100, self.reposition_canvas)

        try:
            self.after_smooth_animation_id = self.after(UPDATE_TIME,
                                                        self.smooth_animation)
            self.after_clients_id = self.after(UPDATE_TIME,
                                               self.disconnect_clients)
            self.after_reinitialize_id = self.after(REINIT_TIME,
                                                    self.reinitialize)
            self.after_process_messages_id = self.after(UPDATE_TIME,
                                                        self.process_messages)
        except AttributeError:
            pass

        Trace.trace(6, "Finishing resize()")

    def reinitialize(self, event=None):

        Trace.trace(6, "Starting reinitialize()")

        ### Keep in mind this function can be called from a Tk callback or
        ### from a SIGALRM signal being recieved.

        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        self._reinit = 1
        self.stopped = 1

        self.quit()

        #Forces cleanup of objects that would not happen otherwise.  As part
        # of the destroy call Tkinter does generate a Destroy event that
        # results in window_killed() being called.  Skipping a destroy()
        # function call would result in a huge memory leak.
        self.destroy()

        Trace.trace(6, "Finishing reinitialize()")

    def print_canvas(self, event):
        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        self.postscript(file="/home/zalokar/entv.ps", pagewidth="8.25i")

    def window_killed(self, event):

        Trace.trace(6, "Starting window_killed()")

        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        self.stopped = 1

        #Since the window has been closed, it makes no sense to continue
        # to update this information.  It is a major performace killer
        # to leave these running.
        if self.after_smooth_animation_id:
            self.after_cancel(self.after_smooth_animation_id)
        if self.after_clients_id:
            self.after_cancel(self.after_clients_id)
        if self.after_reinitialize_id:
            self.after_cancel(self.after_reinitialize_id)
        if self.after_process_messages_id:
            self.after_cancel(self.after_process_messages_id)
        if self.after_join_id:
            self.after_cancel(self.after_join_id)
        if self.after_offline_reason_id:
            self.after_cancel(self.after_offline_reason_id)

        for mov in self.movers.values():
            if mov.timer_id:
                self.after_cancel(mov.timer_id)

        self.master_geometry = self.master.geometry()

        self.cleanup_display()

        Trace.trace(6, "Finishing window_killed()")

    def visibility (self, event):

        Trace.trace(6, "Starting visibility()")

        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        self.master_geometry = self.master.geometry()

        Trace.trace(6, "Finishing visibility()")

    #########################################################################

    def reposition_canvas(self, force = None):

        Trace.trace(5, "Starting reposition_canvas()")

        try:
            size = self.winfo_width(), self.winfo_height()
        except Tkinter.TclError:
            #self.stopped = 1
            return
        if size != (self.width, self.height) or force:
            # size has changed
            self.width, self.height = size
            if self.clients:
                self.reposition_clients()
            if self.movers:
                self.reposition_movers()
            if self.connections:
                self.reposition_connections()

            self.after_reposition_id = None

        Trace.trace(5, "Finishing reposition_canvas()")

    def reposition_movers(self, number_of_movers=None):

        Trace.trace(7, "Starting reposition_movers()")

        items = self.movers.values()
        if number_of_movers:
            N = number_of_movers
        else:
            N = len(items) #need this to determine positioning
        self.mover_label_width = None
        for mover in items:
            mover.reposition(N)

        Trace.trace(7, "Finishing reposition_movers()")

    def reposition_clients(self):

        Trace.trace(7, "Starting reposition_clients()")

        for client in self.clients.values():
            client.reposition()

        Trace.trace(7, "Finishing reposition_clients()")

    def reposition_connections(self):

        Trace.trace(7, "Starting reposition_connections()")

        for connection in self.connections.values():
            connection.reposition()

        Trace.trace(7, "Finish reposition_connections()")

    #########################################################################

    #Called from smooth_animation().
    def connection_animation(self):

        Trace.trace(15, "Starting connection_animation()")

        #If the user turned off animation, don't do it.
        #if not self.animate:
        try:
            if self.master and self.master.entv_do_animation.get() == STILL:
                return
        except AttributeError:
            #Deal with starting enstore_display.py directly.
            return

        now = time.time()
        #### Update all connections.
        for connection in self.connections.values():
            connection.animate(now)

        Trace.trace(15, "Starting connection_animation()")

    #Called from process_messages().
    def is_up_to_date(self, command):

        words = command.split(" ")

        if words and words[0] in ["state"]:

            try:
                mover = self.movers[words[1]]
            except (KeyError, IndexError):
                mover = None

            if mover and mover.volume == None and words[2] in \
                ["HAVE_BOUND", "SEEK", "ACTIVE", "CLEANING", "DRAINING"] \
                 or \
                 mover and self.connections.get(mover.name, None) == None \
                 and words[2] in ["ACTIVE", "DRAINING", "SEEK", "MOUNT_WAIT"]:

                return 0 #Not up-to-date.

        return 1  #Up to date.

    #Called from join_thread().
    # XXX Is this used for anything???
    def _join_thread(self, waitall = None):
        global status_request_threads

        Trace.trace(15, "Starting _join_thread()")

        thread_lock.acquire()

        alive_list = []
        try:
            for i in range(len(status_request_threads)):
                if waitall:
                    status_request_threads[i].join()
                else:
                    status_request_threads[i].join(0.0)
                if status_request_threads[i].isAlive():
                    alive_list.append(status_request_threads[i])
        except (KeyboardInterrupt, SystemExit):
            thread_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        status_request_threads = alive_list

        thread_lock.release()

        Trace.trace(15, "Finishing _join_thread()")

    #########################################################################

    #These functions are all called from Tkinter callbacks.  Because they
    # can happen asynchronously to normal execution protect them with
    # display_lock (process_messages is slightly different).

    #Called from self.after().
    def process_messages(self):
        t0 = time.time()

        Trace.trace(10, "Starting process_messages()")

        if self.stopped: #If we should stop, then stop.
            Trace.trace(10, "Finishing process_messages() early")
            return

        #Only process the messages in the queue at this time.
        number = min(message_queue.len_queue(self.system_name), 1000)

        #Try and only take a small time to do this.
        remember_number = number
        display_count = len(self.master.enstore_systems_enabled)
        wait_time = (MESSAGES_TIME * 0.001 / display_count + 1)

        while (number > 0 and (time.time() - t0) < (wait_time)):

            #Words is a list of the split string command.
            command = self.get_valid_command()
            if command == "":
                number = number - 1
                #For ignored messages or dropped transfer messages...
                continue
            #If a datagram gets dropped, attempt to recover the lost
            # information by asking for it.
            if not self.is_up_to_date(command):
                result = acquire(startup_lock, "startup_lock", blocking=0)
                if result:
                    try:
                        words = command.split()
                        request_queue.put_queue(words[1],
                                            tid = words[1].split("@")[-1])
                    except (KeyboardInterrupt, SystemExit):
                        release(startup_lock, "startup_lock")
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]
                    except:
                        exc, msg, tb = sys.exc_info()
                        traceback.print_exception(exc, msg, tb)
                        del tb  #Avoid resource leak.
                    release(startup_lock, "startup_lock")

            #Process the next item in the queue.
            self.handle_command(command)

            number = number - 1

            if self.stopped:
                return

        #Schedule the next message processing.
        if not self.stopped:
            self.after_process_messages_id = self.after(process_time(),
                                                        self.process_messages)
            if remember_number != number:
                #We remember this time because of the possibility of a
                # signal handler needing it to detect if we are hung.
                self.last_message_processed = time.time()

        Trace.trace(10, "Finishing process_messages() (%f sec/%d messages)" %
                    (time.time() - t0, remember_number - number))

    #Called from self.after().
    def smooth_animation(self):

        Trace.trace(10, "Starting smooth_animation()")

        display_lock.acquire()

        try:
            #If necessary, process the animation of the connections lines.
            self.connection_animation()

            #Schedule the next animation.
            self.after_smooth_animation_id = self.after(animate_time(),
                                                    self.smooth_animation)
        except (KeyboardInterrupt, SystemExit):
            display_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:  #Tkinter.TclError
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        display_lock.release()

        Trace.trace(10, "Finishing smooth_animation()")

    #Called from self.after().
    def disconnect_clients(self):
        global request_queue

        Trace.trace(5, "Starting disconnect_clients()")

        acquire(clients_lock, "clients_lock")

        try:
            now = time.time()
            #### Check for unconnected clients
            for client_name, client in self.clients.items():

                #This for loop is needed to identify clients that have
                # had their connections removed do to a change of mover
                # state instead of from a "disconnect" notify message being
                # received.
                for mover_name in client.mover_names.keys():
                    connection = self.connections.get(mover_name, None)
                    if not connection or \
                           connection.client.name != client.name:

                        if connection == None:
                            connection_client_name = None
                        else:
                            #When we have a connection, but it is incorrect.
                            connection_client_name = connection.client.name

                        Trace.trace(2,
                                    "Found client discrepancy (%s != %s) " \
                                    "for %s" \
                                    % (connection_client_name,
                                       client.name, mover_name))
                        #Remove the mover from the client's list.
                        del client.mover_names[mover_name]
                        #Put this mover into the network/messages thread
                        # reqest queue to find out what its state is and
                        # the client it might be talking to.
                        request_queue.put_queue(mover_name, self.system_name)

                if len(client.mover_names) > 0:
                    #If the mover has active connections, don't remove it
                    # from the display.
                    continue
                if client.waiting == WAITING:
                    #If the mover has pending connections, don't remove it
                    # from the display.
                    #
                    # This is currrently disabled functionality.
                    continue
                if now - client.last_activity_time > 5: # grace period
                    Trace.trace(2, "It's been longer than 5 seconds, %s " \
                                "client must be deleted" % (client_name,))

                    display_lock.acquire()
                    try:
                        client.undraw()
                    except (KeyboardInterrupt, SystemExit):
                        display_lock.release()
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]
                    except:  #Tkinter.TclError
                        exc, msg, tb = sys.exc_info()
                        traceback.print_exception(exc, msg, tb)
                        del tb  #Avoid resource leak.
                    display_lock.release()

                    try:
                        # Remove client from the client list.
                        del self.clients[client_name]
                    except KeyError:
                        pass
                    old_number = self.get_client_column_count()
                    try:
                        # Mark this spot as unoccupied.
                        self.del_client_position(client_name)
                    except KeyError:
                        pass

                    #If the number of columns has changed, redraw so that
                    # everything gets located and sized correctly.
                    new_number = self.get_client_column_count()
                    if old_number != new_number:
                        display_lock.acquire()
                        self.reposition_canvas(force=1)
                        display_lock.release()

                    Trace.trace(2, "Client %s has been deleted" \
                                % (client_name,))
        except (KeyboardInterrupt, SystemExit):
            release(clients_lock, "clients_lock")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        release(clients_lock, "clients_lock")

        display_lock.acquire()
        self.after_clients_id = self.after(UPDATE_TIME,
                                           self.disconnect_clients)
        display_lock.release()

        Trace.trace(5, "Finishing disconnect_clients()")


    #Called from self.after().
    def join_thread(self):

        Trace.trace(10, "Starting join_thread()")

        display_lock.acquire()

        try:
            self._join_thread()

            self.after_join_id = self.after(JOIN_TIME, self.join_thread)
        except (KeyboardInterrupt, SystemExit):
            display_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:  #Tkinter.TclError
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        display_lock.release()

        Trace.trace(5, "Finishing join_thread()")


    def check_offline_reason(self):
        global request_queue

        Trace.trace(5, "Starting check_offline_reason()")

        display_lock.acquire()

        try:
            #For multiple inquisitors; one for each system.
            already_requested = []

            for mover in self.movers.values():
                system_name = mover.name.split("@")[1]

                if mover.state in ['ERROR']:
                    if mover.offline_reason == None:
                        request_queue.put_queue(mover.name, system_name)

                elif mover.state in ['OFFLINE', 'Unknown']:
                    if system_name in already_requested:
                        #We've already asked this Enstore system's inquisitor.
                        pass
                    elif not mover.offline_reason:

                        request_queue.put_queue('inquisitor', system_name)
                        already_requested.append(system_name)
        except (KeyboardInterrupt, SystemExit):
            display_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:  #Tkinter.TclError
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        try:
            #Even if an error occurs above, schedule this function to be
            # executed again in the future.
            self.after_offline_reason_id = self.after(OFFLINE_REASON_TIME,
                                                  self.check_offline_reason)
        except (KeyboardInterrupt, SystemExit):
            display_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:  #Tkinter.TclError
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        display_lock.release()

        Trace.trace(5, "Finishing check_offline_reason()")

    #Called from entv.handle_periodic_actions().
    #def handle_titling(self):
    #
    #    display_lock.acquire()
    #
    #    try:
    #        now = time.time()
    #        #### Handle titling
    #        if self.title_animation:
    #            if now > self.title_animation.stop_time:
    #                self.title_animation = None
    #            else:
    #                self.title_animation.animate(now)
    #
    #        ####force the display to refresh
    #        self.update()
    #    except (KeyboardInterrupt, SystemExit):
    #        display_lock.release()
    #        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    #    except:
    #        exc, msg, tb = sys.exc_info()
    #        traceback.print_exception(exc, msg, tb)
    #        del tb  #Avoid resource leak.
    #
    #    display_lock.release()

    #########################################################################

    def create_movers(self, mover_names):

        Trace.trace(6, "Starting create_movers()")

        #Determine which mover names in the list are new.
        new_mover_names = []
        for mover_name in mover_names:
            if not self.get_mover_position(mover_name):
                new_mover_names.append(mover_name)

        #Shorten the number of new movers.
        N = len(new_mover_names)

        #Make sure to reserve the movers' positions before creating them.
        # Be sure to take into account any previously created movers.
        self.reserve_mover_columns(N + self.get_mover_count())

        #Create a Mover class instance to represent each new mover.
        for k in range(N):
            mover_name = new_mover_names[k]
            #If the mover already exists, skip creating it.
            if not self.movers.has_key(mover_name):
                column, row = self.add_mover_position(mover_name)
                self.movers[mover_name] = Mover(mover_name, self, index=k,
                                                row=row, column=column,
                                                movers=N)

        if N:
            #If we have new movers, rearange the display.
            self.reposition_canvas(force=1)

        Trace.trace(6, "Finishing create_movers()")

    def get_mover_color(self, library):

        #In the event that that mover belongs to mulitple libraries,
        # pick the first one.
        if type(library) == types.ListType:
            library = library[0]

        #If this is the first mover from this library.
        if not getattr(self, "library_colors", None):
            self.library_colors = {}

        #Make some adjustments.
        if type(library) == types.ListType:
            library = library[0]
        if library[-16:] == ".library_manager":
            library = library[:-16]

        #If this mover's library is already remembered.
        library_color = self.library_colors.get(library, None)
        #print "library_color:", library_color, "library:", library
        #pprint.pprint(self.library_colors)
        if self.library_colors.get(library, None):
            return self.library_colors[library]

        self.library_colors[library] = rgbtohex(0, 0, 0)

        return self.library_colors[library]

    def get_client_color(self, client):
        for host_match, color in self.client_colors:
            #Add implicit begining of line (^) and end of line ($) characters
            # to the match pattern.  If the .entvrc file has a client_color
            # line for cmsstor12 and cmsstor121, we want the correct color
            # for cmsstor121, not the cmsstor12 color.
            use_host_match = host_match
            if host_match[0] != "^":
                use_host_match = "^" + use_host_match
            if use_host_match[-1] != "$":
                use_host_match = use_host_match + "$"

            #Check to see if we have a regular expresion match.
            try:
                if re.compile(use_host_match).search(client):
                    return color
            except AttributeError:
                pass

        self.client_colors.append((client, colors('client_outline_color')))

        return self.client_colors[-1][1]  #Return the default color.

    #########################################################################

    def add_client_position(self, client_name):
        # searching the existing columns for the client's name.
        for i in range(len(self.client_positions) + 1)[1:]:
            i2 = self.client_positions[i].get_index(client_name)
            if i2:
                return (i, i2)

        #The variable search_order is a list of two-tuples, where each
        # two-tuple consits of the number in each column and the index number
        # of that column.
        search_order = []
        for i in range(len(self.client_positions) + 1)[1:]:
            search_order.append((self.client_positions[i].count(), i))
        #The search order is column order, lowest to highest which is left
        # to right on the screen.
        #
        #The sort after the loop, first sorts by the number of clients in a
        # column.  If there are ties, it sorts the lowest column number first.
        # We only do this sort as long as we have enough movers to still
        # require the number of client columns current used.  The intent
        # is to that if fewer clients are running at the present time, then
        # fill the lower columns first so that the right most client column
        # may be removed when it is empty.  (Add one to consider this client
        # we are adding.)
        if int(math.ceil((self.get_client_count() + 1) / float(MIPC))) >= \
               self.get_client_column_count():
            ##search_order.sort() #sort into ascending order.
            pass

        for t in search_order:
            rtn = self.client_positions[t[1]].add_item(client_name)
            if rtn: #Filled the column, search the next one.
                continue
            #Otherwise return success.
            return (i, self.client_positions[i].get_index(client_name))
        else:
            #Need to add another column.
            index = len(self.client_positions) + 1
            self.client_positions[index] = Column(index, CLIENTS)
            #The following line is temporary
            self.client_positions[index].add_item(client_name)

            return (index, self.client_positions[index].get_index(client_name))

    def del_client_position(self, client_name):
        #Start searching the existing columns for the existing slot.
        for i in range(len(self.client_positions) + 1)[1:]:
            self.client_positions[i].del_item(client_name)

        #If the rightmost column is now empty, remove the column too.
        if self.client_positions[self.get_client_column_count()].count() == 0:
            del self.client_positions[self.get_client_column_count()]

    def get_client_position(self, client_name):
        for i in range(len(self.client_positions) + 1)[1:]:
            i2 = self.client_positions[i].get_index(client_name)
            if i2 != None:
                return (i, i2) #Tuple of the column number and row index.

        return None

    def get_client_name(self, position):
        return self.client_positions[position[0]].get_name(position[1])

    def get_client_count(self, column = None):
        if column == None:
            columns_search = range(len(self.client_positions) + 1)[1:]
        else:
            columns_search = [column]

        sum_value = 0
        for i in columns_search:
            sum_value = sum_value + self.client_positions[i].count()

        return sum_value

    def get_client_column_count(self):
        if len(self.client_positions) < 1:
            return 1  #Always assume at least one column of clients.
        else:
            return len(self.client_positions)


    def add_mover_position(self, mover_name):
        #Start searching the existing columns for the movers's name.
        for i in range(len(self.mover_positions) + 1)[1:]:
            i2 = self.mover_positions[i].get_index(mover_name)
            if i2 != None:
                return (i, i2) #Aleady existed, return existing location.

        for i in range(len(self.mover_positions) + 1)[1:]:
            rtn = self.mover_positions[i].add_item(mover_name)
            if rtn: #Filled the column, search the next one.
                continue
            #Otherwise return success.
            return (i, self.mover_positions[i].get_index(mover_name))
        else:
            #Need to add another column.
            index = len(self.mover_positions) + 1
            self.mover_positions[index] = Column(index, MOVERS)
            self.mover_positions[index].add_item(mover_name)

            return (index, self.mover_positions[index].get_index(mover_name))

    def del_mover_position(self, mover_name):
        #Start searching the existing columns for the existing slot.
        for i in range(len(self.mover_positions) + 1)[1:]:
            self.mover_positions[i].del_item(mover_name)

    #Return mover position information for linear layout.
    def get_mover_position(self, mover_name):
        #Start searching the existing columns for the requested mover.
        for i in range(len(self.mover_positions) + 1)[1:]:
            i2 = self.mover_positions[i].get_index(mover_name)
            if i2 != None:
                return (i, i2) #Tuple of the column number and row index.

        return None

    #Return mover position information for circular layout.
    def get_mover_index(self, mover_name):
        #Start searching the existing columns for the existing slot.
        running_sum = 0
        for i in range(len(self.mover_positions) + 1)[1:]:
            i2 = self.mover_positions[i].get_index(mover_name)
            if i2 != None:
                #Include the row of this movers column.
                running_sum = running_sum + i2
                return running_sum
            else:
                #Include the number of movers in this summation count.
                running_sum = running_sum + self.mover_positions[i].count()

        return None

    def get_mover_name(self, position):
        return self.mover_positions[position[0]].get_name(position[1])

    def get_mover_count(self, column = None):
        if column == None:
            columns_search = range(len(self.mover_positions) + 1)[1:]
        else:
            columns_search = [column]

        sum_value = 0
        for i in columns_search:
            sum_value = sum_value + self.mover_positions[i].count()

        return sum_value

    #Return the number of movers in the largest column.
    def get_mover_maximum_column_count(self):
        result = 0
        for i in range(len(self.mover_positions) + 1)[1:]:
            temp = self.mover_positions[i].get_max_limit()
            if temp > result:
                result = temp #Found greater column count.

        return result

    def reserve_mover_columns(self, number): #number of movers.

        #Determine the number of columns necessary for 'number' number
        # of columns.
        columns = int(math.ceil(number / float(MMPC)))

        #First, use the whole number division to determine the number of
        # movers that each column must contain to be even.
        if columns == 0:
            #Avoid division by zero if entv.get_mover_list() returned an
            # empty list of movers.
            min_count = 0
        else:
            min_count = int(number) / int(columns)
        #Second, set the minimum number of movers for each column.
        limits = {}
        for i in range(1, columns + 1):
            limits[i] = min_count
        #Third, take the remainder of movers and set them one at a time
        # to the columns to evenly distribute them.
        i2 = 1
        for i in range(number - (min_count * columns)):
            limits[i2] = limits[i2] + 1
            #Adjust the counter to the next limit.
            i2 = ((i2 + 1) % columns)
            if i2 > columns:
                columns = 1

        #Create the nessecary columns and set the limit on the number of
        # movers allowed in each of them.
        for i in range(1, columns + 1):
            if not self.mover_positions.has_key(i):
                self.mover_positions[i] = Column(i, MOVERS)
            #If the new limit is higher, the new max limit will be reset.
            # Otherwise the exisiting limit will remain.  This is to keep
            # the movers in there current locations if a library is
            # deselected in the "library managers" menu.
            self.mover_positions[i].set_max_limit(limits[i])

    def get_mover_column_count(self):
        if len(self.mover_positions) < 1:
            return 1  #Always assume at least one column of movers.
        else:
            return len(self.mover_positions)

    def get_total_column_count(self):
        return self.get_mover_column_count() + \
               self.get_client_column_count() + 1

    def get_mover_coordinates(self, position):
        if type(position) != types.TupleType and len(position) != 2:
            return None

        #We use the first mover in the list and use it to obtain the
        # screen coordinates for the mover at the requested position.
        first_mover = self.movers.keys()[0]
        return self.movers[first_mover].position(len(self.movers),
                                                 position=position)

    #########################################################################

    def toggle_library_managers_enabled(self):
        for mover_name, mover in self.movers.items():
            if type(mover.library) == types.ListType:
                use_libraries = mover.library
            else:
                use_libraries = [mover.library]

            for mover_library in use_libraries:
                #Remove trailing ".library_manager".
                use_mover_library = mover_library.split(".")[0]
                tmp = self.master.enstore_library_managers_enabled.get(
                    use_mover_library, None)
                if tmp == None:
                    continue
                if bool(tmp.get()):
                    #We found one of this mover's libries not on the
                    # igore list.
                    break
            else:
                #All the mover's libraries are on the ignore list.  Remove
                # it from the display.

                connection = self.connections.get(mover_name, None)
                if connection:
                    #If a connection currently exists, we need to remove
                    # it from the display first.
                    client_name = self.connections[mover_name].client.name
                    command = ["disconnect", mover_name, client_name]
                    self.disconnect_command(command)

                #Now the actual removal of the mover from the display.
                mover.undraw()
                del self.movers[mover_name]
                self.del_mover_position(mover_name)

        #We need to see if there are new movers.
        request_queue.put_queue("get_all_movers", self.system_name)

    #########################################################################

    def quit_command(self, command_list):
        #This function is called by apply().  It must have the same signature
        # as the others, even tough command_list is not used.  Thus,
        # suppress the unused args pychecker test.
        __pychecker__ = "no-argsused"

        self.stopped = 1
        self.quit()

    def title_command(self, command_list):
        title = string.join(command_list[1:])
        title=string.replace (title, '\\n', '\n')
        self.title_animation = Title(title, self)

    def menu_command(self, command_list):
        menu_name = command_list[1]  #options, systems or library managers

        if menu_name == "library_managers":

            acquire(clients_lock, "clients_lock")

            try:
                menu_item_name = command_list[2]  #new checkbox label
                if bool(int(command_list[3])):  #default value
                    on_off = Tkinter.TRUE
                else:
                    on_off = Tkinter.FALSE

                #shortcut
                LMs_on_off = self.master.enstore_library_managers_enabled

                if not LMs_on_off.has_key(menu_item_name):
                    #Add the library manager to the menu, but only if it is not
                    # already there.

                    LMs_on_off[menu_item_name] = Tkinter.BooleanVar()
                    LMs_on_off[menu_item_name].set(on_off)

                    self.master.enstore_library_managers_menu.add_checkbutton(
                        label = menu_item_name,
                        indicatoron = Tkinter.TRUE,
                        onvalue = Tkinter.TRUE,
                        offvalue = Tkinter.FALSE,
                        variable = LMs_on_off[menu_item_name],
                        command = self.toggle_library_managers_enabled,
                        )
            except (KeyboardInterrupt, SystemExit):
                release(clients_lock, "clients_lock")
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except Tkinter.TclError, msg:
                #Trap this error for giving the user a better error message.
                if str(msg) in ["out of stack space (infinite loop?)",
                                'expected boolean value but got "??"']:
                    try:
                        message = "Tcl/Tk libraries were not compiled with threading enabled.\n"
                        sys.stderr.write(message)
                    except:
                        pass
                    traceback.print_exception(sys.exc_info()[0], msg,
                                              sys.exc_info()[2])
                    os.abort()  #Not much else to do.
                else:
                    exc, msg, tb = sys.exc_info()
                    traceback.print_exception(exc, msg, tb)
                    del tb  #Avoid resource leak.
            except:
                exc, msg, tb = sys.exc_info()
                traceback.print_exception(exc, msg, tb)
                del tb  #Avoid resource leak.

            release(clients_lock, "clients_lock")

    def client_command(self, command_list):
        ## Only draw waiting clients if the user really wants to see them all.
        if self.master.show_waiting_clients.get() == CONNECTED:
            return

        acquire(clients_lock, "clients_lock")

        try:
            client_name = normalize_name(command_list[1])
            client = self.clients.get(client_name)
            if client is None: #it's a new client
                old_number = self.get_client_column_count()

                self.add_client_position(client_name)
                client = Client(client_name, self)
                self.clients[client_name] = client

                #If the number of client columns changed we need to reposition.
                new_number = self.get_client_column_count()
                if old_number != new_number:
                    self.reposition_canvas(force=1)

                client.update_state(WAITING) #change fill color if needed
                client.draw()
        except (KeyboardInterrupt, SystemExit):
            release(clients_lock, "clients_lock")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        release(clients_lock, "clients_lock")

    def connect_command(self, command_list):

        acquire(clients_lock, "clients_lock")

        now = time.time()

        try:
            #Get the corresponding mover object.
            mover = self.movers.get(command_list[1])
            if mover == None:
                release(clients_lock, "clients_lock")
                Trace.trace(2,
                            "Cannot find internal mover object %s." % \
                            (command_list[1],))

            if mover.state not in ["SETUP", "MOUNT_WAIT", "loaded", "SEEK"]:
                #Insert into the beginning of the queue.  This avoids
                # deadlocks from trying to call update_state() directly.
                queue_item = "state %s SETUP" % (mover.name, )
                message_queue.insert_queue(queue_item, self.system_name)

            ###What are these for?
            mover.t0 = now
            mover.b0 = 0L
        except (KeyboardInterrupt, SystemExit):
            release(clients_lock, "clients_lock")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

            release(clients_lock, "clients_lock")
            return

        try:
            #Draw the client.

            client_name = normalize_name(command_list[2])
            client = self.clients.get(client_name)
            if not client: ## New client, we must add it
                old_number = self.get_client_column_count()

                column, row = self.add_client_position(client_name)
                client = Client(client_name, self)
                self.clients[client_name] = client

                #If the client command is ever used, these lines are necessary.
                #   client.update_state(CONNECTED) #change fill color if needed
                client.draw()  #Draws the client.

                #If the number of client columns changed we need to reposition.
                new_number = self.get_client_column_count()
                if old_number != new_number:
                    self.reposition_canvas(force = 1)

                if client_name not in self.clients.keys():
                    print "ERROR: Newly added client not found in client list."
            else:
                client.update_state(CONNECTED) #change fill color if needed

            #Update this set of movers that this client has connections to.
            client.mover_names[mover.name] = mover.name
        except (KeyboardInterrupt, SystemExit):
            release(clients_lock, "clients_lock")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

            release(clients_lock, "clients_lock")
            return

        try:
            #Draw the connection.

            #First test if a connection is already present.
            connection = self.connections.get(mover.name, None)
            if connection:
                if connection.client != client:
                    del connection.client.mover_names[mover.name]
                connection.reinit(mover, client)
            #If not create a new connection.
            else:
                connection = Connection(mover, client, self)
                #Add the connection to the list.
                self.connections[mover.name] = connection
                connection.update_rate(0)
                connection.draw() #Draw it correctly.
        except (KeyboardInterrupt, SystemExit):
            release(clients_lock, "clients_lock")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        release(clients_lock, "clients_lock")

    def disconnect_command(self, command_list):

        acquire(clients_lock, "clients_lock")

        try:
            mover_name = command_list[1]
            client_name = normalize_name(command_list[2])

            Trace.trace(2, "mover %s is disconnecting from %s" %
                        (mover_name, client_name))

            #Get these python objects representing what is drawn on the screen.
            mover = self.movers.get(mover_name, None)
            client = self.clients.get(client_name, None)
            connection = self.connections.get(mover.name, None)

            if client != None:
                #Decrease the number of connections this client has.
                try:
                    del client.mover_names[mover.name]
                except (AttributeError, KeyError), msg:
                    pass

            ## Only change the active color to the WAITING/COUNTDOWN
            ## color if the "Show Waiting Clients" option is checked.
            if self.master.show_waiting_clients.get():
                if len(client.mover_names) == 0:
                    client.update_state(COUNTDOWN) #change fill color if needed

            if connection != None:
                #Remove all references to the connection.
                try:
                    connection.undraw()
                except (AttributeError, KeyError, Tkinter.TclError), msg:
                    pass
                try:
                    del self.connections[mover.name]
                except (AttributeError, KeyError), msg:
                    pass

            if mover != None:
                #Remove the progress bar.
                mover.update_progress(None, None)
                mover.update_buffer(None)
                mover.update_rate(None)

            Trace.trace(2, "mover %s is disconnected from %s" %
                        (mover_name, client_name))
        except (KeyboardInterrupt, SystemExit):
            release(clients_lock, "clients_lock")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.

        release(clients_lock, "clients_lock")

    def loaded_command(self, command_list):

        mover = self.movers.get(command_list[1])

        if mover.state in ['IDLE']:
            Trace.trace(2, "An idle mover cannot have tape...ignore")
            return
        load_state = command_list[0] #=='loaded'
        what_volume = command_list[2]
        mover.load_tape(what_volume, load_state)

    def state_command(self, command_list):

        mover = self.movers.get(command_list[1])

        what_state = command_list[2]
        try:
            time_in_state = int(float(command_list[3]))
        except (ValueError, TypeError, IndexError):
            time_in_state = 0
        mover.update_state(what_state, time_in_state)

    def error_command(self, command_list):
        #In this case "error" is from the point of view of the
        # inquisitor/schedular.  In entv it is the offline_reason.

        mover = self.movers.get(command_list[1])

        what_error = string.join(command_list[2:])
        mover.update_offline_reason(what_error)

    def unload_command(self, command_list):

        mover = self.movers.get(command_list[1])

        # Ignore the passed-in volume name, unload
        ## any currently loaded volume
        mover.unload_tape()

    def transfer_command(self, command_list):
        #      transfer MOVER_NAME BYTES_TRANSFERED BYTES_TO_TRANSFER \
        #               (media | network) [BUFFER_SIZE] [CURRENT_TIME]
        #
        # command_list[0] = transfer
        # command_list[1] = MOVER_NAME
        # command_list[2] = BYTES_TRANSFERED
        # command_list[3] = TOTAL_BYTES_TO_TRANSFER
        # command_list[4] = media or network
        # command_list[5] = BUFFER_SIZE
        # command_list[6] = CURRENT_TIME
        # command_list[7] = DRAINING

        #Get local handles for the objects that we will be using.
        mover = self.movers.get(command_list[1])
        try:
            raw_fraction = float(command_list[2]) / float(command_list[3])
        except ZeroDivisionError:
            raw_fraction = 1.0
        percent_done = abs(int(100 * raw_fraction))

        if command_list[4] == "network":
            mover.update_progress(percent_done, mover.alt_percent_done)
        elif command_list[4] == "media":
            mover.update_progress(mover.percent_done, percent_done)
        else:
            return

        #Transfer messages can only show up if the mover is in the ACTIVE
        # state.  If it is not active, make it active now.
        if mover.state != "ACTIVE":
            mover.update_state("ACTIVE")

        #If the mover sends the buffer size info. display the bar.
        try:
            #Redraw/reposition the buffer bar.
            mover.update_buffer(command_list[5])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass
        try:
            # set draining
            mover.draining = bool(int(command_list[7]))
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass

        #Skip media transfers from the network connection update.
        connection = self.connections.get(mover.name, None)
        if connection and command_list[4] == "network":
            #Calculate the new instantaneous rate.
            try:
                rate = mover.transfer_rate(command_list[2], command_list[6])
            except IndexError:
                rate = mover.transfer_rate(command_list[2])

            #Now that the rate is known, update the display.
            mover.update_rate(rate)
            if percent_done == 100:
                connection.update_rate(None)
            else:
                #Experience shows this is a good adjustment.
                connection.update_rate(rate / 262144)

        if connection:
            #Don't use the variable "now" here.  We want to use the local
            # machine's time for this measure.
            connection.client.last_activity_time = time.time()

    def movers_command(self, command_list):
        self.create_movers(command_list[1:])

    def newconfig_command(self, command_list):
        __pychecker__ = "no-argsused"  # Suppress pychecker warning.

        self.reinitialize()

    #########################################################################

    def queue_command(self, command):
        command = string.strip(command) #get rid of extra blanks and newlines
        words = string.split(command)

        if words[0] in self.comm_dict.keys():
            #Under normal situations.
            message_queue.put_queue(command, self.system_name)

    #      connect MOVER_NAME CLIENT_NAME
    #      disconnect MOVER_NAME CLIENT_NAME
    #      loaded MOVER_NAME VOLUME_NAME
    #      loading MOVER_NAME VOLUME_NAME
    #      #moveto MOVER_NAME VOLUME_NAME
    #      #remove MOVER_NAME VOLUME_NAME
    #      state MOVER_NAME STATE_NAME [TIME_IN_STATE]
    #      error MOVER_NAME <error>
    #      unload MOVER_NAME VOLUME_NAME
    #      transfer MOVER_NAME BYTES_TRANSFERED BYTES_TO_TRANSFER \
    #               (media | network) [BUFFER_SIZE] [CURRENT_TIME]
    #      movers MOVER_NAME_1 MOVER_NAME_2 ...MOVER_NAME_N
    comm_dict = {'quit' : {'function':quit_command, 'length':1},
                 'reinit': {'function':reinitialize, 'length':1},
                 'title' : {'function':title_command, 'length':1},
                 'menu' : {'function':menu_command, 'length':4},
                 #'csc' : {'function':csc_command, 'length':2},
                 'client' : {'function':client_command, 'length':2},
                 'connect' : {'function':connect_command, 'length':3,
                              'mover_check':1},
                 'disconnect' : {'function':disconnect_command, 'length':3,
                              'mover_check':1},
                 'loaded' : {'function':loaded_command, 'length':3,
                              'mover_check':1},
                 'loading' : {'function':loaded_command, 'length':3,
                              'mover_check':1},
                 'state' : {'function':state_command, 'length':3,
                              'mover_check':1},
                 'error' : {'function':error_command, 'length':3,
                            'mover_check': 1},
                 'unload': {'function':unload_command, 'length':3,
                              'mover_check':1},
                 'transfer' : {'function':transfer_command, 'length':3,
                              'mover_check':1},
                 'movers' : {'function':movers_command, 'length':2},
                 'newconfigfile' : {'function':newconfig_command, 'length':1}}

    def get_valid_command(self):

        command = message_queue.get_queue(self.system_name)
        if not command:
            return ""

        command = string.strip(command) #get rid of extra blanks and newlines
        words = string.split(command)

        if not words: #input was blank, nothing to do!
            return ""

        if words[0] not in self.comm_dict.keys():
            return ""

        #Don't bother processing transfer messages if we are not keeping up.
        if words[0] in ["transfer"] and \
               message_queue.len_queue(self.system_name) > \
               max(20, (len(self.movers))):
            return ""

        if len(words) < self.comm_dict[words[0]]['length']:
            Trace.trace(1, "Insufficent length for %s command: %s" % (words[0], string.join(words, "")))
            return ""

        if self.comm_dict[words[0]].get('mover_check', None) and \
           not self.movers.get(words[1]):
            #This is an error, a message from a mover we never heard of
            #Trace.trace(1, "Don't recognize mover %s, continuing ...."
            #            % words[1])

            #Adding new mover.
            self.create_movers([words[1]])
            #Force the canvas to move everything around now that a mover
            # has been added.
            self.reposition_canvas(force=True)

        return command

    def handle_command(self, command):
        ## Accept commands of the form:
        # 1 word:
        #      quit
        #      #robot
        #      title
        # 2 words:
        #      #delete MOVER_NAME
        #      client CLIENT_NAME
        # 3 words:
        #      connect MOVER_NAME CLIENT_NAME
        #      disconnect MOVER_NAME CLIENT_NAME
        #      loaded MOVER_NAME VOLUME_NAME
        #      loading MOVER_NAME VOLUME_NAME
        #      #moveto MOVER_NAME VOLUME_NAME
        #      #remove MOVER_NAME VOLUME_NAME
        #      state MOVER_NAME STATE_NAME [TIME_IN_STATE]
        #      unload MOVER_NAME VOLUME_NAME
        # 4 words:
        #      transfer MOVER_NAME nbytes total_bytes
        # (N) number of words:
        #      movers M1 M2 M3 ...

        words = command.split()
        if words:
            #Every command gets processed though handle_command().  Thus,
            # this will output all processed messages and those necessary
            # to insert because of missed messges.
            #if Trace.print_levels.has_key(10):
                #Building this string is resource expensive, only build it
                # if necessary.
            #    Trace.message(10, string.join((time.ctime(), command), " "))

            #Run the corresponding function.
            display_lock.acquire()
            try:
                apply(self.comm_dict[words[0]]['function'], (self, words,))
            except (KeyboardInterrupt, SystemExit):
                display_lock.release()
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except:
                exc, msg, tb = sys.exc_info()
                traceback.print_exception(exc, msg, tb)
                del tb  #Avoid resource leak.
            display_lock.release()

    #########################################################################

    def cleanup_display(self):
        global _font_cache
        #global _image_cache
        global message_queue, request_queue

        self._join_thread(waitall = True)

        #Undraw the display before deleting the objects (in the lists).
        try:
            if self.winfo_exists():
                self.undraw()
                self.update()
        except Tkinter.TclError:
            pass

        #Clobber these lists.
        for key in self.movers.keys():
            try:
                self.movers[key].display = None
                del self.movers[key]
            except KeyError:
                Trace.trace(1, "Unable to remove mover %s" % key)
        acquire(clients_lock, "clients_lock")
        try:
            for key in self.clients.keys():
                try:
                    self.clients[key].display = None
                    del self.clients[key]
                except KeyError:
                    Trace.trace(1, "Unable to remove client %s" % key)
        except (KeyboardInterrupt, SystemExit):
            release(clients_lock, "clients_lock")
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg, tb = sys.exc_info()
            traceback.print_exception(exc, msg, tb)
            del tb  #Avoid resource leak.
        release(clients_lock, "clients_lock")
        for key in self.connections.keys():
            try:
                self.connections[key].mover = None
                self.connections[key].client = None
                self.connections[key].display = None
                del self.connections[key]
            except KeyError:
                Trace.trace(1, "Unable to remove connection %s" % key)

        self.movers = {}
        self.clients = {}
        self.connections = {}

        #Perform the following two deletes explicitly to avoid obnoxious
        # tkinter warning messages printed to the terminal when using
        # python 2.2.
        for key in _font_cache.keys():
            try:
                del _font_cache[key]
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except:
                exc, msg, tb = sys.exc_info()
                traceback.print_exception(exc, msg, tb)
                del tb  #Avoid resource leak.

        #for key in _image_cache.keys():
        #    try:
        #        del _image_cache[key]
        #    except (KeyboardInterrupt, SystemExit):
        #        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        #    except:
        #        exc, msg, tb = sys.exc_info()
        #        traceback.print_exception(exc, msg, tb)
        #        del tb  #Avoid resource leak.

        _font_cache = {}
        #_image_cache = {}

    #overloaded
    def update(self):
        try:
            if self.winfo_exists():
                self.master.update()
        except ValueError, msg:
            Trace.trace(1, "Unexpected Tkinter error...ignore[update]: %s" %
                        str(msg))
        except Tkinter.TclError:
            exc, msg, tb = sys.exc_info()
            Trace.trace(1, "TclError...ignore[update]: %s: %s" %
                        (str(exc), str(msg)))
            pass  #If the window is already destroyed (i.e. user closed it)
                  # then this error will occur.

    def startup(self):
        #If the window does not yet exist, draw it.  This should only
        # be true the first time mainloop() gets called; for
        # reinitializations this should not happen.
        if self.master.state() == "withdrawn":
            self.reposition_canvas(force = True)

        self.after_smooth_animation_id = self.after(ANIMATE_TIME,
                                                    self.smooth_animation)
        self.after_clients_id = self.after(UPDATE_TIME,
                                           self.disconnect_clients)
        self.after_reinitialize_id = self.after(REINIT_TIME,
                                                self.reinitialize)
        self.after_process_messages_id = self.after(MESSAGES_TIME,
                                                    self.process_messages)
        self.after_join_id = self.after(JOIN_TIME,
                                        self.join_thread)
        #Always set this for a one time check right after starting.
        self.after_offline_reason_id = self.after(OFFLINE_REASON_INITIAL_TIME,
                                                  self.check_offline_reason)
        self.after_reposition_id = None

    def shutdown(self):
        global offline_reason_thread

        #Grab last thread if necessary.
        if offline_reason_thread:
            thread_lock.acquire()
            try:
                offline_reason_thread.join()
                del offline_reason_thread
                offline_reason_thread = None
            except (KeyboardInterrupt, SystemExit):
                thread_lock.release()
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except:
                exc, msg, tb = sys.exc_info()
                traceback.print_exception(exc, msg, tb)
                del tb  #Avoid resource leak.
            thread_lock.release()

#########################################################################

if __name__ == "__main__":
    if len(sys.argv)>1:
        title = sys.argv[1]
    else:
        title = "Enstore"

    master = Tkinter.Tk()
    display = Display({'background' : rgbtohex(173, 216, 230)},
                      "localhost", master = master)
    display.mainloop()


#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import pprint
import cmath
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
import gc

import Trace
import mover_client
import configuration_client
import enstore_constants
import e_errors

#Set up paths to find our private copy of tcl/tk 8.3

#Get the environmental variables.
ENSTORE_DIR = os.environ.get("ENSTORE_DIR", None)
ENTV_DIR = os.environ.get("ENTV_DIR", None)
PYTHONLIB = os.environ.get("PYTHONLIB", None)
IMAGE_DIR = None
LIB = None

#Determine the expected location of the local copy of Tcl/Tk.
try:
    #Determine the expected location of the local copy of Tcl/Tk.
    if ENSTORE_DIR:
        TCLTK_DIR = os.path.join(ENSTORE_DIR, 'etc','TclTk')
        IMAGE_DIR = os.path.join(ENSTORE_DIR, 'etc', 'Images')
        
        #If the local copy of tcl/tk is found, use it.
        temp_dir_tcl = os.path.join(TCLTK_DIR, 'tcl8.3')
        temp_dir_tk = os.path.join(TCLTK_DIR, 'tk8.3')
        temp_dir_lib = os.path.join(TCLTK_DIR, sys.platform)
        if os.path.exists(temp_dir_tcl) and os.path.exists(temp_dir_tk) and \
           os.path.exists(temp_dir_lib):
            os.environ["TCL_LIBRARY"] = temp_dir_tcl
            os.environ["TK_LIBRARY"] = temp_dir_tk
            LIB = temp_dir_lib            
        else:
            #Don't use the local copy.
            temp_dir = os.path.join(PYTHONLIB, "lib-dynload")
            if(os.path.exists(temp_dir)):
                LIB = temp_dir
    elif ENTV_DIR:
        TCLTK_DIR = ENTV_DIR
        IMAGE_DIR = os.path.join(ENTV_DIR, 'Images')

        #When using the cut version the libraries are shipped with it.
        temp_dir_tcl = os.path.join(TCLTK_DIR, 'tcl8.3')
        temp_dir_tk = os.path.join(TCLTK_DIR, 'tk8.3')
        if os.path.exists(temp_dir_tcl) and os.path.exists(temp_dir_tk):
            os.environ["TCL_LIBRARY"] = temp_dir_tcl
            os.environ["TK_LIBRARY"] = temp_dir_tk
            LIB = None

except:
    pass

#This is a very important line.  Includes the library's directory in the
# system search path.

if LIB:
    sys.path.insert(0, LIB)

#Make sure the environment has at least one copy of TCL/TK.
if not os.environ.get("TCL_LIBRARY", None):
    sys.stderr.write("Tcl library not found.\n")
    sys.exit(1)
if not os.environ.get("TK_LIBRARY", None):
    sys.stderr.write("Tk library not found.\n")
    sys.exit(1)

if not IMAGE_DIR:
    sys.stderr.write("IMAGE_DIR is not set.\n")
    sys.exit(1)

#print "_tkinter.so =", LIB
#print "TCL_LIBRARY =", os.environ["TCL_LIBRARY"]
#print "TK_LIBRARY =", os.environ["TK_LIBRARY"]

import Tkinter
import tkFont

#A lock to allow only one thread at a time access the display class instance.
display_lock = threading.Lock()
#queue_lock   = threading.Lock()
startup_lock = threading.Lock()
thread_lock  = threading.Lock()

CIRCULAR, LINEAR = range(2)
layout = LINEAR

ANIMATE = 1
STILL = 0

MMPC = 20.0     # Max Movers Per Column
MIPC = 20       # Max Items Per Column

REINIT_TIME = 3600000  #in milliseconds (1 hour)
#ANIMATE_TIME = 30      #in milliseconds (~1/33rd of second)
ANIMATE_TIME = 42      #in milliseconds (~1/42nd of second)
UPDATE_TIME = 1000     #in milliseconds (1 second)
MESSAGES_TIME = 250    #in milliseconds (1/4th of second)
JOIN_TIME = 10000      #in milliseconds (10 seconds)

status_request_threads = []

class Queue:
    def __init__(self):
        self.queue = []
        self.lock = threading.Lock()

    def len_queue(self):
        self.lock.acquire()
        
        number = len(self.queue)

        self.lock.release()

        return number

    def clear_queue(self):
        self.lock.acquire()
        
        #while len(self.queue):
        #    del self.queue[0]

        del self.queue[:]

        self.lock.release()
        
    def put_queue(self, queue_item, tid=None):
        self.lock.acquire()

        self.queue.append({'item' : queue_item, 'tid' : tid})

        self.lock.release()

    def get_queue(self, tid=None):
        self.lock.acquire()

        for i in range(len(self.queue)):
            try:
                temp = self.queue[i]
            except IndexError:
                temp = None

            if tid == None or (temp != None and temp['tid'] == tid):
                del self.queue[i]
                break
        else:
            temp = {'item': None}
            
        self.lock.release()

        return temp['item']

#These are the queue classes.
message_queue = Queue()
request_queue = Queue()
    
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
    'client_wait_color' :   rgbtohex(100, 100, 100),  # grey
    'client_active_color' : rgbtohex(0, 255, 0), # green
    'client_outline_color' : rgbtohex(0, 0, 0), # black
    'client_font_color' : rgbtohex(0, 0, 0), # white
    #mover colors
    'mover_color':          rgbtohex(0, 0, 0), # black
    'mover_error_color':    rgbtohex(255, 0, 0), # red
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
    'timer_color':          rgbtohex(255, 255, 255), # white
    'timer_longtime_color': rgbtohex(255, 0, 0), # red
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

_image_cache = {} #key is filename, value is (modtime, img)

def find_image(name):
    """Look in IMAGE_DIR for a file of the given name.  Cache already loaded image,
    but check modification time for file changes"""
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
    def __init__(self, name, display, index=0,N=0):
        #self.color         = None
        self.display       = display
        self.index         = index
        self.name          = name
        #self.N             = N
        #self.column        = 0 #Movers may be laid out in multiple columns
        self.state         = None

        #Classes that are used.
        self.volume        = None

        #Set geometry of mover.
        self.resize() #Even though this is the initial size, still works.
        self.x, self.y  = self.position(N)
        
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
        self.volume_display     = None
        self.volume_bg_display  = None
        self.rate_display       = None
        
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

        #Find out information about the mover.
        try:
            csc = self.display.csc_dict[self.name.split("@")[1]]
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

        #Carefull.  As long as draw_timer() gets called after animate_timer
        # in __init__() we are okay.
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
                                                    outline=self.library_color)
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

    def draw_state(self):
        x, y                    = self.x, self.y

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
        img          = find_image(self.state + '.gif')
        if self.state_display:
            #If current state is an image and the new state is an image.
            if img and self.display.type(self.state_display) == "image":
                self.display.coords(self.state_display,
                                    x+self.img_offset.x, y+self.img_offset.y)
                self.display.itemconfigure(self.state_display, image=img)
            #If currect state is in text and new state is an image.
            elif img and self.display.type(self.state_display) == "text":
                self.display.delete(self.state_display)
                self.state_display = self.display.create_image(
                    x+self.img_offset.x, y+self.img_offset.y,
                    anchor=Tkinter.NW, image=img)
            #If current state is an image and new state is in text.
            elif self.display.type(self.state_display) == "image":
                self.display.delete(self.state_display)
                self.state_display = self.display.create_text(
                x+self.state_offset.x, y+self.state_offset.y, font = self.font,
                text=fit_string(self.font, self.state, self.state_width),
                         fill=self.state_color, anchor=Tkinter.CENTER)
            #if current state is in text and the new state is in text.
            else:
                self.display.coords(self.state_display,
                                 x+self.state_offset.x, y+self.state_offset.y)
                self.display.itemconfigure(self.state_display,
                                           text=fit_string(self.font,
                                                           self.state,
                                                           self.state_width),
                                           anchor=Tkinter.CENTER,
                                           fill=self.state_color)
        #No currect state display.
        else:
            if img:
                self.state_display = self.display.create_image(
                    x+self.img_offset.x, y+self.img_offset.y,
                    anchor=Tkinter.NW, image=img)
            else:
                self.state_display = self.display.create_text(
                    x+self.state_offset.x, y+self.state_offset.y,
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
        x, y                    = self.x, self.y

        #Diaplay the volume.
        if self.volume:
            
            #If necessary define the font to use.
            if not self.volume_font:
                self.volume_font = get_font(self.vol_height, 'arial',
                                            fit_string=self.volume,
                                            width_wanted=self.vol_width)

            #Draw the volume background.
            if self.volume_bg_display:
                self.display.coords(
                    self.volume_bg_display, self.x + self.volume_offset.x,
                    self.y + self.volume_offset.y,
                    self.x + self.volume_offset.x + self.vol_width,
                    self.y + self.volume_offset.y + self.vol_height)
            else:
                self.volume_bg_display = self.display.create_rectangle(
                    self.x + self.volume_offset.x,
                    self.y + self.volume_offset.y,
                    self.x + self.volume_offset.x + self.vol_width,
                    self.y + self.volume_offset.y + self.vol_height,
                    fill = self.volume_bg_color,)
            
        
            if self.volume_display:
                self.display.coords(self.volume_display,
                                    x + self.volume_label_offset.x,
                                    y + self.volume_label_offset.y)
            else:
                self.volume_display = self.display.create_text(
                   self.x + self.volume_offset.x + (self.vol_width / 2.0) + 1,
                   self.y + self.volume_offset.y + (self.vol_height / 2.0) + 1,
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

        #The rate is usualy draw when the mover is in active state.
        # However, it can go into draining state while a transfer is in
        # progress.  The display of the draining state and rate can not
        # occur at the same time.  Give precdence to the state.
        if self.state == "DRAINING":
            self.undraw_rate()
        elif self.rate_display:
            self.display.itemconfigure(
                self.rate_display,
                text=fit_string(self.font, self.rate_string, self.state_width))
        elif self.rate != None:
            self.rate_display =  self.display.create_text(
                self.x + self.rate_offset.x, self.y + self.rate_offset.y,
                #Use the state with since this is displayed in that space.
                text=fit_string(self.font, self.rate_string, self.state_width),
                fill = colors('percent_color'),
                anchor = Tkinter.NE, font = self.font)

    def draw(self):

        self.draw_mover()

        self.draw_state()

        self.draw_timer()

        self.draw_progress() #Display the progress bar and percent done.
        
        self.draw_buffer()

        self.draw_volume()

        self.draw_rate()

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

    def undraw(self):
        self.undraw_timer()
        self.undraw_state()
        self.undraw_progress()
        self.undraw_buffer()
        self.undraw_volume()
        self.undraw_mover()
        self.undraw_rate()

    #########################################################################
        
    def update_state(self, state, time_in_state=0):
        if state == self.state:
            return
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

        self.draw_mover() #Some state changes change the mover color.
        self.draw_state()

        #Perform some cleanup in case some UDP Mmessages were lost.

        #If the mover should not have a volume, remove it.
        if state in ['IDLE', 'Unknown']:
            msg="need to unload tape because mover state changed to: %s"
            Trace.trace(2, msg % (state,))
            self.unload_tape()

        #If a transfer is not in progress, some things need to be undrawn.
        if state in ['ERROR', 'IDLE', 'OFFLINE', 'Unknown', 'HAVE_BOUND'
                     'FINISH_WRITE', 'CLEANING']:
            #If the connection line needs to be removed.
            if self.display.connections.get(self.name, None):
                msg="Need to disconnect because mover state changed to: %s"
                Trace.trace(2, msg % (state,))
                self.display.disconnect_command(
                    ["mover", self.name, "Unknown"])

        #Undraw these objects that correlate only to the ACTIVE/DRAINING
        # state.
        if state in ['ERROR', 'IDLE', 'OFFLINE', 'Unknown', 'HAVE_BOUND',
                     'FINISH_WRITE', 'CLEANING', 'SEEK', 'SETUP',
                     'DISMOUNT_WAIT', 'MOUNT_WAIT']:
            self.update_rate(None)
            self.update_progress(None, None)
            self.update_buffer(None)
        
    def update_timer(self, now):
        seconds = int(now - self.timer_started)
        if seconds == self.timer_seconds:
            return

        self.timer_seconds = seconds
        self.timer_string = HMS(seconds)

        #Only worry if this for "too long" situation.  When the state changes
        # this will be set back to normal color in update_state().
        if (self.timer_seconds > 3600) and \
           (self.state in ["ACTIVE", "SEEK", "SETUP", "loaded",
                           "MOUNT_WAIT", "DISMOUNT_WAIT", "FINISH_WRITE",
                           "HAVE_BOUND", "DRAINING", "CLEANING"]):
            self.timer_color = colors('timer_longtime_color')
            self.display.itemconfigure(self.timer_display,
                                       fill = self.timer_color)

        if self.timer_string:
            self.display.itemconfigure(self.timer_display,
                                       text = self.timer_string)
        else:
            print "Traceback avoided.  timer_string = %s" % self.timer_string

    def update_rate(self, rate):

        if rate == self.rate:
            return
            
        self.rate = rate

        if rate != None:
            self.rate_string = "%.2f MB/S" % (self.rate / 1048576)
            self.draw_rate()
        else:
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

    def position_circular(self, N):
        k = self.index
        if N == 1: ## special positioning for a single mover.
            k = 1
            angle = math.pi / 2
        else:
            angle = math.pi / (N-1)
        i=(0+1J)
        coord=.75+.8*cmath.exp(i*(math.pi/2 + angle*k))
        return scale_to_display(coord.real, coord.imag, self.display.width,
                                self.display.height)

    def position_linear(self, N):
        #N = number of movers

        position = self.display.get_mover_position(self.name)
        
        #k = self.index  # k = number of this movers
        mmpc = float(MMPC) #Maximum movers per column

        #total number of columns 
        num_cols = self.display.get_total_column_count()
        #total number of rows in the largest column
        num_rows = self.display.get_mover_maximum_column_count()
        #this mover's column
        column = position[0]
        #this mover's row
        row = self.display.mover_positions[column].get_index(self.name)

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
        
    def position(self, N):
        if layout==CIRCULAR:
            return self.position_circular(N)
        elif layout==LINEAR:
            return self.position_linear(N)
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
        self.font = get_font(self.height/3.5, #'arial',
                             width_wanted=self.max_font_width(),
                             fit_string="DISMOUNT_WAIT")

        #Size of the volume portion of mover display.
        self.vol_height = (self.height)/2.5
        self.volume_font = get_font(self.vol_height, #'arial',
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
        self.buffer_bar_height     = self.height/10
        self.progress_bar_offset1  = XY(4, self.height - 2 - self.bar_height)
        self.progress_bar_offset2  = XY(4, self.height - 2)#yellow
        self.buffer_bar_offset1 = XY(4, self.height - 4 -
                                     self.buffer_bar_height - self.bar_height)
        self.buffer_bar_offset2 = XY(4, self.height - 4 -
                                     self.bar_height)#magenta
        self.percent_disp_offset   = XY(self.bar_width + 6, self.height)#green


    def max_font_width(self):
        return (self.width - self.width/3.0) - 10

    def max_label_font_width(self):
        #total number of columns 
        num_cols = self.display.get_total_column_count()
        #size of column in pixels
        column_width = (self.display.width / float(num_cols + 1))
        #difference of column width and mover rectangle with fudge factor.
        return (column_width - self.width) - 10

    def reposition(self, N): #, state=None):
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
        name = self.name.split("@")[0]
        csc = self.display.csc_dict[self.name.split("@")[1]]
        mov = mover_client.MoverClient(csc, name + ".mover",
                flags=enstore_constants.NO_ALARM | enstore_constants.NO_LOG,)
        return mov
        
    def get_status(self):
        mov = self.get_mover_client()
        status = mov.status(rcv_timeout=3, tries=3)

        if e_errors.is_ok(status):
            commands = self.handle_status(self.name, status)
            if not commands:
                return
            for command in commands:
                #Queue the command.  Calling handle_command() directly here
                # results in startup problems, since get_status is called
                # before __init__ is finished (and the object is added to
                # the list of movers).
                self.display.queue_command(command)
                #message_queue.put_message(command, mov.name.split("@")[-1])

#########################################################################
##
#########################################################################

class Client:

    def __init__(self, name, display):
        self.name               = name
        self.display            = display
        self.n_connections      = 0
        self.last_activity_time = time.time()
        self.waiting            = 0
        self.label              = None
        self.outline            = None

        self.resize()
        self.position()
        self.update_state()
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

    def update_state(self):

        ### color

        self.font_color = colors('client_font_color')
        
        if self.waiting:
            self.color = colors('client_wait_color')
        else:
            self.color = colors('client_active_color')

        #If configured in the .entvrc file, else use black.
        self.outline_color = self.display.get_client_color(self.name)

        #If the object is already drawn; update it.
        if self.outline:
            self.display.itemconfigure(self.outline,
                                       fill = self.color,
                                       outline = self.outline_color) 

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

        self.position()

    #########################################################################
        
    def draw(self):

        if not self.color:
            self.color = self.display.get_client_color(self.client.name)

        if self.line:
            self.display.coords(self.line, tuple(self.path))
            self.display.itemconfigure(self.line, dashoffset = self.dashoffset)
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
        # includeing when it will have now effect.

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
        now                       = time.time()
        self.segment_start_time   = now     #starting time at this rate
        self.segment_stop_time    = now + 5 #let the animation run 5 seconds
        #self.segment_start_offset = self.dashoffset
        self.rate                 = rate

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
            print "An unknown error occured.  Please send the enstore" \
                  "developers the following output."
            print "ERROR:", position, self.client.name, self.mover.name
            pprint.pprint(self.display.clients)
            for i in range(len(self.display.client_positions.keys())):
                print "COLUMN:", i + 1
                pprint.pprint(self.display.client_positions[i + 1].item_positions)

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
    def __init__(self, mover, **attributes):
        if not hasattr(self, "state_display"):
            Tkinter.Toplevel.__init__(self)
            self.configure(attributes)
            #Font geometry.
            self.font = get_font(12)  #, 'arial')
        
        #Tell it to set the remaining configuration values and to apply them.
        self.title(mover.name)

        self.mover_name = mover.name
        self.display = mover.display
        self.state_display = None
        self.after_mover_diplay_id = None

        self.status_text = self.format_mover_status(self.get_mover_status())

        #When the window is closed, we have some things to cleanup.
        self.bind('<Destroy>', self.window_killed)

        self.update_status()

    def reinit(self, mover = None, display = None):

        self.after_cancel(self.after_mover_diplay_id)
        
        if display:
            self.display = display
        if mover:
            self.mover_name = mover.name
        
        self.status_text = self.format_mover_status(self.get_mover_status())

        self.update_status()

    def window_killed(self, event):
        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        #With the window closed, don't do an update.
        self.after_cancel(self.after_mover_diplay_id)

        #Clear this to avoid a cyclic reference.
        self.display = None

    def draw(self):

        try:
            self.state_display.configure(text = self.status_text,
                                         foreground = self.state_color,
                                         background = self.mover_color,)
            self.state_display.pack(side=Tkinter.LEFT, expand=Tkinter.YES,
                                    fill=Tkinter.BOTH)
        except:
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
        except AttributeError, msg:
            pass
        except Tkinter.TclError, msg:
            pass

    def get_mover_status(self):
        #Find out information about the mover.  self.mover.name must be
        # a string of the format like: mover31@stken
        mover_name, system_name = tuple(self.mover_name.split("@"))
        mover_name = mover_name + ".mover"
        try:
            csc = self.display.csc_dict[system_name]
        except (KeyError, AttributeError), msg:
            return {'status' : (e_errors.DOESNOTEXIST, None),
                    'state' : "ERROR"}
        mov = mover_client.MoverClient(csc, mover_name)
        status = mov.status(rcv_timeout=5, tries=1)

        #In case of timeout, set the state to Unknown.
        if status.get('state', None) == None:
            status['state'] = "Unknown"
        
        return status

    def format_mover_status(self, status_dict):
        order = status_dict.keys()
        order.sort()
        msg = ""
        for item in order:
            msg = msg + "%s: %s\n" % (item, pprint.pformat(status_dict[item]))
        return msg

    def update_status(self):
        status = self.get_mover_status()
        self.status_text = self.format_mover_status(status)
        
        if status['state'] in ['ERROR']:
            self.state_color = colors('state_error_color')
            self.mover_color = colors('mover_error_color')
        elif status['state'] in ['OFFLINE']:
            self.state_color = colors('state_offline_color')
            self.mover_color = colors('mover_offline_color')
        elif status['state'] in ['IDLE', 'Unknown']:
            self.state_color = colors('state_idle_color')
            self.mover_color = colors('mover_stable_color')
        else:
            self.state_color =colors('state_stable_color')
            self.mover_color = colors('mover_stable_color')

        self.draw()
        
        #Reset the time for 5 seconds.
        self.after_mover_diplay_id = self.after(5000, self.update_status)

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
        self.column_limit = MIPC

    def get_index(self, item_name):
        for index in self.item_positions.keys():
            if self.item_positions[index] == item_name:
                return index

        return None

    def get_name(self, item_index):
        return self.item_positions.get(item_index, None)

    def get_max_limit(self):
        return self.column_limit

    def set_max_limit(self, limit):
        if type(limit) == types.IntType and limit > 0 and limit <= MMPC:
            self.column_limit = limit
            
    def add_item(self, item_name):
        
        if len(self.item_positions.keys()) >= self.column_limit:
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

        if i < -10 or i > 10:
            return True
        
        self.item_positions[i] = item_name
        return False

    def add_seq_item(self, item_name):
        i = 0
        
        while self.item_positions.has_key(i):
            i = i + 1

        if i > 20:
            return True

        self.item_positions[i] = item_name
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
    def __init__(self, entvrc_info, master = None, mover_display = None,
                 **attributes):
        if not hasattr(self, "master"):
            self.master = master
            reinited = 0
        else:
            reinited = 1

        title = entvrc_info.get('title', "Enstore")
        self.library_colors = entvrc_info.get('library_colors', {})
        self.client_colors = entvrc_info.get('client_colors', {})

        self.csc_dict = {}

        #Only call Tkinter.Canvas.__init__() on the first time through.
        if not reinited:
            if master:
                self.master_geometry = self.master.geometry()
                Tkinter.Canvas.__init__(self, master = master)
                self.master.title(title)
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
                master.entv_do_animation.set(ANIMATE)

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

        #Clear the window for drawing to the screen.
        self.pack(expand = 1, fill = Tkinter.BOTH)
        self.update()

        #Force the specific mover display to be reinitialized.
        if mover_display:
            mover_display.reinit(display = self)
            self.mover_display = mover_display
        else:
            self.mover_display = None

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
        x, y = self.canvasx(event.x), self.canvasy(event.y)
        overlapping = self.find_overlapping(x-1, y-1, x+1, y+1)
        Trace.trace(1, "%s %s" % (overlapping, (x, y)))

        #Display detailed mover information.
        for mover in self.movers.values():
            for i in range(len(overlapping)):
                if mover.state_display == overlapping[i]:
                    #If the window already exits; reuse it.
                    if getattr(self, "mover_display", None):
                        #self.mover_display.__init__(mover=mover)
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
                                                 
    def resize(self, event):
        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"
        
        Trace.trace(1, "New dimensions: %s" % self.master.wm_geometry())
        self.master_geometry = self.master.geometry()

        try:
            self.after_cancel(self.after_smooth_animation_id)
            self.after_cancel(self.after_clients_id)
            self.after_cancel(self.after_reinitialize_id)
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

    def reinitialize(self, event=None):
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

    def print_canvas(self, event):
        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"
        
        self.postscript(file="/home/zalokar/entv.ps", pagewidth="8.25i")

    def window_killed(self, event):
        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        self.stopped = 1

        #Since the window has been closed, it makes no sense to continue
        # to update this information.  It is a major performace killer
        # to leave these running.
        self.after_cancel(self.after_smooth_animation_id)
        self.after_cancel(self.after_clients_id)
        self.after_cancel(self.after_reinitialize_id)
        self.after_cancel(self.after_process_messages_id)
        self.after_cancel(self.after_join_id)

        for mov in self.movers.values():
            self.after_cancel(mov.timer_id)

        #if self.mover_display:
        #    self.mover_display.destroy()
        #    self.mover_display = None
            
        self.master_geometry = self.master.geometry()

        self.cleanup_display()

    def visibility (self, event):
        #This is a callback function that must take as arguments self and
        # event.  Thus, turn off the unused args test in pychecker.
        __pychecker__ = "no-argsused"

        self.master_geometry = self.master.geometry()
        
    #########################################################################

    def reposition_canvas(self, force = None):
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
        
    def reposition_movers(self, number_of_movers=None):
        items = self.movers.values()
        if number_of_movers:
            N = number_of_movers
        else:
            N = len(items) #need this to determine positioning
        self.mover_label_width = None
        for mover in items:
            mover.reposition(N)            
         
    def reposition_clients(self):
        for client in self.clients.values():
            client.reposition()

    def reposition_connections(self):
        for connection in self.connections.values():
            connection.reposition()

    #########################################################################

    #Called from smooth_animation().
    def connection_animation(self):

        #If the user turned off animation, don't do it.
        #if not self.animate:
        if self.master and self.master.entv_do_animation.get() == STILL: 
            return
        
        now = time.time()
        #### Update all connections.
        for connection in self.connections.values():
            connection.animate(now)

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

    #Called from self.after().
    def process_messages(self):
        if self.stopped: #If we should stop, then stop.
            return

        #Only process the messages in the queue at this time.
        number = min(message_queue.len_queue(), MIPC)
        #queue_lock.acquire()
        #number = min(len(self.command_queue), MIPC)
        #queue_lock.release()

        while number > 0:

            #Words is a list of the split string command.
            command = self.get_valid_command()

            #If a datagram gets dropped, attempt to recover the lost
            # information by asking for it.
            if not self.is_up_to_date(command):
                result = startup_lock.acquire(0)
                if result:
                    words = command.split()
                    request_queue.put_queue(words[1],
                                            tid = words[1].split("@")[-1])
                    #self.put_request(words[1], words[1].split("@")[-1])
                    startup_lock.release()
            
            #Process the next item in the queue.
            self.handle_command(command)
            
            number = number - 1

            if self.stopped:
                return

        #Schedule the next animation.
        if not self.stopped:
            self.after_process_messages_id = self.after(MESSAGES_TIME,
                                                        self.process_messages)

        return
        
    #Called from self.after().
    def smooth_animation(self):
        #If necessary, process the animation of the connections lines.
        self.connection_animation()

        #Schedule the next animation.
        self.after_smooth_animation_id = self.after(ANIMATE_TIME,
                                                    self.smooth_animation)

    #Called from self.after().
    def disconnect_clients(self):
        now = time.time()
        #### Check for unconnected clients
        for client_name, client in self.clients.items():
            if (client.n_connections > 0 or client.waiting == 1):
                continue
            if now - client.last_activity_time > 5: # grace period
                Trace.trace(2, "It's been longer than 5 seconds, %s " \
                            " client must be deleted" % (client_name,))
                client.undraw()
                try:
                    # Remove client from the client list.
                    del self.clients[client_name]
                except KeyError:
                    pass
                try:
                    # Mark this spot as unoccupied.
                    self.del_client_position(client_name)
                except KeyError:
                    pass

        self.after_clients_id = self.after(UPDATE_TIME,
                                           self.disconnect_clients)

    #Called from join_thread().
    def _join_thread(self, waitall = None):
        global status_request_threads
        
        thread_lock.acquire()

        #del_list = []
        alive_list = []
        for i in range(len(status_request_threads)):
            if waitall:
                status_request_threads[i].join()
            else:
                status_request_threads[i].join(0.0)
            if status_request_threads[i].isAlive():
                alive_list.append(status_request_threads[i])
            #else:
            #    del_list.append(i)

        #del status_request_threads[0]
        status_request_threads = alive_list

        thread_lock.release()

    #Called from self.after().
    def join_thread(self):
        __pychecker__ = "unusednames=i"

        self._join_thread()
        
        self.after_join_id = self.after(JOIN_TIME, self.join_thread)

    #Called from entv.handle_periodic_actions().
    #def handle_titling(self):
    #
    #    now = time.time()
    #    #### Handle titling
    #    if self.title_animation:
    #        if now > self.title_animation.stop_time:
    #            self.title_animation = None
    #        else:
    #            self.title_animation.animate(now)
    #
    #    ####force the display to refresh
    #    self.update()

    #########################################################################
    
    def create_movers(self, mover_names):
        #Shorten the number of movers.
        N = len(mover_names)

        #Make sure to reserve the movers' positions before creating them.
        self.reserve_mover_columns(N)

        #Create a Mover class instance to represent each mover.
        for k in range(N):
            mover_name = mover_names[k]
            self.add_mover_position(mover_name)
            self.movers[mover_name] = Mover(mover_name, self, index=k, N=N)

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
        if self.library_colors.get(library, None):
            return self.library_colors[library]

        self.library_colors[library] = rgbtohex(0, 0, 0)

        return self.library_colors[library]

    def get_client_color(self, client):
        #If this is the first client.
        if not getattr(self, "client_colors", None):
            self.client_colors = {}

        #If this mover's library is already remembered.
        if self.client_colors.get(client, None):
            return self.client_colors[client]

        self.client_colors[client] = colors('client_outline_color')

        return self.client_colors[client]

    #########################################################################

    def add_client_position(self, client_name):
        #Start searching the existing columns for the client's name.
        for i in range(len(self.client_positions) + 1)[1:]:
            if self.client_positions[i].has_item(client_name):
                return

        #The variable search_order is a list of two-tuples, where each
        # two-tuple consits of the number in each column and the index number
        # of that column.  The sort after the loop, first sorts by the number
        # of clients in a column.  If there are ties, it sorts the lowest
        # column number first.
        search_order = []
        for i in range(len(self.client_positions) + 1)[1:]:
            search_order.append((self.client_positions[i].count(), i))
        search_order.sort() #sort into ascending order.

        for t in search_order:
            rtn = self.client_positions[t[1]].add_item(client_name)
            if rtn: #Filled the column, search the next one.
                continue
            #Otherwise return success.
            return
        else:
            #Need to add another column.
            index = len(self.client_positions) + 1
            self.client_positions[index] = Column(index, CLIENTS)
            #The following line is temporary
            self.client_positions[index].add_item(client_name)

        return

    def del_client_position(self, client_name):
        #Start searching the existing columns for the existing slot.
        for i in range(len(self.client_positions) + 1)[1:]:
            self.client_positions[i].del_item(client_name)

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
            columns_search = range(len(self.mover_positions) + 1)[1:]
        else:
            columns_search = [column]
        
        sum = 0
        for i in columns_search:
            sum = sum + self.client_positions[i].count()

        return sum

    def get_client_column_count(self):
        if len(self.client_positions) < 1:
            return 1  #Always assume at least one column of clients.
        else:
            return len(self.client_positions)


    def add_mover_position(self, mover_name):
        #Start searching the existing columns for the movers's name.
        for i in range(len(self.mover_positions) + 1)[1:]:
            if self.mover_positions[i].has_item(mover_name):
                return

        for i in range(len(self.mover_positions) + 1)[1:]:
            rtn = self.mover_positions[i].add_item(mover_name)
            if rtn: #Filled the column, search the next one.
                continue
            #Otherwise return success.
            #return
            break
        else:
            #Need to add another column.
            index = len(self.mover_positions) + 1
            self.mover_positions[index] = Column(index, MOVERS)
            self.mover_positions[index].add_item(mover_name)
            #self.reposition_canvas()

        return

    def del_mover_position(self, mover_name):
        #Start searching the existing columns for the existing slot.
        for i in range(len(self.mover_positions) + 1)[1:]:
            self.mover_positions[i].del_item(mover_name)

    def get_mover_position(self, mover_name):
        for i in range(len(self.mover_positions) + 1)[1:]:
            i2 = self.mover_positions[i].get_index(mover_name)
            if i2 != None:
                return (i, i2) #Tuple of the column number and row index. 

        return None

    def get_mover_name(self, position):
        return self.mover_positions[position[0]].get_name(position[1])
    
    def get_mover_count(self, column = None):
        if column == None:
            columns_search = range(len(self.mover_positions) + 1)[1:]
        else:
            columns_search = [column]
        
        sum = 0
        for i in columns_search:
            sum = sum + self.mover_positions[i].count()

        return sum

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
        limits = {}
        min_count = int(number) / int(columns)
        for i in range(1, columns + 1):
            limits[i] = min_count
        #Second, take the remainder of movers and set them one at a time
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
            self.mover_positions[i] = Column(i, MOVERS)
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
        
        name = self.get_mover_name(position)

        mover = self.movers.get(name)

        try:
            return mover.x, mover.y
        except AttributeError:
            return None
        
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

    def csc_command(self, command_list):
        try:
            csc = configuration_client.ConfigurationClient((command_list[1],
                                                         int(command_list[2])))
            csc.dump_and_save()
            csc.new_config_obj.enable_caching()
            
            #Before blindly setting the value.  Make sure that it is good.
            rtn = csc.get_enstore_system(3, 5)
            if rtn:
                self.csc_dict[rtn] = csc
            else:
                #This is rather harsh, but hopefully will fix all 'major'
                #  failures.  Wait 1 minutes before starting over.
                time.sleep(60)
                self.queue_command("reinit")
        except KeyboardInterrupt:
            raise sys.exc_info()
        except:
            exc, msg = sys.exc_info()[:2]
            print "Error processing %s: %s" % (str(command_list),
                                               (str(exc), str(msg)))

    def client_command(self, command_list):
        ## For now, don't draw waiting clients (there are just
        ## too many of them)
        return
            
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
                self.reposition_canvas()
            
            client.waiting = 1
            client.update_state() #change fill color if needed
            client.draw()

    def connect_command(self, command_list):

        now = time.time()

        mover = self.movers.get(command_list[1])
        if mover.state in ['ERROR', 'IDLE', 'OFFLINE']:
            Trace.trace(2,
                "Cannot connect to mover that is %s." % (mover.state,))
            return

        #Draw the client.
        
        client_name = normalize_name(command_list[2])
        client = self.clients.get(client_name)
        if not client: ## New client, we must add it
            old_number = self.get_client_column_count()

            self.add_client_position(client_name)
            client = Client(client_name, self)
            self.clients[client_name] = client
            #If the client command is ever used, these lines are necessary.
            #   client.waiting = 0
            #   client.update_state() #change fill color if needed
            client.draw()  #Draws the client.

            #If the number of client columns changed we need to reposition.
            new_number = self.get_client_column_count()
            if old_number != new_number:
                self.reposition_canvas(force = 1)
        else:
            client.waiting = 0
            client.update_state() #change fill color if needed

        #Draw the connection.

        #First test if a connection is already present.
        if self.connections.get(mover.name, None):
            connection = self.connections[mover.name]
            #Decrease the old clients connection count.
            old_client = connection.client
            old_client.n_connections = old_client.n_connections - 1
            #Take the existing connection and make it like new.
            connection.undraw()
            connection.__init__(mover, client, self)
            #Increase the number of connections this client has.
            client.n_connections = client.n_connections + 1
        #If not create a new connection.
        else:
            connection = Connection(mover, client, self)
            self.connections[mover.name] = connection
            connection.update_rate(0)
            #Increase the number of connections this client has.
            client.n_connections = client.n_connections + 1
        connection.draw() #Either draw or redraw correctly.

        ###What are these for?
        mover.t0 = now
        mover.b0 = 0L
                
    def disconnect_command(self, command_list):
        Trace.trace(2, "mover %s is disconnecting from %s" %
                    (command_list[1], command_list[2]))
        
        mover = self.movers.get(command_list[1])

        client_name = normalize_name(command_list[2])
        client = self.clients.get(client_name)

        #Decrease the number of connections this client has.
        try:
            client.n_connections = client.n_connections - 1
        except (AttributeError, KeyError):
            pass
        
        #Remove all references to the connection.
        try:
            self.connections[mover.name].undraw()
        except (AttributeError, KeyError, Tkinter.TclError):
            pass
        try:
            del self.connections[mover.name]
        except (AttributeError, KeyError):
            pass

        #Remove the progress bar.
        if mover == None:
            mover.update_progress(None, None)
            mover.update_buffer(None)
            mover.update_rate(None)
                
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

        #If the mover sends the buffer size info. display the bar.
        try:
            #Redraw/reposition the buffer bar.
            mover.update_buffer(command_list[5])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()
        except:
            pass

        #Skip media transfers from the network connection update.
        connection = self.connections.get(mover.name, None)
        if connection and command_list[4] == "network":
            #Calculate the new instantanious rate.
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
                connection.update_rate(rate / (256*1024))

        if connection:
            #Don't use the variable "now" here.  We want to use the local
            # machine's time for this measure.
            connection.client.last_activity_time = time.time()

    def movers_command(self, command_list):
        self.number_of_movers = len(command_list[1:])
        self.create_movers(command_list[1:])

    def newconfig_command(self, command_list):
        __pychecker__ = "no-argsused"  #Supress pychecker warning.
        
        self.reinitialize()

    #########################################################################

    def queue_command(self, command):
        command = string.strip(command) #get rid of extra blanks and newlines
        words = string.split(command)

        if words[0] in self.comm_dict.keys():
            #Under normal situations.
            message_queue.put_queue(command)

    #      connect MOVER_NAME CLIENT_NAME
    #      disconnect MOVER_NAME CLIENT_NAME
    #      loaded MOVER_NAME VOLUME_NAME
    #      loading MOVER_NAME VOLUME_NAME
    #      #moveto MOVER_NAME VOLUME_NAME
    #      #remove MOVER_NAME VOLUME_NAME
    #      state MOVER_NAME STATE_NAME [TIME_IN_STATE]
    #      unload MOVER_NAME VOLUME_NAME
    #      transfer MOVER_NAME BYTES_TRANSFERED BYTES_TO_TRANSFER \
    #               (media | network) [BUFFER_SIZE] [CURRENT_TIME]
    #      movers MOVER_NAME_1 MOVER_NAME_2 ...MOVER_NAME_N
    comm_dict = {'quit' : {'function':quit_command, 'length':1},
                 'reinit': {'function':reinitialize, 'length':1},
                 'title' : {'function':title_command, 'length':1},
                 'csc' : {'function':csc_command, 'length':2},
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
                 'unload': {'function':unload_command, 'length':3,
                              'mover_check':1},                            
                 'transfer' : {'function':transfer_command, 'length':3,
                              'mover_check':1},                               
                 'movers' : {'function':movers_command, 'length':2},
                 'newconfigfile' : {'function':newconfig_command, 'length':1}}

    def get_valid_command(self):  #, command):
        command = message_queue.get_queue()
        if not command:
            return ""

        command = string.strip(command) #get rid of extra blanks and newlines
        words = string.split(command)

        if not words: #input was blank, nothing to do!
            return ""  #[]

        if words[0] not in self.comm_dict.keys():
            return ""  #[]

        #Don't bother processing transfer messages if we are not keeping up.
        if words[0] in ["transfer"] and \
           message_queue.len_queue() > max(20, (len(self.movers))):
            return ""  #[]

        if len(words) < self.comm_dict[words[0]]['length']:
            Trace.trace(1, "Insufficent length for %s command." % (words[0],))
            return ""  #[]

        if self.comm_dict[words[0]].get('mover_check', None) and \
           not self.movers.get(words[1]):
            #This is an error, a message from a mover we never heard of
            Trace.trace(1, "Don't recognize mover %s, continuing ...."
                        % words[1])
            return ""  #[]

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
            Trace.message(10, string.join((time.ctime(), command), " "))

            #Run the corresponding function.
            display_lock.acquire()
            apply(self.comm_dict[words[0]]['function'], (self, words,))
            display_lock.release()

    #########################################################################

    #overloaded
    #def destroy(self):
    #    Tkinter.Canvas.destroy(self)
    
    def cleanup_display(self):
        global _font_cache
        global _image_cache
        global message_queue, request_queue

        self._join_thread(waitall = True)

        #Undraw the diplay before deleting the objects (in the lists).
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
        for key in self.clients.keys():
            try:
                self.clients[key].display = None
                del self.clients[key]
            except KeyError:
                Trace.trace(1, "Unable to remove client %s" % key)
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
        
        #When a reinitialization occurs, there is a resource leak.
        for key in self.csc_dict.keys():
            try:
                del self.csc_dict[key]
            except (AttributeError, KeyError):
                pass

        self.csc_dict = {}

        #Perform the following two deletes explicitly to avoid obnoxious
        # tkinter warning messages printed to the terminal when using
        # python 2.2.
        for key in _font_cache.keys():
            try:
                del _font_cache[key]
            except:
                exc, msg = sys.exc_info()[:2]
                Trace.trace(1, "ERROR: %s: %s" % (str(exc), str(msg)))

        for key in _image_cache.keys():
            try:
                del _image_cache[key]
            except:
                exc, msg = sys.exc_info()[:2]
                Trace.trace(1, "ERROR: %s: %s" % (str(exc), str(msg)))

        _font_cache = {}
        _image_cache = {}

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

    def mainloop(self, threshold = None):
        #self.reposition_canvas(force = True)
        #self.pack(expand = 1, fill = Tkinter.BOTH)
        #If the window does not yet exist, draw it.  This should only
        # be true the first time mainloop() gets called; for
        # reinitializations this should not happen.
        if self.master.state() == "withdrawn":
            self.reposition_canvas(force = True)
            #This tells the window to let the Canvas fill the entire window.
            # When this is here, it is one factor to having the first drawing
            # look correct (among other factors).
            
            self.master.deiconify()
            self.master.lift()
            self.master.update()
        
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

        #Since, the startup_lock is still held, we have nothing yet to do.
        # This would be a good time to cleanup before things get hairy.
        gc.collect()
        del gc.garbage[:]

        #Let the other startup threads go.
        try:
            startup_lock.release()
        except:
            #Will get here if run from enstore_display.py not entv.py.
            pass

        self.after_reposition_id = None
        if threshold == None:
            self.master.mainloop()
        else:
            #Threshold is the number of "MainWindows" allowed.  Should be
            # an integer.
            self.master.mainloop(threshold)

#########################################################################

if __name__ == "__main__":
    if len(sys.argv)>1:
        title = sys.argv[1]
    else:
        title = "Enstore"

    display = Display({}, background=rgbtohex(173, 216, 230))
    display.mainloop()


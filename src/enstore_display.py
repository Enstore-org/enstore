#!/usr/bin/env python

import pprint
import cmath
import exceptions
import math
import os
import select
import socket
import string
import sys
import time
import stat
import event_relay_client
import event_relay_messages
import Trace
import mover_client
# from Tkinter import *
# import tkFont


#Set up paths to find our private copy of tcl/tk 8.3
ENSTORE_DIR=os.environ.get("ENSTORE_DIR")
TCLTK_DIR=None
if ENSTORE_DIR:
    TCLTK_DIR=os.path.join(ENSTORE_DIR, 'etc','TclTk')
if TCLTK_DIR is None or not os.path.exists(TCLTK_DIR):
    TCLTK_DIR=os.path.normpath(os.path.join(os.getcwd(),'..','etc','TclTk'))
os.environ["TCL_LIBRARY"]=os.path.join(TCLTK_DIR, 'tcl8.3')
os.environ["TK_LIBRARY"]=os.path.join(TCLTK_DIR, 'tk8.3')
sys.path.insert(0, os.path.join(TCLTK_DIR, sys.platform))

IMAGE_DIR=None
if ENSTORE_DIR:
    IMAGE_DIR=os.path.join(ENSTORE_DIR, 'etc', 'Images')
if IMAGE_DIR is None or not os.path.exists(IMAGE_DIR):
    IMAGE_DIR=os.path.normpath(os.path.join(os.getcwd(),'..','etc','Images'))

##print "IMAGE_DIR=", IMAGE_DIR
    
import Tkinter
import tkFont
import entv
import threading

#A lock to allow only one thread at a time access the display class instance.
display_lock = threading.Lock()

CIRCULAR, LINEAR = range(2)
layout = LINEAR

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

def my_atof(s):
    if s[-1] == 'L':
        s = s[:-1] #chop off any trailing "L"
    return string.atof(s)

_font_cache = {}

def get_font(height_wanted, family='arial', fit_string="", width_wanted=0):

    height_wanted = int(height_wanted)

    f = _font_cache.get((height_wanted, width_wanted, len(fit_string), family))
    if f:
        if width_wanted and f.measure(fit_string) > width_wanted:
            pass
        else:
            return f

    size = height_wanted
    while size > 0:
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

color_dict = {
    #client colors
    'client_wait_color' :   rgbtohex(100, 100, 100),  # grey
    'client_active_color' : rgbtohex(0, 255, 0), # green
    #mover colors
    'mover_color':          rgbtohex(0, 0, 0), # black
    'mover_error_color':    rgbtohex(255, 0, 0), # red
    'mover_offline_color':  rgbtohex(169, 169, 169), # grey
    'mover_stable_color':   rgbtohex(0, 0, 0), # black
    'percent_color':        rgbtohex(0, 255, 0), # green
    'progress_bar_color':   rgbtohex(255, 255, 0), # yellow
    'progress_bg_color':    rgbtohex(255, 0, 255), # magenta
    'state_stable_color':   rgbtohex(255, 192, 0), # orange
    'state_idle_color':     rgbtohex(191, 239, 255), # lightblue
    'state_error_color':    rgbtohex(0, 0, 0), # black
    'state_offline_color':  rgbtohex(0, 0, 0), # black
    'timer_color':          rgbtohex(255, 255, 255), # white
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
        except:
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
        except:
            del _image_cache[name]
            img = None
    if not img: # Need to load it
        try:
            statinfo = os.stat(filename)
            file_mtime = statinfo[stat.ST_MTIME]
            img = Tkinter.PhotoImage(file=filename)
            _image_cache[name] = file_mtime, img #keep track of image and modification time
        except:
            img = None
    return img
    

class XY:
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    
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
#     load_tape() - tape gets loaded onto mover:
#                        gray indicates robot recognizes tape and loaded it
#                        orange indicates when mover actually recognizes tape
#     unload_tape() - will unload tape to side of each mover, ready for
#                                 robot to remove f/screen
#     transfer_rate() - rate at which transfer being sent; calculates a rate
#     undraw() - undraws the features fromthe movers
#     position() - calculates the position for each mover
#     reposition() - reposition each feature after screen has been moved
#     __del__() - calls undraw() module and deletes features
#
#########################################################################
class Mover:
    def __init__(self, name, display, index=0,N=0):
        #self.color         = None
        self.display       = display
        self.index         = index
        self.name          = name
        self.N             = N
        self.column        = 0 #Movers may be laid out in multiple columns
        self.state         = None

        #Classes that are used.
        self.connection    = None         
        self.volume        = None

        #Set geometry of mover.
        self.resize(N) #Even though this is the initial size, still works.
        self.x, self.y  = self.position(N)
        
        #These 3 pieces make up the progress gauge display
        self.progress_bar             = None
        self.progress_bar_bg          = None
        self.progress_percent_display = None
        # This is the numeric value.  "None" means don't show the progress bar.
        self.percent_done = None

        #These are other display items.
        self.outline            = None
        self.label              = None
        self.timer_display      = None
        self.state_display      = None
        self.volume_display     = None
        self.volume_bg_display  = None
        
        # Anything that deals with time
        self.b0                 = 0
        now                     = time.time()
        self.last_activity_time = now
        self.rate               = 0.0
        self.t0                 = 0
        self.timer_seconds      = 0
        self.timer_started      = now
        self.timer_string       = '00:00:00'

        self.update_state("Unknown")
        
        self.draw()

    def __del__(self):
        if self.connection:
            self.connection = None
        try:
            self.undraw()
        except Tkinter.TclError:
            pass #internal Tcl problems.

#########################################################################

    def draw_mover(self):
        x, y                    = self.x, self.y

        #Display the mover rectangle.
        if self.outline:
            self.display.coords(self.outline, x, y,
                                x+self.width, y+self.height)
            self.display.itemconfigure(self.outline, fill=self.mover_color)
        else:
            self.outline = self.display.create_rectangle(x, y, x+self.width,
                                                         y+self.height,
                                                         fill=self.mover_color)
        #Display the mover name label.
        if self.label:
            self.display.coords(self.label, x+self.label_offset.x,
                                y+self.label_offset.y)
        else:
            self.label   = self.display.create_text(x+self.label_offset.x,
                                                    y+self.label_offset.y,
                                                    text=self.name,
                                                    anchor=Tkinter.SW,
                                                    font = self.label_font)

    def draw_state(self):
        x, y                    = self.x, self.y

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
                text=self.state, fill=self.state_color)
            #if current state is in text and the new state is in text.
            else:
                self.display.coords(self.state_display,
                                 x+self.state_offset.x, y+self.state_offset.y)
                self.display.itemconfigure(self.state_display,
                                        text=self.state, fill=self.state_color)
        #No currect state display.
        else:
            if img:
                self.state_display = self.display.create_image(
                    x+self.img_offset.x, y+self.img_offset.y,
                    anchor=Tkinter.NW, image=img)
            else:
                self.state_display = self.display.create_text(
                    x+self.state_offset.x, y+self.state_offset.y,
                    font = self.font, text=self.state, fill=self.state_color)

    def draw_timer(self):
        if self.timer_display:
            self.display.itemconfigure(self.timer_display,
                                       text=self.timer_string)
        else:
            self.timer_display = self.display.create_text(
                self.x + self.timer_offset.x, self.y + self.timer_offset.y,
                text = self.timer_string, fill = self.timer_color,
                font = self.font)

    def draw_volume(self):
        x, y                    = self.x, self.y

        #Diaplay the volume.
        if self.volume:

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
                    fill = self.volume_bg_color)
            
        
            if self.volume_display:
                self.display.coords(self.volume_display,
                            x+self.volume_offset.x+(self.vol_width / 2.0) + 1,
                            y+self.volume_offset.y+(self.vol_height / 2.0) + 1)
            else:
                self.volume_display = self.display.create_text(
                   self.x + self.volume_offset.x + (self.vol_width / 2.0) + 1,
                   self.y + self.volume_offset.y + (self.vol_height / 2.0) + 1,
                   text = self.volume, fill = self.volume_font_color,
                   font = self.volume_font, width = self.vol_width,)
            
    def draw_progress(self, percent_done):

        #### color
        progress_bg_color     = colors('progress_bg_color')
        progress_bar_color    = colors('progress_bar_color')
        percent_display_color = colors('percent_color')
        
        x,y=self.x,self.y
        if percent_done == self.percent_done:
            #don't need to redraw
            return
        
        self.percent_done = percent_done

        # Undraw the old progress gauge
        if self.progress_bar:
            self.display.delete(self.progress_bar)
            self.progress_bar = None
        if self.progress_bar_bg:
            self.display.delete(self.progress_bar_bg)
            self.progress_bar_bg = None
        if self.progress_percent_display:
            self.display.delete(self.progress_percent_display)
            self.progress_percent_display = None
            
        if self.percent_done is None:
            #Don't display the progress gauge
            return

        # Draw the new progress gauge
        self.progress_bar_bg = self.display.create_rectangle(
            x + self.progress_bar_bg_offset1.x,
            y + self.progress_bar_bg_offset1.y,
            x + self.progress_bar_bg_offset2.x + self.bar_width,
            y + self.progress_bar_bg_offset2.y, fill=progress_bg_color)  
        self.progress_bar = self.display.create_rectangle(
            x + self.progress_bar_offset1.x, y + self.progress_bar_offset1.y,
            x+self.progress_bar_offset2.x+(self.bar_width*self.percent_done/100.0),
            y + self.progress_bar_offset2.y, fill=progress_bar_color)
        if self.display.width > 470:
            self.progress_percent_display =  self.display.create_text(
                x + self.percent_disp_offset.x, y + self.percent_disp_offset.y,
                text = str(self.percent_done)+"%",
                fill = percent_display_color, font = self.font)

    def draw(self):
        x, y                    = self.x, self.y

        self.draw_mover()

        self.draw_state()

        self.draw_timer()

        #Display the progress bar and percent done.
        self.draw_progress(self.percent_done)

        self.draw_volume()

        #Display the connection.
        if self.connection:
            self.connection.draw()

        self.display.update()

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
            self.display.delete(self.progress_bar_bg)
            self.progress_bar_bg = None
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

    def undraw(self):
        self.undraw_timer()
        self.undraw_state()
        self.undraw_progress()
        self.undraw_volume()
        self.undraw_mover()

#########################################################################
        
    def update_state(self, state, time_in_state=0):
        if state == self.state:
            return
        self.state = state

        #different mover colors
        mover_error_color   = colors('mover_error_color')
        mover_offline_color = colors('mover_offline_color')
        mover_stable_color  = colors('mover_stable_color')
        state_error_color   = colors('state_error_color')
        state_offline_color = colors('state_offline_color')
        state_stable_color  = colors('state_stable_color')
        state_idle_color    = colors('state_idle_color')
        #tape_stable_color   = colors('tape_stable_color')
        #label_stable_color  = colors('label_stable_color')
        #tape_offline_color  = colors('tape_offline_color')
        #label_offline_color = colors('label_offline_color')

        #These mover colors stick around.
        self.percent_color       =  colors('percent_color')
        self.progress_bar_color  = colors('progress_bar_color')
        self.progress_bg_color   = colors('progress_bg_color')
        #self.state_color         = colors('state_color') 
        self.timer_color         = colors('timer_color')
        self.volume_font_color = colors('label_stable_color')
        self.volume_bg_color = colors('tape_stable_color')
        self.mover_color = {'ERROR': mover_error_color,
                            'OFFLINE':mover_offline_color}.get(self.state,
                                                           mover_stable_color)
        self.state_color = {'ERROR': state_error_color,
                            'OFFLINE':state_offline_color,
                            'Unknown':state_idle_color,
                            'IDLE':state_idle_color}.get(self.state,
                                                         state_stable_color)
        
        #Update the time in state counter for the mover.
        now = time.time()
        self.timer_started = now - time_in_state
        self.update_timer(now)
        
    def update_timer(self, now):
        seconds = int(now - self.timer_started)
        if seconds == self.timer_seconds:
            return

        self.timer_seconds = seconds
        self.timer_string = HMS(seconds)

        self.draw_timer()

    def load_tape(self, volume_name, load_state):
        self.volume = volume_name
        self.update_state(load_state)
        self.draw_volume()

    def unload_tape(self):
        if not self.volume:
            Trace.trace(1, "Mover %s has no volume." % (self.name,))
            return

        self.volume = None
        self.undraw_volume()

    def transfer_rate(self, num_bytes, total_bytes):
        #keeps track of last number of bytes and time; calculates rate
        # in bytes/second
        self.b1 = num_bytes
        self.t1 = time.time()
        rate    = (self.b1-self.b0)/(self.t1-self.t0)
        self.b0 = self.b1
        self.t0 = self.t1
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

        #k = number of movers
        k = self.index

        #total number of columns 
        num_cols = (N / 20) + 1
        #total number of rows in the largest column
        num_rows = int(round(float(N) / float(num_cols)))
        #this movers column and row
        column = (k / num_rows)
        row = (k % num_rows)

        #vertical distance seperating the bottom of one mover with the top
        # of the next.
        space = ((self.display.height - (self.height * 19.0)) / 19.0)
        space = (self.height - space) * ((19.0 - num_rows) / 19.0) + space

        #The following offsets the y values for a second column.
        y_offset = ((self.height + space) / 2.0) * (column % 2)

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
        column_width = (self.display.width / float(num_cols + 1))
        x =  column_width * (column + 1)
        
        #This value is used when drawing the dotted connection line.
        self.column = column
        self.display.mover_columns[self.column] = int(x)
        
        return int(x), int(y)
    
    def position(self, N):
        if layout==CIRCULAR:
            return self.position_circular(N)
        elif layout==LINEAR:
            return self.position_linear(N)
        else:
            Trace.trace(1, "Unknown layout %s." % (layout,))
            sys.exit(-1)

    def resize(self, N):
        self.height = ((self.display.height - 40) / 20)
        #This line assumes that their will not be 40 or more movers.
        self.width = (self.display.width/4.0)

        #These are the new offsets
        self.volume_offset         = XY(2, 2)
        self.label_offset          = XY(self.width+5, self.height)
        self.img_offset            = XY(self.width/2.3, self.height/8.)
        self.state_offset          = XY(self.width/1.4, self.height/3.)
        self.timer_offset          = XY(self.width/1.3, self.height/1.3)
        self.percent_disp_offset   = XY(self.width/1.9, self.height/1.2)#green
        self.progress_bar_offset1  = XY(self.width/25., self.height/1.6)#yellow
        self.progress_bar_offset2  = XY(self.width/25., self.height/1.2)#yellow
        self.progress_bar_bg_offset1 = \
                                 XY(self.width/25., self.height/1.6)#magenta
        self.progress_bar_bg_offset2 = \
                                 XY(self.width/25., self.height/1.2)#magenta
        self.bar_width             = self.width/2.5 #(how long bar should be)
        self.vol_width = (self.width)/2.5
        self.vol_height = (self.height)/2.5

        #Font geometry.
        self.font = get_font(self.height/2.5, 'arial',
                             width_wanted=self.max_font_width(),
                             fit_string="DISMOUNT_WAIT")
        self.label_font = get_font(self.height/2.5, 'arial',
                                   width_wanted=self.max_label_font_width(),
                                   fit_string=self.name)
        if self.volume:
            self.volume_font = get_font(self.vol_height, 'arial',
                                        fit_string=self.volume,
                                        width_wanted=self.vol_width)
        else:
            self.volume_font = None

    def max_font_width(self):
        return (self.width - self.width/3.0) - 10

    def max_label_font_width(self):
        #total number of columns 
        num_cols = (self.N / 20) + 1
        #size of column
        column_width = (self.display.width / float(num_cols + 1))
        #difference of column width and mover rectangle with fudge factor.
        return (column_width - self.width) - 10

    def reposition(self, N): #, state=None):
        #Undraw the mover before moving it.
        self.undraw()

        self.resize(N)
        self.x, self.y = self.position(N)

        self.draw()
    
    
class Client:

    def __init__(self, name, display):
        self.name               = name
        self.display            = display
        self.last_activity_time = time.time()
        self.n_connections      = 0
        self.waiting            = 0
        self.label              = None
        self.outline            = None
        #self.font = get_font(12, 'arial')

        self.resize()
        self.position()
        self.update_state()
        self.draw()

    def draw(self):
        x, y = self.x, self.y

        if self.outline:
            self.display.coords(self.outline, x,y, x+self.width,y+self.height)
            #self.display.itenconfigure(self.outline, fill=self.color)
        else:
            self.outline = self.display.create_oval(x, y, x+self.width,
                                               y+self.height, fill=self.color)

        if self.label:
            self.display.coords(self.label,
                                x + (self.width/2), y + (self.height/2))
            self.display.itemconfigure(self.label, font = self.font)
        else:
            self.label = self.display.create_text(x+self.width/2,
                                                  y+self.height/2,
                                                  text=self.name,
                                                  font=self.font)

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

    def update_state(self):

        ### color
        client_wait_color   = colors('client_wait_color')
        client_active_color = colors('client_active_color')
        
        if self.waiting:
            self.color = client_wait_color 
        else:
            self.color =  client_active_color

        if self.outline:
            self.display.itemconfigure(self.outline, fill = self.color) 

    def resize(self):
        self.width = self.display.width/12
        self.height =  self.display.height/28
        self.font = get_font(self.height/2.5, 'arial')

    def position(self):
        i = 0

        ## Step through possible positions in order 0, 1, -1, 2, -2, 3, -3, ...
        while self.display.client_positions.has_key(i):
            if i == 0:
                i =1
            elif i>0:
                i = -i
            else:
                i = 1 - i
        self.index = i
        self.display.client_positions[i] = self.name
        
        self.x, self.y = scale_to_display(-0.9, self.index/10.,
                                          self.display.width,
                                          self.display.height)        
        
    def reposition(self):
        self.undraw()
        self.resize()
        self.position()
        self.draw()

    def __del__(self):
        ##Mark this spot as unoccupied
        try:
            del self.display.client_positions[self.index]
        except KeyError:
            pass

        self.undraw()
        
class Connection:
    """ a line connecting a mover and a client"""
    def __init__(self, mover, client, display):
        # we are passing instances of movers and clients
        self.mover              = mover
        client.n_connections    = client.n_connections + 1
        self.client             = client
        self.display            = display
        self.rate               = 0 #pixels/second, not MB
        self.dashoffset         = 0
        self.segment_start_time = 0
        self.segment_stop_time  = 0
        self.line               = None
        
    def draw(self):
        path = []

        # middle of left side of mover
        mx,my = self.mover.x, self.mover.y + self.mover.height/2.0
        path.extend([mx,my])
        # if multiple columns are used, go in between.           
        if self.mover.column == 1:
            mx = self.display.mover_columns[0]
            path.extend([mx,my])

        #middle of right side of client
        cx, cy = (self.client.x + self.client.width,
                  self.client.y + self.client.height/2.0)
        x_distance = mx - cx
        path.extend([mx-x_distance/3., my, cx+x_distance/3., cy, cx, cy])

        #Draw or update the line.
        if self.line:
            self.display.coords(self.line, tuple(path))
            self.display.itemconfigure(self.line, dashoffset = self.dashoffset)
        else:
            self.line = self.display.create_line(path, dash='...-',width=2,
                                                 dashoffset = self.dashoffset,
                                                 smooth=1)
    def reposition(self):
        self.undraw()
        self.draw()

    def undraw(self):
        try:
            self.display.delete(self.line)
            self.line = None
        except Tkinter.TclError:
            pass

    def __del__(self):
        self.client.n_connections = self.client.n_connections - 1
        self.undraw()
        
    def update_rate(self, rate):
        now                       = time.time()
        self.segment_start_time   = now #starting time at this rate
        self.segment_stop_time    = now + 5 #let the animation run 5 seconds
        self.segment_start_offset = self.dashoffset
        self.rate                 = rate
        
    def animate(self, now=None):
        if now is None:
            now=time.time()
        if now >= self.segment_stop_time:
            return

        new_offset = self.segment_start_offset + \
                     self.rate * (now-self.segment_start_time) 
    
        if new_offset != self.dashoffset:  #we need to redraw the line
            self.dashoffset = new_offset
            if self.line:
                self.display.itemconfigure(self.line, dashoffset=new_offset)
            else:
                self.draw()

        
class Title:
    def __init__(self, text, display):
        self.text       = text #this is just a string
        self.display    = display
        self.tk_text    = None #this is a tk Text object
        self.fill       = None #color to draw with
        #self.font       = tkFont.Font(size=36, family="Arial")
        self.font       = get_font(20, "arial")
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
    def __del__(self):
        self.display.delete(self.tk_text)

class MoverDisplay(Tkinter.Toplevel):
    """  The mover state display """
    ##** means "variable number of keyword arguments" (passed as a dictionary)
    def __init__(self, mover, **attributes):
        Tkinter.Toplevel.__init__(self)

        #Tell it to set the remaining configuration values and to apply them.
        self.title(mover.name)
        self.configure(attributes)
        
        csc = entv.get_csc()
        mov = mover_client.MoverClient(csc, mover.name+".mover")
        status = mov.status(rcv_timeout=5, tries=1)

        #Font geometry.
        self.font = get_font(12, 'arial')
        
        msg = ""
        for item, value in status.items():
            msg = msg + "%s: %s\n" % (item, pprint.pformat(value))
        self.state_display = Tkinter.Label(master=self,
                                           justify=Tkinter.LEFT,
                                           font = self.font,
                                           width = 0,
                                           text = msg,
                                           foreground = mover.state_color,
                                           background = mover.mover_color,
                                           anchor=Tkinter.NW)
        self.state_display.pack(side=Tkinter.LEFT, expand=Tkinter.YES,
                                fill=Tkinter.BOTH)

class Display(Tkinter.Canvas):
    """  The main state display """
    ##** means "variable number of keyword arguments" (passed as a dictionary)
    def __init__(self, master, title, window_width, window_height,
                 canvas_width=None, canvas_height=None, **attributes):

        #If the initial size is larger than the screen size, use the
        #  screen size.
        tk = Tkinter.Tk()
        window_width = min(tk.winfo_screenwidth(), window_width)
        window_height= min(tk.winfo_screenheight(), window_height)
        
        if 1 or canvas_width is None:
            canvas_width = window_width
        if 1 or canvas_height is None:
            canvas_height = window_height

        Tkinter.Canvas.__init__(self, master,width=window_width,
                                height=window_height,
                                scrollregion=(0,0,canvas_width,canvas_height))
###XXXXXXXXXXXXXXXXXX  --get rid of scrollbars--
##        self.scrollX = Tkinter.Scrollbar(self, orient=Tkinter.HORIZONTAL)
##        self.scrollY = Tkinter.Scrollbar(self, orient=Tkinter.VERTICAL)

##       #When the canvas changes size or moves, update the scrollbars
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
        Tkinter.Tk.title(self.master, title)
        self.configure(attributes)
        self.pack(expand=1, fill=Tkinter.BOTH)
        self.stopped = 0
        self.width =  int(self['width'])
        self.height = int(self['height'])
        self.pack()

        self.mover_names      = [] ## List of mover names.
        self.movers           = {} ## This is a dictionary keyed by mover name,
                                   ##value is an instance of class Mover
        self.mover_columns    = {} #x-coordinates for columns of movers
        #self.mover_label_width = None #width to allow for mover labels
        self.clients          = {} ## dictionary, key = client name,
                                   ##value is instance of class Client
        self.client_positions = {} ##key is position index (0,1,-1,2,-2) and
                                   ##value is Client
        self.connections      = {} ##dict. of connections.

        self.command_queue = [] #List of notify commands to process.

        self.bind('<Button-1>', self.action)
        self.bind('<Button-3>', self.reinititalize)
        self.bind('<Configure>', self.resize)

        #Draw the window and update the canvas size.
        self.update()
        self.width, self.height = self.winfo_width(), self.winfo_height()

    def __del__(self):
        self.connections = {}        
        self.movers = {}
        self.clients = {}
        self.client_positions = {}
        
    def undraw(self):
        for connection in self.connections.values():
            connection.undraw()
        for mover in self.movers.values():
            mover.undraw()
        for client in self.clients.values():
            client.undraw()

        self.update()
        
    def action(self, event):
        x, y = self.canvasx(event.x), self.canvasy(event.y)
        overlapping = self.find_overlapping(x-1, y-1, x+1, y+1)
        print overlapping, (x, y)

        for mover in self.movers.values():
            for i in range(len(overlapping)):
                if mover.state_display == overlapping[i]:
                    mover_display = MoverDisplay(mover=mover)
                    print mover.name, mover.state

    def resize(self, event):
        #If the user changed the window size, update.
        self.reposition_canvas()

    def reinititalize(self, event):
        self.after_cancel(self.after_timer_id)
        self.after_cancel(self.after_animation_id)
        self.after_cancel(self.after_clients_id)
        self.after_cancel(self.after_idle_id)
        self._reinit = 1
        self.quit()

    def reinit(self):
        self._reinit = 0
        self.stopped = 0
        
        self.mover_names      = [] ## List of mover names.
        self.movers           = {} ## This is a dictionary keyed by mover name,
                                   ##value is an instance of class Mover
        self.mover_columns    = {} #x-coordinates for columns of movers
        #self.mover_label_width = None #width to allow for mover labels
        self.clients          = {} ## dictionary, key = client name,
                                   ##value is instance of class Client
        self.client_positions = {} ##key is position index (0,1,-1,2,-2) and
                                   ##value is Client
        self.connections      = {} ##dict. of connections.

        self.command_queue = [] #List of notify commands to process.        

    def attempt_reinit(self):
        return self._reinit

    def position_canvas(self):
        try:
            size = self.winfo_width(), self.winfo_height()
        except:
            self.stopped = 1
            return

        (self.width, self.height) = size
            
    def reposition_canvas(self):
        try:
            size = self.winfo_width(), self.winfo_height()
        except:
            self.stopped = 1
            return

        if size != (self.width, self.height):
            # size has changed
            self.width, self.height = size
            if self.clients:
                self.reposition_clients()
            if self.movers:
                self.reposition_movers()
            if self.connections:
                self.reposition_connections()
        
    def reposition_movers(self, number_of_movers=None):
        items = self.movers.items()
        if number_of_movers:
            N = number_of_movers
        else:
            N = len(items) #need this to determine positioning
        self.mover_label_width = None
        for mover_name, mover in items:
            mover.reposition(N)            
         
    def reposition_clients(self):
        del self.client_positions
        self.client_positions = {}
        for client_name, client in self.clients.items():
            client.reposition()

    def reposition_connections(self):
        for connection in self.connections.values():
            connection.reposition()

    #Called from self.after().
    def update_timers(self):
        now = time.time()
        #### Update all mover timers
        #This checks to see if the timer has changed at all.  If it has,
        # it resets the timer for new state.
        for mover in self.movers.values():
            mover.update_timer(now)

        self.after_timer_id = self.after(30, self.update_timers)
        
    #Called from self.after().
    def connection_animation(self):
        now = time.time()
        #### Update all connections.
        for connection in self.connections.values():
            connection.animate(now)

        self.after_animation_id = self.after(30, self.connection_animation)

    #Called from self.after().
    def disconnect_clients(self):
        now = time.time()
        #### Check for unconnected clients
        for client_name, client in self.clients.items():
            if (client.n_connections > 0 or client.waiting == 1):
                continue
            if now - client.last_activity_time > 5: # grace period
                Trace.trace(1, "It's been longer than 5 seconds, %s " \
                            " client must be deleted" % (client_name,))
                client.undraw()
                del self.clients[client_name]

        self.after_clients_id = self.after(30, self.disconnect_clients)

    #Called from entv.handle_periodic_actions().
    def handle_titling(self):

        now = time.time()
        #### Handle titling
        if self.title_animation:
            if now > self.title_animation.stop_time:
                self.title_animation = None
            else:
                self.title_animation.animate(now)

        ####force the display to refresh
        self.update()

    def create_movers(self, mover_names):
        #Create a Mover class instance to represent each mover.
        N = len(mover_names)

        for k in range(N):
            mover_name = mover_names[k]
            self.movers[mover_name] = Mover(mover_name, self, index=k, N=N)

    def queue_command(self, command):
        display_lock.acquire()
        self.command_queue.append(command)
        display_lock.release()

    def handle_command(self, command):
        ## Accept commands of the form:
        # 1 word:
        #      quit
        #      robot
        #      title
        # 2 words:
        #     delete MOVER_NAME
        #      client CLIENT_NAME
        # 3 words:
        #      connect MOVER_NAME CLIENT_NAME
        #      disconnect MOVER_NAME CLIENT_NAME
        #      loaded MOVER_NAME VOLUME_NAME
        #      loading MOVER_NAME VOLUME_NAME
        #      moveto MOVER_NAME VOLUME_NAME
        #      remove MOVER_NAME VOLUME_NAME
        #      state MOVER_NAME STATE_NAME [TIME_IN_STATE]
        #      unload MOVER_NAME VOLUME_NAME
        # 4 words:
        #      transfer MOVER_NAME nbytes total_bytes
        # (N) number of words:
        #      movers M1 M2 M3 ...
    
        
        comm_dict = {'quit' : 1, 'client' : 1, 'connect' : 1, 'disconnect' : 1,
                     'loading' : 1, 'title' : 1, 'loaded' : 1, 'state' : 1,
                     'unload': 1, 'transfer' : 1, 'movers' : 1}

        now = time.time()
        command = string.strip(command) #get rid of extra blanks and newlines
        words = string.split(command)
        if not words: #input was blank, nothing to do!
            return

        if words[0] not in comm_dict.keys():
            pass
            #print "just passing"
        else:
            if words[0]=='quit':
                self.stopped = 1
                return

            if words[0]=='title':
                title = command[6:]
                title=string.replace (title, '\\n', '\n')
                self.title_animation = Title(title, self)
                return

            # command needs (N) words
            if words[0]=='movers':
                self.mover_names = words[1:]
                self.create_movers(self.mover_names)
                return
            
            # command does not require a mover name, will only put clients
            # in a queue
            if words[0]=='client':
                ## For now, don't draw waiting clients (there are just
                ## too many of them)
                return
            
                client_name = normalize_name(words[1])
                client = self.clients.get(client_name) 
                if client is None: #it's a new client
                    client = Client(client_name, self)
                    self.clients[client_name] = client
                    client.waiting = 1
                    client.draw()
                return

            ###################################################################
            #                                                                 #
            #            all following commands have the name of the mover    #
            #            in the 2nd field                                     #
            #                                                                 #
            ###################################################################
            mover_name = words[1]
            mover = self.movers.get(mover_name)
            if not mover:
                #This is an error, a message from a mover we never heard of
                Trace.trace(1, "Don't recognize mover, continueing ....")
                return

            if words[0]=='disconnect':
                #Ignore the passed-in client name, disconnect from
                ## any currently connected client
                if not mover.connection:
                    Trace.trace(1, "Mover is not connected")
                    return
                
                Trace.trace(1, "mover %s is disconnecting" % (mover_name,))

                #Remove all references to the connection.
                del self.connections[mover.name]
                mover.connection = None

                #Remove the progress bar.
                #mover.t0 = time.time()
                #mover.b0 = 0
                mover.draw_progress(None)
                
                return

            # command requires 3 words
            if len(words) < 3:
                Trace.trace(1, "Bad command: %s" % (command,))
                return
            
            if words[0]=='state':
                what_state = words[2]
                try:
                    time_in_state = int(float(words[3]))
                except:
                    time_in_state = 0
                mover.update_state(what_state, time_in_state)
                mover.draw()
                if what_state in ['ERROR', 'IDLE', 'OFFLINE']:
                    msg="Need to disconnect because mover state changed to: %s"
                    if mover.connection: #no connection with mover object
                        Trace.trace(1, msg % (what_state,))
                        del self.connections[mover.name]
                        mover.connection=None
                return
        
            if words[0]== 'connect':
                if mover.state in ['ERROR', 'IDLE', 'OFFLINE']:
                    Trace.trace(1,
                        "Cannot connect to mover that is %s." % (mover.state,))
                    return

                client_name = normalize_name(words[2])
                client = self.clients.get(client_name)
                if not client: ## New client, we must add it
                    client = Client(client_name, self)
                    self.clients[client_name] = client
                else:
                    client.waiting = 0
                    client.update_state() #change fill color if needed
                client.draw()
                #First test if a connection is already present.
                if self.connections.get(mover.name, None):
                    connection = self.connections[mover.name]
                    connection.undraw()
                    connection.__init__(mover, client, self)
                #If not create a new connection.
                else:
                    connection = Connection(mover, client, self)
                    self.connections[mover.name] = connection
                    connection.update_rate(0)
                connection.draw() #Either draw or redraw correctly.
                ###What are these for?
                mover.t0 = now
                mover.b0 = 0
                mover.connection = connection
                return

            if words[0] in ['loading', 'loaded']:
                if mover.state in ['IDLE']:
                    Trace.trace(1, "An idle mover cannot have tape...ignore")
                    return
                load_state = words[0] #=='loaded'
                what_volume = words[2]
                mover.load_tape(what_volume, load_state)
                return
        
            if words[0]=='unload': # Ignore the passed-in volume name, unload
                                   ## any currently loaded volume
                mover.unload_tape()
                return

            # command requires 4 words
            if len(words)<4: 
                Trace.trace(1, "Bad command: %s" % (command,))
                return
        
            if words[0]=='transfer':
                num_bytes = my_atof(words[2])
                total_bytes = my_atof(words[3])
                if total_bytes==0:
                    percent_done = 100
                else:
                    percent_done = abs(int(100 * num_bytes/total_bytes))
                mover.draw_progress(percent_done)
                rate = mover.transfer_rate(num_bytes, total_bytes) / (256*1024)
                if mover.connection:
                    mover.connection.update_rate(rate)
                    mover.connection.client.last_activity_time = time.time()
                return

    def display_idle(self):
        display_lock.acquire()
        if self.command_queue: #If the queue is not empty:
            self.handle_command(self.command_queue[0])
            del self.command_queue[0]
        display_lock.release()
        self.after_idle_id = self.after(30, self.display_idle)

    #overloaded 
    def update(self):
        try:
            if Tkinter.Tk.winfo_exists(self):
                Tkinter.Tk.update(self)
        except ValueError:
            Trace.trace(1, "Unexpected Tkinter error...ignore")
        except Tkinter.TclError:
            Trace.trace(1, "TclError...ignore")


    def mainloop(self):
        self.after_timer_id = self.after(30, self.update_timers)
        self.after_animation_id = self.after(30, self.connection_animation)
        self.after_clients_id = self.after(30, self.disconnect_clients)
        self.after_idle_id = self.after(30, self.display_idle)
        Tkinter.Tk.mainloop(self)
        self.undraw()
        self.stopped = 1


if __name__ == "__main__":
    if len(sys.argv)>1:
        title = sys.argv[1]
    else:
        title = "Enstore"
    display = Display(master=None, title=title,
                      window_width=700, window_height=1600,
                      canvas_width=1000, canvas_height=2000,
                      background=rgbtohex(173, 216, 230))
    display.mainloop()



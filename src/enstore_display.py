#!/usr/bin/env python

# $Id$

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
import re
import configuration_client

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
        return f
        #Why do this?  MWZ
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
    #'client_active_color' : rgbtohex(0, 255, 0), # green
    'client_color' : rgbtohex(0, 255, 0), # black
    'client_font_color' : rgbtohex(0, 0, 0), # white
    #mover colors
    'mover_color':          rgbtohex(0, 0, 0), # black
    'mover_error_color':    rgbtohex(255, 0, 0), # red
    'mover_offline_color':  rgbtohex(169, 169, 169), # grey
    'mover_stable_color':   rgbtohex(0, 0, 0), # black
    'percent_color':        rgbtohex(0, 255, 0), # green
    'progress_bar_color':   rgbtohex(255, 255, 0), # yellow
    'progress_bg_color':    rgbtohex(255, 0, 255), # magenta
    'progress_alt_bar_color':rgbtohex(255, 192, 0), # orange
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
        self.last_activity_time = now
        self.rate               = None  #In bytes per second.
        self.rate_string        = "0 MB/S"
        self.t0                 = 0
        self.timer_seconds      = 0
        self.timer_started      = now
        self.timer_string       = '00:00:00'

        #Find out information about the mover.
        try:
            csc = self.display.csc
            minfo = csc.get(self.name+".mover")
            #64GB is the default listed in mover.py.
            self.max_buffer = long(minfo.get('max_buffer', 67108864))
            self.library = minfo.get('library', "Unknown")
        except AttributeError:
            self.max_buffer = 67108864
            self.library = "Unknown"
            
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
            self.display.itemconfigure(self.outline, fill=self.mover_color,
                                       outline=self.library_color)
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

        #For small window sizes, the rate display is largely more important.
        # This is simalar to the percent display size drawing restrictions.
        if self.state == "ACTIVE" and self.display.width <= 470:
            self.undraw_state()
            return

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
                text=self.state, fill=self.state_color, anchor=Tkinter.CENTER)
            #if current state is in text and the new state is in text.
            else:
                self.display.coords(self.state_display,
                                 x+self.state_offset.x, y+self.state_offset.y)
                self.display.itemconfigure(self.state_display,
                                           text=self.state, anchor=Tkinter.CENTER,
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
                    font = self.font, text=self.state, fill=self.state_color,
                    anchor=Tkinter.CENTER)

    def draw_timer(self):
        if self.timer_display:
            self.display.itemconfigure(self.timer_display,
                                       text=self.timer_string)
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
            
    def draw_progress(self, percent_done, alt_percent_done):

        #### color
        progress_bg_color     = colors('progress_bg_color')
        progress_bar_color    = colors('progress_bar_color')
        progress_alt_bar_color = colors('progress_alt_bar_color')
        percent_display_color = colors('percent_color')
        
        x,y=self.x,self.y
        if percent_done == self.percent_done and \
           alt_percent_done == self.alt_percent_done:
            #don't need to redraw
            return
        
        self.percent_done = percent_done
        self.alt_percent_done = alt_percent_done

        # Redraw the old progress bg gauge.
        if self.percent_done == None and self.alt_percent_done == None:
            self.undraw_progress()
            return
        elif self.progress_bar_bg:
            self.display.coords(self.progress_bar_bg,
                                self.get_bar_position(100))
        else:
            #If both are not to be drawn, then this will draw the correct one.
            self.progress_bar_bg = self.display.create_rectangle(
                self.get_bar_position(100),
                fill=progress_bg_color, outline="")

        # Redraw the old progress gauge.
        if self.percent_done == None:
            pass
        elif self.progress_bar:
            self.display.coords(self.progress_bar,
                                self.get_bar_position(percent_done))
        else:
            #If both are not to be drawn, then this will draw the correct one.
            self.progress_bar = self.display.create_rectangle(
                self.get_bar_position(percent_done),
                fill=progress_bar_color, outline="")

        # Redraw the old alternate progress gauge.
        if self.alt_percent_done == None:
            pass
        elif self.progress_alt_bar:
            self.display.coords(self.progress_alt_bar,
                                self.get_bar_position(alt_percent_done))
        else:
            #If both are not to be drawn, then this will draw the correct one.
            self.progress_alt_bar = self.display.create_rectangle(
                self.get_bar_position(alt_percent_done),
                fill=progress_alt_bar_color, outline="")

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

        #Draw the percent of transfer done next to progress bar.
        if self.display.width > 470:
            if self.progress_percent_display:
                self.display.itemconfigure(self.progress_percent_display,
                                           text = str(self.percent_done)+"%")
                self.display.lift(self.progress_percent_display)
            else:
                self.progress_percent_display =  self.display.create_text(
                    x + self.percent_disp_offset.x,
                    y + self.percent_disp_offset.y,
                    text = str(self.percent_done)+"%",
                    fill = percent_display_color, font = self.font)

    def get_bar_position(self, percent):
        offset = (self.bar_width*percent/100.0)
        bar = (self.x + self.progress_bar_offset1.x,
               self.y + self.progress_bar_offset1.y,
               self.x + self.progress_bar_offset2.x + offset,
               self.y + self.progress_bar_offset2.y)
        
        return bar
    
    def draw_buffer(self, buffer_size):
        
        #### color
        progress_bg_color     = colors('progress_bg_color')
        progress_bar_color    = colors('progress_bar_color')

        if buffer_size == self.buffer_size:
            #don't need to redraw
            return

        self.buffer_size = buffer_size
        if self.buffer_size == None:
            #Don't display the buffer bar.
            self.undraw_buffer()
            return
        
        if self.buffer_bar_bg:
            self.display.coords(self.buffer_bar_bg,
                                self.get_buffer_position(100))
        else:
            #self.buffer_bar_bg = self.draw_buffer_bar(100, progress_bg_color)
            self.buffer_bar_bg = self.display.create_rectangle(
                self.get_buffer_position(100),
                fill = progress_bg_color, outline = "")

        if self.buffer_bar:
            self.display.coords(self.buffer_bar,
                                self.get_buffer_position(self.buffer_size))
        else:
            #self.buffer_bar = self.draw_buffer_bar(self.buffer_size,
            #                                       progress_bar_color)
            self.buffer_bar = self.display.create_rectangle(
                self.get_buffer_position(self.buffer_size),
                fill = progress_bar_color, outline = "")
            
    def get_buffer_position(self, bufsize):
        offset = (self.bar_width*bufsize/100.0)
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

        #The rate is usualy draw when the mover is in active state.
        # However, it can go into draining state while a transfer is in
        # progress.  The display of the draining state and rate can not
        # occur at the same time.  Give precdence to the state.
        if self.state == "DRAINING":
            self.undraw_rate()
        elif self.rate_display:
            self.display.itemconfigure(self.rate_display,
                                       text=self.rate_string)
        elif self.rate != None:
            self.rate_display =  self.display.create_text(
                self.x + self.rate_offset.x, self.y + self.rate_offset.y,
                text = self.rate_string, fill = colors('percent_color'),
                anchor = Tkinter.NE, font = self.font)

    def draw(self):
        x, y                    = self.x, self.y

        self.draw_mover()

        self.draw_state()

        self.draw_timer()

        #Display the progress bar and percent done.
        self.draw_progress(self.percent_done, self.alt_percent_done)
        
        self.draw_buffer(self.buffer_size)

        self.draw_volume()

        self.draw_rate()

        #Display the connection.
        if self.connection:
            self.connection.draw()

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
        self.library_color = self.display.get_mover_color(self.library)
        
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

    def update_rate(self, rate):

        if rate == self.rate:
            return
            
        self.rate = rate

        if rate != None:
            self.rate_string = "%.2f MB/S" % (self.rate / 1048576)
            self.draw_rate()
        else:
            self.undraw_rate()

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

    def transfer_rate(self, num_bytes, total_bytes, mover_time=None):
        #keeps track of last number of bytes and time; calculates rate
        # in bytes/second
        self.b1 = num_bytes
        if mover_time and type(mover_time) == type(0.0):
            #Newer mover code will include its current time.  This should
            # reduce bouncing rates from network delays, etc.
            self.t1 = mover_time
        else:
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
        self.width = (self.display.number_of_movers / 20)
        self.width = (self.display.width/(self.width + 3))
        #Size of the volume portion of mover display.
        self.vol_width = (self.width)/2.5
        self.vol_height = (self.height)/2.5

        #These are the new offsets
        self.volume_offset         = XY(2, 2)
        self.label_offset          = XY(self.width+5, self.height)
        self.img_offset            = XY(4 + self.vol_width, 0)
        self.state_offset          = XY(
            ((self.width - self.vol_width - 6) / 2) + (4 + self.vol_width),
            (2 + self.vol_height) / 2.0)
        self.timer_offset          = XY(self.width - 2, self.height - 2)
        self.percent_disp_offset   = XY(self.width/1.9, self.height/1.2)#green
        self.rate_offset           = XY(self.width - 2, 2) #green

        self.bar_width             = self.width/2.5 #(how long bar should be)
        self.bar_height            = self.height/4
        self.buffer_bar_height     = self.height/10
        self.progress_bar_offset1  = XY(4, self.height - 2 - self.bar_height)
        self.progress_bar_offset2  = XY(4, self.height - 2)#yellow
        #self.progress_bar_bg_offset1 = XY(4, self.height/1.6)#magenta
        #self.progress_bar_bg_offset2 = XY(4, self.height/1.2)#magenta
        self.buffer_bar_offset1 = XY(4, self.height - 4 -
                                     self.buffer_bar_height - self.bar_height)
        self.buffer_bar_offset2 = XY(4, self.height - 4 -
                                     self.bar_height)#magenta
        
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

#########################################################################
##
#########################################################################

class Client:

    def __init__(self, name, display):
        self.name               = name
        self.display            = display
        self.last_activity_time = time.time()
        self.n_connections      = 0
        self.waiting            = 0
        self.label              = None
        self.outline            = None

        self.resize()
        self.position()
        self.update_state()
        self.draw()
        
    def __del__(self):
        ##Mark this spot as unoccupied
        try:
            del self.display.client_positions[self.index]
        except KeyError:
            pass

        self.undraw()

    #########################################################################
        
    def draw(self):
        x, y = self.x, self.y

        if self.outline:
            self.display.coords(self.outline, x,y, x+self.width,y+self.height)
            #self.display.itenconfigure(self.outline, fill=self.color)
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
        client_wait_color   = colors('client_wait_color')
        client_active_color = colors('client_active_color')

        self.color = colors('client_color')
        self.font_color = colors('client_font_color')
        if self.waiting:
            self.outline_color = client_wait_color 
        else:
            #self.color =  client_active_color
            self.outline_color = self.display.get_client_color(self.name)

        if self.outline:
            self.display.itemconfigure(self.outline, fill = self.color) 

    #########################################################################

    def resize(self):
        self.width = self.display.width/8
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
        
        self.x, self.y = scale_to_display(-0.95, self.index/10.,
                                          self.display.width,
                                          self.display.height)        
        
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
        client.n_connections    = client.n_connections + 1
        self.client             = client
        self.display            = display
        self.rate               = 0 #pixels/second, not MB
        self.dashoffset         = 0
        self.segment_start_time = 0
        self.segment_stop_time  = 0
        self.line               = None
        self.path               = []

        self.position()

    def __del__(self):
        self.client.n_connections = self.client.n_connections - 1
        self.undraw()
        
    #########################################################################
        
    def draw(self):
        if self.line:
            self.display.coords(self.line, tuple(self.path))
            self.display.itemconfigure(self.line,dashoffset = self.dashoffset)
        else:
            self.line = self.display.create_line(self.path, dash='...-',
                                                 width=2, smooth=1,
                                                 dashoffset = self.dashoffset)

    def undraw(self):
        try:
            self.display.delete(self.line)
            self.line = None
        except Tkinter.TclError:
            pass

    def animate(self, now=None):
        if now is None:
            now=time.time()
        if now >= self.segment_stop_time:
            return

        new_offset = self.segment_start_offset + \
                     self.rate * (now-self.segment_start_time) 
    
        if new_offset != self.dashoffset:  #we need to redraw the line
            self.dashoffset = new_offset
            self.draw()
                
    #########################################################################
                
    def update_rate(self, rate):
        now                       = time.time()
        self.segment_start_time   = now #starting time at this rate
        self.segment_stop_time    = now + 5 #let the animation run 5 seconds
        self.segment_start_offset = self.dashoffset
        self.rate                 = rate

    #########################################################################

    def position(self):
        self.path = [] #remove old path

        # middle of left side of mover
        mx,my = self.mover.x, self.mover.y + self.mover.height/2.0
        self.path.extend([mx,my])
        # if multiple columns are used, go in between.           
        if self.mover.column == 1:
            mx = self.display.mover_columns[0]
            self.path.extend([mx,my])

        #middle of right side of client
        cx, cy = (self.client.x + self.client.width,
                  self.client.y + self.client.height/2.0)
        x_distance = mx - cx
        self.path.extend([mx-x_distance/3., my, cx+x_distance/3., cy, cx, cy])

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

#########################################################################
##
#########################################################################

class MoverDisplay(Tkinter.Toplevel):
    """  The mover state display """
    ##** means "variable number of keyword arguments" (passed as a dictionary)
    def __init__(self, mover, **attributes):
        Tkinter.Toplevel.__init__(self)

        #Tell it to set the remaining configuration values and to apply them.
        self.title(mover.name)
        self.configure(attributes)

        self.mover = mover
        self.status = self.get_mover_status()

        #Font geometry.
        self.font = get_font(12, 'arial')
        
        self.state_display = Tkinter.Label(master=self,
                                           justify=Tkinter.LEFT,
                                           font = self.font,
                                           width = 0,
                                           text = self.status,
                                           foreground = mover.state_color,
                                           background = mover.mover_color,
                                           anchor=Tkinter.NW)
        self.state_display.pack(side=Tkinter.LEFT, expand=Tkinter.YES,
                                fill=Tkinter.BOTH)

        self.after(5000, self.update_status)

    def get_mover_status(self):
        csc = self.mover.display.csc
        mov = mover_client.MoverClient(csc, self.mover.name+".mover")
        status = mov.status(rcv_timeout=5, tries=1)
        order = status.keys()
        order.sort()
        msg = ""
        for item in order:
            msg = msg + "%s: %s\n" % (item, pprint.pformat(status[item]))
        return msg

    def update_status(self):
        self.status = self.get_mover_status()
        self.state_display.configure(text = self.status,
                                     foreground = self.mover.state_color,
                                     background = self.mover.mover_color,)
        self.after(5000, self.update_status)

#########################################################################
##
#########################################################################

class Display(Tkinter.Canvas):
    """  The main state display """
    ##** means "variable number of keyword arguments" (passed as a dictionary)
    #entvrc_info is a dictionary of various parameters.
    def __init__(self, entvrc_info, **attributes):
        #title="", window_width=None, window_height=None,
        #geometry=None, x_position=None, y_position=None,
        #**attributes):

        geometry = entvrc_info.get('geometry', None)
        window_width = entvrc_info.get('window_width', None)
        window_height = entvrc_info.get('window_height', None)
        x_position = entvrc_info.get('x_position', None)
        y_position = entvrc_info.get('y_position', None)
        title = entvrc_info.get('title', "")
        animate = int(entvrc_info.get('animate', 1))#Python true for animation.
        self.library_colors = entvrc_info.get('library_colors', {})
        self.client_colors = entvrc_info.get('client_colors', {})

        tk = Tkinter.Tk()
        #Don't draw the window until all geometry issues have been worked out.
        tk.withdraw()

        #Use the geometry argument first.
        if geometry != None:
            window_width = int(re.search("^[0-9]+", geometry).group(0))
            window_height = re.search("[x][0-9]+", geometry).group(0)
            window_height = int(window_height.replace("x", " "))
            x_position = re.search("[+][-]{0,1}[0-9]+[+]", geometry).group(0)
            x_position = int(x_position.replace("+", ""))
            y_position = re.search("[+][-]{0,1}[0-9]+$", geometry).group(0)
            y_position = int(y_position.replace("+", ""))
        
        #If the initial size is larger than the screen size, use the
        #  screen size.
        if window_width != None and window_height != None:
            window_width = min(tk.winfo_screenwidth(), window_width)
            window_height= min(tk.winfo_screenheight(), window_height)
        else:
            window_width = 0
            window_height = 0
        if x_position != None and y_position != None:
            x_position = max(min(tk.winfo_screenwidth(), x_position), 0)
            y_position = max(min(tk.winfo_screenheight(), y_position), 0)
        else:
            x_position = 0
            y_position = 0

        #Recompile the geometry string.
        geometry = "%sx%s+%s+%s" % (window_width, window_height,
                                    x_position, y_position)

        #Remember the unframed geometry.  This is used when determining the
        # correct geometry to write to the .entvrc file.
        self.unframed_geometry = geometry

        #Set the geometry of the cavas and its toplevel window.
        Tkinter.Canvas.__init__(self, master=tk, height=window_height,
                                width=window_width)
        tk.winfo_toplevel().winfo_toplevel().geometry(geometry)

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

        #The toplevel widget the canvas created.
        toplevel = self.winfo_toplevel()
        #Various toplevel window attributes.
        toplevel.title(title)
        self.configure(attributes)
        #Menubar attributes.
        self.menubar = Tkinter.Menu(master=self.master)
        #Options menu.
        self.option_menu = Tkinter.Menu(master=self.menubar, tearoff=0)
        #Create the animate check button and set animate accordingly.
        self.animate = animate
        self.option_menu.add_checkbutton(label="Animate",
                                         indicatoron=Tkinter.TRUE,
                                         onvalue="animate",
                                         offvalue=0,
                                         variable=Tkinter.FALSE,
                                         command=self.toggle_animation)
        #If the initial posistion is on, then invoke the command function
        # and remember to keep the animate variable true.
        if self.animate:
            self.option_menu.invoke(0)
            self.animate = 1 #keep it on
        #Added the menus to there respective parent widgets.
        self.menubar.add_cascade(label="options", menu=self.option_menu)
        toplevel.config(menu=self.menubar)

        #With the window attributes created, pack them in.
        self.pack(expand=1, fill=Tkinter.BOTH)
        self.width  = int(self['width'])
        self.height = int(self['height'])

        self._init() #Other none window related attributes.

        self.bind('<Button-1>', self.action)
        self.bind('<Button-3>', self.reinititalize)
        self.bind('<Configure>', self.resize)
        self.bind('<Destroy>', self.window_killed)
        self.bind('<Visibility>', self.visibility)
        self.bind('<Button-2>', self.print_canvas)

        #Clear the window for drawing to the screen.
        self.winfo_toplevel().deiconify()
        self.update()

    def toggle_animation(self):
        #Toggle the animation flag variable.  (on or off)
        self.animate = (not self.animate)

        if self.animate:  #If turn on, schedule the next animation.
            self.after_animation_id = self.after(30, self.connection_animation)

    def __del__(self):
        self.connections = {}        
        self.movers = {}
        self.clients = {}
        self.client_positions = {}

    def _init(self):
        self._reinit = 0
        self.stopped = 0
        
        self.mover_names      = [] ## List of mover names.
        self.movers           = {} ## This is a dictionary keyed by mover name,
                                   ##value is an instance of class Mover
        self.mover_columns    = {} #x-coordinates for columns of movers
        self.clients          = {} ## dictionary, key = client name,
                                   ##value is instance of class Client
        self.client_positions = {} ##key is position index (0,1,-1,2,-2) and
                                   ##value is Client
        self.connections      = {} ##dict. of connections.

        self.command_queue    = [] #List of notify commands to process.

    def reinit(self):
        self._init()

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

        for mover in self.movers.values():
            for i in range(len(overlapping)):
                if mover.state_display == overlapping[i]:
                    mover_display = MoverDisplay(mover=mover)

        for connection in self.connections.values():
            for i in range(len(overlapping)):
                if connection.line == overlapping[i]:
                    self.itemconfigure(connection.line,
                                       fill=rgbtohex(255, 0, 0))
                else:
                    self.itemconfigure(connection.line, fill=rgbtohex(0, 0, 0))
                                                 
    def resize(self, event):
        try:
            self.after_cancel(self.after_timer_id)
            self.after_cancel(self.after_animation_id)
            self.after_cancel(self.after_clients_id)
            self.after_cancel(self.after_idle_id)
            self.after_cancel(self.after_reinititalize_id)
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
            self.after_timer_id = self.after(1000, self.update_timers)
            self.after_animation_id = self.after(1000,
                                                 self.connection_animation)
            self.after_clients_id = self.after(1000, self.disconnect_clients)
            self.after_idle_id = self.after(1000, self.display_idle)
        except AttributeError:
            pass

    def reinititalize(self, event=None):
        self.after_cancel(self.after_timer_id)
        self.after_cancel(self.after_animation_id)
        self.after_cancel(self.after_clients_id)
        self.after_cancel(self.after_idle_id)
        self.after_cancel(self.after_reinititalize_id)
        self._reinit = 1
        self.quit()

    def print_canvas(self, event):
        self.postscript(file="/home/zalokar/entv.ps", pagewidth="8.25i")

    def window_killed(self, event):
        self.stopped = 1

        new_position = self.unframed_geometry.split("+", 1)[1]
        new_size = self.unframed_geometry.split("+")[0]

        geometry = self.winfo_toplevel().geometry()
        size = geometry.split("+")[0]
        position = geometry.split("+", 1)[1]

        initial_framed_size= self.framed_geometry.split("+")[0]
        initial_framed_position= self.framed_geometry.split("+", 1)[1]
        
        ###If the user never repositioned the window then the value returned
        ### from self.winfo_toplevel().geometry() points to the top left of the
        ### canvas (aka top left of framed window).

        ###If the user moved the window, then the value returned from
        ### self.winfo_toplevel().geometry() contains the unframed window
        ### geometry.  If this is different than the initial framed geometry
        ### then we know the user moved the window and to set self.geometry
        ### to tell the calling code (aka entv.py) to save the geometry.
        if position != initial_framed_position:
            new_position = position

        ###The size doesn't seem to change with respect to the window being
        ### framed or unframed...
        if size != initial_framed_size:
            new_size = size

        #By setting this everytime, this will force entv to rewrite the
        # .entvrc file everytime.  This will also help correct errors in
        # the .entvrc file.
        self.geometry = "%s+%s" % (new_size, new_position)
        

    def visibility (self, event):
        #The current framed geometry.
        geometry = self.winfo_toplevel().geometry()

        ###The following records the initial framed geometry of the window.
        if not hasattr(self, "framed_geometry"):
            self.framed_geometry = geometry
        
    #########################################################################

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

            self.after_reposition_id = None
        
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

    #########################################################################

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

        #If the user turned off animation, don't do it.
        if not self.animate:
            return
        
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

    #########################################################################
    
    def create_movers(self, mover_names):
        #Create a Mover class instance to represent each mover.
        N = len(mover_names)

        for k in range(N):
            mover_name = mover_names[k]
            self.movers[mover_name] = Mover(mover_name, self, index=k, N=N)

    def get_mover_color(self, library):
        #If this is the first mover from this library.
        if not getattr(self, "library_colors", None):
            self.library_colors = {}

        #Make some adjustments.
        if type(library) == type([]):
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

        self.client_colors[client] = colors('client_active_color')

        return self.client_colors[client]

    #########################################################################

    def quit_command(self, command_list):
        self.stopped = 1

    def title_command(self, command_list):
        #title = command[6:]
        title = string.join(command_list[1:])
        title=string.replace (title, '\\n', '\n')
        self.title_animation = Title(title, self)

    def csc_command(self, command_list):
        self.csc = configuration_client.ConfigurationClient((command_list[1],
                                                         int(command_list[2])))
    
    def client_command(self, command_list):
        ## For now, don't draw waiting clients (there are just
        ## too many of them)
        return
            
        client_name = normalize_name(command_list[1])
        client = self.clients.get(client_name) 
        if client is None: #it's a new client
            client = Client(client_name, self)
            self.clients[client_name] = client
            client.waiting = 1
            client.draw()
    
    def connect_command(self, command_list):

        now = time.time()

        mover = self.movers.get(command_list[1])
        if mover.state in ['ERROR', 'IDLE', 'OFFLINE']:
            Trace.trace(1,
                "Cannot connect to mover that is %s." % (mover.state,))
            return

        client_name = normalize_name(command_list[2])
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
                
    def disconnect_command(self, command_list):
        Trace.trace(1, "mover %s is disconnecting from %s" %
                    (command_list[1], command_list[2]))
        
        mover = self.movers.get(command_list[1])

        #Remove all references to the connection.
        mover.connection = None
        try:
            del self.connections[mover.name]
        except KeyError:
            pass

        #Remove the progress bar.
        #mover.t0 = time.time()
        #mover.b0 = 0
        mover.draw_progress(None, None)
        mover.draw_buffer(None)
        mover.update_rate(None)
                
    def loaded_command(self, command_list):

        mover = self.movers.get(command_list[1])
        
        if mover.state in ['IDLE']:
            Trace.trace(1, "An idle mover cannot have tape...ignore")
            return
        load_state = command_list[0] #=='loaded'
        what_volume = command_list[2]
        mover.load_tape(what_volume, load_state)
        
    def state_command(self, command_list):
        
        mover = self.movers.get(command_list[1])

        what_state = command_list[2]
        try:
            time_in_state = int(float(command_list[3]))
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
        # command_list[3] = BYTES_TO_TRANSFER
        # command_list[4] = media or network
        # command_list[5] = BUFFER_SIZE
        # command_list[6] = CURRENT_TIME
        
        #print command_list
        mover = self.movers.get(command_list[1])

        num_bytes = my_atof(command_list[2])
        total_bytes = my_atof(command_list[3])
        if total_bytes==0:
            percent_done = 100
        else:
            percent_done = abs(int(100 * num_bytes/total_bytes))
        if command_list[4] == "network":
            mover.draw_progress(percent_done, mover.alt_percent_done)
        elif command_list[4] == "media":
            mover.draw_progress(mover.percent_done, percent_done)
        else:
            return

        #If the mover sends the buffer size info. display the bar.
        try:
            buffer_size = float(long(command_list[5]))
            #This helps prevent potential problems against changes on the fly.
            if long(buffer_size) > long(mover.max_buffer):
                mover.max_buffer = long(buffer_size)
            buffer_percent = int(100 * (buffer_size/float(mover.max_buffer)))
            mover.draw_buffer(buffer_percent)
        except:
            pass
            #exc, msg, tb = sys.exc_info()
            #print msg

        #Skip media transfers from the network connection update.
        if mover.connection and command_list[4] == "network":
            try:
                rate = mover.transfer_rate(num_bytes, total_bytes,
                                           command_list[6])
            except IndexError:
                rate = mover.transfer_rate(num_bytes, total_bytes)
            #Experience shows this is a good adjustment.
            mover.connection.update_rate(rate / (256*1024))
            mover.connection.client.last_activity_time = time.time()
            mover.update_rate(rate)

    def movers_command(self, command_list):
        self.mover_names = command_list[1:]
        self.number_of_movers = len(command_list[1:])
        self.create_movers(self.mover_names)

    #########################################################################

    def queue_command(self, command):
        display_lock.acquire()
        self.command_queue.append(command)
        display_lock.release()
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
                 'movers' : {'function':movers_command, 'length':2}}

    def get_valid_command(self, command):

        command = string.strip(command) #get rid of extra blanks and newlines
        words = string.split(command)
        if not words: #input was blank, nothing to do!
            return []

        if words[0] not in self.comm_dict.keys():
            #print "just passing"
            return []

        if len(words) < self.comm_dict[words[0]]['length']:
            Trace.trace(1, "Insufficent length for %s command." % (words[0],))
            return []

        if self.comm_dict[words[0]].get('mover_check', None) and \
           not self.movers.get(words[1]):
            #This is an error, a message from a mover we never heard of
            Trace.trace(1, "Don't recognize mover, continuing ....")
            return []

        return words

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

        #Words is a list of the split string command.
        words = self.get_valid_command(command)

        if words:
            apply(self.comm_dict[words[0]]['function'], (self, words,))
            
    #########################################################################
            
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
        self.after_reinititalize_id = self.after(3600000, self.reinititalize)
        self.after_reposition_id = None
        Tkinter.Tk.mainloop(self)
        self.undraw()
        self.stopped = 1

#########################################################################

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


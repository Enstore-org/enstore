#!/usr/bin/env python

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

#Set up paths to find our private copy of tcl/tk 8.3
ENSTORE_DIR=os.environ.get("ENSTORE_DIR")
if ENSTORE_DIR:
    TCLTK_DIR=os.path.join(ENSTORE_DIR, 'etc','TclTk')
else:
    TCLTK_DIR=os.path.normpath(os.path.join(os.getcwd(),'..','etc','TclTk'))
os.environ["TCL_LIBRARY"]=os.path.join(TCLTK_DIR, 'tcl8.3')
os.environ["TK_LIBRARY"]=os.path.join(TCLTK_DIR, 'tk8.3')
sys.path.insert(0, os.path.join(TCLTK_DIR, sys.platform))

if ENSTORE_DIR:
    IMAGE_DIR=os.path.join(ENSTORE_DIR, 'etc', 'Images')
else:
    IMAGE_DIR=os.path.normpath(os.path.join(os.getcwd(),'..','etc','Images'))

    
from Tkinter import *
from tkFont import Font

debug = 1 

CIRCULAR, LINEAR = range(2)
layout = LINEAR

def scale_to_display(x, y, w, h):
    """Convert coordinates on unit circle to Tk display coordinates for
    a window of size w, h"""
    return int((x+1)*(w/2)), int((1-y)*(h/2))

def HMS(s):
    h = s / 3600
    s = s - (h*3600)
    m = s / 60
    s = s - (m*60)
    return "%02d:%02d:%02d" % (h, m, s)

def my_atof(s):
    if s[-1] == 'L':
        s = s[:-1] #chop off any trailing "L"
    return string.atof(s)

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

def strip_domain(hostname):
    parts = string.split(hostname, '.')
    if parts[-1]=='gov' and parts[-2]=='fnal':
        parts = parts[:-2]
    return string.join(parts,'.')

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
            img = PhotoImage(file=filename)
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
#     draw() - draws most features on the movers
#     update_state() - as the state of the movers change, display for state will be updated
#     update_timer() - timer associated w/state, will update for each state
#     load_tape() - tape gets loaded onto mover:
#                                  gray indicates robot recognizes tape and loaded it
#                                  orange indicates when mover actually recognizes tape     
#     unload_tape() - will unload tape to side of each mover, ready for robot to remove f/screen
#     show_progress() - indicates progress of each data transfer; is it almost complete?
#     transfer_rate() - rate at which transfer being sent; calculates a rate
#     undraw() - undraws the features fromthe movers
#     position() - calculates the position for each mover
#     reposition() - reposition each feature after screen has been moved
#     __del__() - calls undraw() module and deletes features
#
######################################################################################

class Mover:
    def __init__(self, name, display, index=0,N=0):
        self.name = name
        self.display = display
        self.index = index
        self.N=N
        self.width = 125
        self.height = 30
        self.state = "Unknown"
        self.color = None
        now = time.time()
        self.timer_started = now
        self.timer_seconds = 0
        self.timer_string = '00:00:00'
        self.last_activity_time = now
        self.connection = None
        self.client = None
        self.rate = 0.0
        self.t0 = 0
        self.b0 = 0
        self.x, self.y = 0, 0 # Not placed yet
        self.volume = None
        #These 3 pieces make up the progress gauge display
        self.progress_bar = None
        self.progress_bar_bg = None
        self.progress_percent_display = None
        # This is the numeric value.  "None" means don't show the progress bar.
        self.percent_done = None
        self.x, self.y = self.position(N)
        self.draw()

    def draw(self):
        x, y = self.x, self.y
        label_offset = XY(60, 40)
        img_offset  =  XY(90, 2)
        percent_disp_offset = XY(60, 22)
        state_offset = XY(90, 8)
        timer_offset = XY(100, 22)

        self.outline =  self.display.create_rectangle(x, y, x+self.width, y+self.height, fill='black')
        self.label = self.display.create_text(x+label_offset.x,  y+label_offset.y,  text=self.name)
        img = find_image(self.state+'.gif')
        if img:
            self.state_display = self.display.create_image(x+img_offset.x, y+img_offset.y, anchor=NW, image=img)
        else:
            self.state_display = self.display.create_text(x+state_offset.x, y+state_offset.y, text=self.state, fill='light blue')
        self.timer_display = self.display.create_text(x+timer_offset.x, y+timer_offset.y, text='00:00:00',fill='white')
        if self.percent_done != None:
            bar_width = 38
            #XXXX  offset variable
            self.progress_bar_bg = self.display.create_rectangle(x+5,y+17,x+6+bar_width,y+26,fill='magenta')
            self.progress_bar = self.display.create_line(x+5,y+17,
                                                         x+6+(bar_width*self.percent_done/100.0), y+22,
                                                         fill='yellow', width=8)
            
            self.progress_percent_display =  self.display.create_text(x+percent_disp_offset.x, y+percent_disp_offset.y,
                                                              text = str(self.percent_done)+"%",
                                                              fill = 'green') #,font=8)

    def update_state(self, state, time_in_state=0):
        img_offset=XY(90, 2)
        state_disp_offset=XY(90, 8)
        
        mover_color=None
        if state == self.state:
            return
        self.state = state
        mover_color = {'ERROR': 'red', 'OFFLINE':'grey'}.get(self.state, 'black')
        if mover_color != self.color:
            self.display.itemconfigure(self.outline, fill=mover_color)
            self.color = mover_color
        x, y = self.x, self.y
        self.display.delete(self.state_display) # "undraw" the prev. state message
        img = find_image(state+'.gif')
        if img:
            self.state_display = self.display.create_image(x+img_offset.x, y+img_offset.y, anchor=NW, image=img)
        else:
            self.state_display = self.display.create_text(x+state_disp_offset.x, y+state_disp_offset.y, text=self.state, fill='light blue')
        now = time.time()
        self.timer_started = now - time_in_state
        if state != 'ACTIVE':
            self.show_progress(None)
        self.update_timer(time_in_state)
        
    def update_timer(self, seconds):
        timer_offset = XY(100, 22)
        
        x, y = self.x, self.y
        self.timer_seconds = seconds
        self.timer_string = HMS(seconds)
        self.display.delete(self.timer_display)
        self.timer_display = self.display.create_text(x+ timer_offset.x, y+ timer_offset.y, text=self.timer_string,fill='white')

    def load_tape(self, volume, load_state):
        self.volume = volume
        x, y = self.volume_position(ejected=0)
        self.volume.x, self.volume.y = x, y
        self.volume.ejected = 0
        self.volume.loaded = load_state
        self.volume.draw()

    def unload_tape(self, volume_name):
        if not (self.volume and self.volume.name == volume_name):
            print "Mover ",self.name,"  does not have this tape : ", volume_name
            return
            
        self.volume.loaded = 0
        self.volume.ejected = 1
        x, y = self.volume_position(ejected=1)
        self.volume.moveto(x, y)

    def volume_position(self, ejected=0):
        tape_offset = XY(5, 2)
        if layout==CIRCULAR:
            k=self.index
            N=self.N
            angle=math.pi/(N-1)
            i=(0+1J)
            coord=.75+.5*cmath.exp(i*(math.pi/2 + angle*k))
            x, y = scale_to_display(coord.real. coord.imag, self.display.width, self.display.height)
        else:
            if ejected:
                x, y = self.x+self.width+tape_offset.x, self.y+tape_offset.y
            else:
                x, y = self.x+ tape_offset.x, self.y+ tape_offset.y
        return x, y


    def show_progress(self, percent_done):
        progress_bar_offset = XY(6, 22)
        perc_disp_offset = XY(60, 22)
        
        x,y=self.x,self.y
        bar_width = 38
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
        self.progress_bar_bg = self.display.create_rectangle(x+5,y+17,x+6+bar_width,y+26,fill='magenta')    #XXXXX  need to create variable for offset
        self.progress_bar = self.display.create_line(x+progress_bar_offset.x, y+progress_bar_offset.y,
                                                     x+progress_bar_offset.x+bar_width*(self.percent_done/100.0),y+progress_bar_offset.y,
                                                     fill='yellow', width=8)
        self.progress_percent_display =  self.display.create_text(x+perc_disp_offset.x, y+perc_disp_offset.y,
                                                                  text = str(self.percent_done)+"%",
                                                                  fill = 'green') #,font=8)

    def transfer_rate(self, num_bytes, total_bytes):
        #keeps track of last number of bytes and time; calculates rate in bytes/second
        self.b1=num_bytes
        self.t1=time.time()
        rate=(self.b1-self.b0)/(self.t1-self.t0)
        self.b0=self.b1
        self.t0=self.t1
        return rate

    def undraw(self):
        self.display.delete(self.timer_display)
        self.display.delete(self.outline)
        self.display.delete(self.label)
        self.display.delete(self.state_display)
        self.display.delete(self.progress_bar_bg)
        self.display.delete(self.progress_bar)
        self.display.delete(self.progress_percent_display)
    
    def position_circular(self, N):
        k = self.index
        if N == 1: ## special positioning for a single mover.
            k = 1
            angle = math.pi / 2
        else:
            angle = math.pi / (N-1)
        i=(0+1J)
        coord=.75+.8*cmath.exp(i*(math.pi/2 + angle*k))
        return scale_to_display(coord.real, coord.imag, self.display.width, self.display.height)

    def position_linear(self, N):
        k = self.index
        if N == 1:
            y = self.display.height / 2.
        else:
            y = (k*2.5)*(self.display.height  / (N+1))#(k+0.5) * 
        x = self.display.width - 200
        return int(x), int(y)
    
    def position(self, N):
        if layout==CIRCULAR:
            return self.position_circular(N)
        elif layout==LINEAR:
            return self.position_linear(N)
        else:
            print "Unknown layout", layout
            sys.exit(-1)

    
    def reposition(self, N):
        self.undraw()
        self.x, self.y = self.position(N)
        self.draw()
        if self.volume:
            x, y = self.volume_position(self.volume.ejected)
            self.volume.moveto(x,y)

        if self.connection:
            self.connection.undraw()
            self.connection.draw()
        
    def __del__(self):
        self.undraw()

class Volume:
    width = 50
    height = 11
    def __init__(self, name, display, x=None, y=None, loaded=0, ejected=0):
        self.name = name
        self.display = display
        self.outline = None
        self.label = None
        self.loaded = loaded
        self.ejected = ejected
        self.x, self.y = x, y
        self.draw()
        
    def __setattr__(self, attr, value):
        if attr == 'loaded':
            if self.outline:
                if value:
                    tape_color, label_color = 'orange', 'white'
                else:
                    tape_color, label_color = 'grey', 'black'
                self.display.itemconfigure(self.outline, fill=tape_color)
                self.display.itemconfigure(self.label, fill=label_color)
        self.__dict__[attr] = value
        
    def draw(self):
        x, y = self.x, self.y
        if x is None or y is None:
            return
        if self.loaded:
            tape_color, label_color = 'orange', 'white'
        else:
            tape_color, label_color = 'grey', 'black'
        if self.outline or self.label:
            self.undraw()
        self.outline = self.display.create_rectangle(x, y, x+self.width, y+self.height, fill=tape_color)
        self.label = self.display.create_text(x+self.width/2, 1+y+self.height/2, text=self.name, fill=label_color)
        
    def moveto(self, x, y):
        self.undraw()
        self.x, self.y = x, y
        self.draw()

    def undraw(self):
        self.display.delete(self.outline)
        self.display.delete(self.label)
        self.outline =  self.label = None
        self.x = self.y = None
        
    def __del__(self):
        self.undraw()

    
class Client:

    def __init__(self, name, display):
        self.name = name
        self.width = 100
        self.height = 26
        self.display = display
        self.last_activity_time = 0.0 
        self.n_connections = 0
        self.waiting = 0
        i = 0
        ## Step through possible positions in order 0, 1, -1, 2, -2, 3, -3, ...
        while display.client_positions.has_key(i):
            if i == 0:
                i =1
            elif i>0:
                i = -i
            else:
                i = 1 - i
        self.index = i
        display.client_positions[i] = self
        self.x, self.y = scale_to_display(-0.9, i/10., display.width, display.height)

    def draw(self):
        x, y = self.x, self.y
        if self.waiting:
            color = rgbtohex(100,100,100)
        else:
            color = 'green'
        self.outline =  self.display.create_oval(x, y, x+self.width, y+self.height, fill=color)
        self.label = self.display.create_text(x+self.width/2, y+self.height/2, text=self.name)
        
    def undraw(self):
        self.display.delete(self.outline)
        self.display.delete(self.label)

    def update_state(self):
        if self.waiting:
            color = rgbtohex(100,100,100)
        else:
            color = 'green'
        self.display.itemconfigure(self.outline, fill = color) 
        
    def reposition(self):
        self.undraw()
        self.x, self.y = scale_to_display(-0.9, self.index/10.,
                                          self.display.width, self.display.height)
        self.draw()
        
    def __del__(self):
        self.undraw()
        
class Connection:
    """ a line connecting a mover and a client"""
    def __init__(self, mover, client, display):
        self.mover = mover
        self.client = client
        self.display = display
        self.dashoffset = 0 #current offset
        self.start_offset = 0 #offset when we got most recent rate info
        self.rate = 0 #pixels/second, not MB
        self.segment_start_time = 0
        self.segment_stop_time = 0

    def draw(self, n_name=None, c_name=None):
        mover_end = (self.mover.x,
                     self.mover.y + self.mover.height/2.0) #middle of left side
        client_end = (self.client.x + self.client.width,
                      self.client.y + self.client.height/2.0) #middle of right side
        x_distance = mover_end[0] - client_end[0]
        self.line = self.display.create_line(mover_end[0], mover_end[1],
                                             mover_end[0]-x_distance/3, mover_end[1],
                                             client_end[0]+x_distance/3,client_end[1],
                                             client_end[0],client_end[1],
                                             dash='...-',width=2,
                                             dashoffset = self.dashoffset,
                                             smooth=1)
    def undraw(self):
        self.display.delete(self.line)

    def __del__(self):
        self.undraw()
        
    def update_rate(self, rate):
        now = time.time()
        self.segment_start_time = now #starting time at this rate
        self.segment_stop_time = now + 5 #let the animation run 5 seconds
        self.segment_start_offset = self.dashoffset
        self.rate = rate
        
    def animate(self, now=None):
        if now is None:
            now=time.time()
        if now >= self.segment_stop_time:
            return

        new_offset = self.segment_start_offset + self.rate * (now-self.segment_start_time) 
    
        if new_offset != self.dashoffset:  #we need to redraw the line
            self.dashoffset = new_offset
            self.display.itemconfigure(self.line,dashoffset=new_offset)

        
class Title:
    def __init__(self, text, display):
        self.text = text #this is just a string
        self.display = display
        self.tk_text = None #this is a tk Text object
        self.fill = None #color to draw with
        self.font = Font(size=36, family="Helvetica")
        self.length = 2.5  #animation runs 2.5 seconds
        now = time.time()
        self.start_time = now
        self.stop_time = now + self.length

    def draw(self):
        #center this in the entire canvas
        self.tk_text = self.display.create_text(self.display.width/2, self.display.height/2,
                                                text=self.text,
                                                font=self.font, justify=CENTER)

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
            currentrgb[i] = int(startrgb[i] + (endrgb[i]-startrgb[i])*(elapsed/self.length))
        fill=rgbtohex(currentrgb[0], currentrgb[1], currentrgb[2])
        self.display.itemconfigure(self.tk_text, fill=fill)
    def __del__(self):
        self.display.delete(self.tk_text)

        
class Display(Canvas):
    """  The main state display """
    def __init__(self, master, title, window_width, window_height, canvas_width=None, canvas_height=None, **attributes):

        if canvas_width is None:
            canvas_width = window_width
        if canvas_height is None:
            canvas_height = window_height
        ##** means "variable number of keyword arguments" (passed as a dictionary)
        Canvas.__init__(self, master,width=window_width, height=window_height,
                        scrollregion=(0, 0, canvas_width, canvas_height))
        
        if (window_width, window_height) != (canvas_width, canvas_height):
            self.scrollX = Scrollbar(self, orient=HORIZONTAL)
            self.scrollY = Scrollbar(self, orient=VERTICAL)

            ##When the canvas changes size or moves, update the scrollbars
            self['xscrollcommand']= self.scrollX.set
            self['yscrollcommand'] = self.scrollY.set

            ##When scrollbar clicked on, move the canvas
            self.scrollX['command'] = self.xview
            self.scrollY['command'] = self.yview

            ##pack 'em up
            self.scrollX.pack(side=BOTTOM, fill=X)
            self.scrollY.pack(side=RIGHT, fill=Y)
            self.pack(side=LEFT)

        Tk.title(self.master, title)
        self.configure(attributes)
        self.pack(expand=1, fill=BOTH)
        self.stopped = 0
        self.width =  int(self['width'])
        self.height = int(self['height'])
        
        self.movers = {} ## This is a dictionary keyed by mover name,
                      ##value is an instance of class Mover
        self.clients = {} ## dictionary, key = client name, value is instance of class Client
        self.client_positions = {} #key is position index (0,1,-1,2,-2) and value is Client
        self.volumes={}
        self.title_animation = None
        
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #use IP addressing and UDP protocol
        myaddr = (os.uname()[1], 0)
        s.bind(myaddr)
        self.inputs = [s]
        host, port = s.getsockname()
        print "addr=", host, port
            
        self.bind('<Button-1>', self.action)

    def action(self, event):
        x, y = self.canvasx(event.x), self.canvasy(event.y)
        print self.find_overlapping(x-1, y-1, x+1, y+1)
        
    def create_movers(self, mover_names):
        #Create a Mover class instance to represent each mover.
        N = len(mover_names)
        for k in range(N):
            mover_name = mover_names[k]
            self.movers[mover_name] = Mover(mover_name, self, index=k, N=N)

    def reposition_movers(self):
        items = self.movers.items()
        N = len(items) #need this to determine positioning
        for mover_name, mover in items:
            mover.reposition(N)            
                
    def reposition_clients(self):
        for client_name, client in self.clients.items():
            client.reposition()
            
    def handle_command(self, command):
        ## Accept commands of the form:
        # 1 word:
        #      quit
        #      robot
        #      title
        # 2 words:
        #     delete MOVER_NAME
        # 3 words:
        #      client MOVER-NAME CLIENT_NAME
        #      connect MOVER_NAME CLIENT_NAME
        #      disconnect MOVER_NAME CLIENT_NAME
        #      loaded MOVER_NAME VOLUME_NAME
        #      loading MOVER_NAME VOLUME_NAME
        #      moveto MOVER_NAME VOLUME_NAME
        #      remove MOVER_NAME VOLUME_NAME
        #      state MOVER_NAME STATE_NAME
        #      time MOVER_NAME TIME (### time mover has been in that state)
        #      unload MOVER_NAME VOLUME_NAME
        # 4 words:
        #      transfer MOVER_NAME nbytes total_bytes
        # (N) number of words:
        #      movers M1 M2 M3 ...
    
        
        now = time.time()
        command = string.strip(command) #get rid of extra blanks and newlines
        words = string.split(command)
        if not words: #input was blank, nothing to do!
            return
        #"quit" is the only 1-word command
        if words[0]=='quit':
            self.stopped = 1
            return

        if words[0]=='movers':
            self.create_movers(words[1:])
            return

        if words[0]=='client':
            client_name = strip_domain(words[1])
            ##XXX we are working around a bug in the library manager.
            # Remove this code once library manager sends correct message
            if client_name[0] == '(':
                client_name = client_name[2:-2]
                try:
                    client_name = socket.gethostbyaddr(client_name)[0]
                    client_name = strip_domain(client_name)
                except:
                    print "Can't resolve address", client_name
             
            #XXX end hack
            client = self.clients.get(client_name) 
            if client is None: #new client
                client = Client(client_name, self)
                self.clients[client_name] = client
                client.waiting = 1
                client.draw()
            return

        if words[0]=='title':
            title = command[6:]
            title=string.replace(title, '\\n', '\n')
            self.title_animation = Title(title, self)
            return
            
        # all following commands have the name of the mover in the 2nd field
        mover_name = words[1]
        mover = self.movers.get(mover_name)
  
        if words[0]=='delete':
            del self.movers[mover_name]
            return

        if len(words) < 3:
            print "Error, bad command", command
            return
        if not mover:#This is an error, a message from a mover we never heard of
            return

        if words[0]=='state':
            what_state = words[2]
            time_in_state = 0
            if len(words) > 3:
                try:
                    time_in_state = int(float(words[3]))
                except:
                    print "bad numeric value", words[3]
            
            mover.update_state(what_state, time_in_state)
            return
        
        if words[0]== 'connect':
            client_name = strip_domain(words[2])
            client = self.clients.get(client_name)
            if not client: ## New client, we must add it
                client = Client(client_name, self)
                self.clients[client_name] = client
                client.draw()
            client.waiting = 0
            client.update_state() #change fill color if needed
            client.last_activity_time = now
            connection = Connection(mover, client, self)
            mover.t0 = now
            mover.b0 = 0
            connection.update_rate(0)
            connection.draw()
            mover.client = client
            mover.connection = connection
            client.n_connections = client.n_connections + 1
            return
        
        if words[0]=='disconnect':
            client_name = strip_domain(words[2])
            client = self.clients.get(client_name)
            if not client: ## this client is not displayed
                return
            if mover.connection:
                mover.connection = None
            if mover.client:
                mover.client = None
                client.n_connections = client.n_connections - 1
            mover.t0 = time.time()
            mover.b0 = 0
            mover.show_progress(None)
            print words[1], " disconnecting from ", client_name
            return

        if words[0] in ['loading', 'loaded']:
            load_state = words[0]=='loaded'
            what_volume = words[2]
            volume=self.volumes.get(what_volume)
            if volume is None:
                volume=Volume(what_volume, self, loaded=load_state)
            self.volumes[what_volume]=volume
            mover.load_tape(volume, load_state)
            return
        
        if words[0]=='unload':
            what_volume = words[2]
            mover.unload_tape(what_volume)
            return

        if len(words)<4: 
            print "Error, bad command", command
            return
        
        if words[0]=='transfer':
            num_bytes = my_atof(words[2])
            total_bytes = my_atof(words[3])
            if total_bytes==0:
                percent_done = 100
            else:
                percent_done = abs(int(100 * num_bytes/total_bytes))
            mover.show_progress(percent_done)
            rate = mover.transfer_rate(num_bytes, total_bytes) / (256*1024)
            if mover.connection:
                mover.connection.update_rate(rate)
            if mover.client:
                mover.client.last_activity_time = time.time()

            
    def mainloop(self):
        # Our mainloop is different from the normal Tk mainloop in that we have
        # (A) an interval timer to control animations and
        # (B) we check for commands coming from standard input

        while not self.stopped:
            size = self.winfo_width(), self.winfo_height()
            if size != (self.width, self.height):
                # size has changed
                self.width, self.height = size
                if self.clients:
                    self.reposition_clients()
                if self.movers:
                    self.reposition_movers()
                    
            #test whether there is a command ready to read, timeout in 1/30 second.
            readable, junk, junk = select.select(self.inputs, [], [], 1.0/30)

            for r in readable:
                command = r.recv(1024)
                if debug:
                    self.handle_command(command)
                else:
                    try:
                        self.handle_command(command)
                    except: 
                        print "cannot handle", command 
                    
            ## Here is where we handle periodic things
            now = time.time()
            #### Update all mover timers
            #This checks to see if the timer has changed at all.  If it has, it resets the timer for new state.
            for mover in self.movers.values():
                seconds = int(now - mover.timer_started)
                if seconds != mover.timer_seconds:
                    mover.update_timer(seconds)           #We must advance the timer
                if mover.connection:
                    mover.connection.animate(now)

            #### Check for unconnected clients
            tmp_client_dict={}
            t={}
            for client_name, client in self.clients.items():
                if client.n_connections >0:
                    continue
                if now - client.last_activity_time >  5 and client.waiting != 1: # grace period; ignores clients in queue
                    del self.clients[client_name]
                    del client

            #### Handle titling
            if self.title_animation:
                if now > self.title_animation.stop_time:
                    self.title_animation = None
                else:
                    self.title_animation.animate(now)
                
            ####force the display to refresh
            self.update()
        

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



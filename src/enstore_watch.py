#!/usr/bin/env python

# $Id$

from Tkinter import *
import math
import os
import string
import time
import select
import string
import sys
import socket
import exceptions

###

DEFAULTPORT = 60126

def scale_to_display(x, y, w, h):
    """Convert coordinates on unit circle to Tk display coordinates for
    a window of size w, h"""
    return (x+1)*(w/2), (1-y)*(h/2)

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

class Mover:
    def __init__(self, name, display):
        self.name = name
        self.display = display
        self.width = 110
        self.height = 25
        self.state = "Unknown"
        now = time.time()
        self.timer_started = now
        self.timer_seconds = 0
        self.timer_string = '00:00:00'
        self.last_activity_time = now
        self.connection = None
        self.rate = 0.0
        self.x, self.y = 0, 0 # Not placed yet
        self.volume = None
        #These 3 pieces make up the progress gauge display
        self.progress_bar = None
        self.progress_bar_bg = None
        self.progress_percent_display = None
        # This is the numeric value.  "None" means don't show the progress bar.
        self.percent_done = None
        self.removed = 0
        
    def draw(self):
        x, y = self.x, self.y
        self.outline =  self.display.create_rectangle(x, y, x+self.width, y+self.height, fill='black')
        self.label = self.display.create_text(x+60, y+40, text=self.name)
        self.state_display = self.display.create_text(x+85, y+8, text=self.state, fill='light blue')
        self.timer_display = self.display.create_text(x+85, y+18, text='00:00:00',fill='white')
        self.tape_slot = self.display.create_rectangle(x+5,y+2,x+55,y+13,fill='grey')
        if self.percent_done != None:
            bar_width = 35
            self.progress_bar_bg = self.display.create_rectangle(x+5,y+16,x+6+bar_width,y+22,fill='magenta')
            self.progress_bar = self.display.create_line(x+6,y+19,
                                                         x+6+(bar_width*self.percent_done/100.0), y+19,
                                                         fill='yellow', width=5)
            
            self.progress_percent_display =  self.display.create_text(x+50, y+18,
                                                              text = str(self.percent_done)+"%",
                                                              fill = 'green',font=8)

    def update_state(self, state):
        self.state = state
        x, y = self.x, self.y
        self.display.delete(self.state_display) # "undraw" the prev. state message
        self.state_display = self.display.create_text(x+85, y+8, text=self.state, fill='light blue')
        now = time.time()
        self.timer_started = now
        self.update_timer(0)
        
    def update_timer(self, seconds):
        x, y = self.x, self.y
        self.timer_seconds = seconds
        self.timer_string = HMS(seconds)
        self.display.delete(self.timer_display)
        self.timer_display = self.display.create_text(x+85, y+18, text=self.timer_string,fill='white')

    def load_tape(self, volume_name):
        self.volume = Volume(volume_name, self.display)
        self.volume.x, self.volume.y = self.x + 5, self.y + 5
        self.volume.draw()

    def unload_tape(self, volume):
        self.volume.moveto(self.volume.x + 150, self.volume.y)

    def show_progress(self, percent_done):
        x,y=self.x,self.y
        bar_width = 35
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
        self.progress_bar_bg = self.display.create_rectangle(x+5,y+16,x+6+bar_width,y+22,fill='magenta')
        self.progress_bar = self.display.create_line(x+6,y+19,
                                                     x+6+bar_width*(self.percent_done/100.0),y+19,
                                                     fill='yellow', width=5)
        self.progress_percent_display =  self.display.create_text(x+50, y+18,
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

    def __del__(self):
        self.display.delete(self.timer_display)
        self.display.delete(self.outline)
        self.display.delete(self.label)
        self.display.delete(self.state_display)
        self.display.delete(self.tape_slot)
        self.display.delete(self.progress_bar_bg)
        self.display.delete(self.progress_bar)
        self.display.delete(self.progress_percent_display)

    
class Client:

    def __init__(self, name, display):
        self.name = name
        self.width = 100
        self.height = 25
        self.display = display
        self.last_activity_time = 0.0 
        self.n_connections = 0
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
        self.outline =  self.display.create_oval(x, y, x+self.width, y+self.height, fill='yellow')
        self.label = self.display.create_text(x, y, text=None)
        index1=self.display.bbox(self.outline)
        index2=self.display.bbox(self.label)

        outline_coords=[]
        label_coords=[]

        for i in index1:
            outline_coords.append(i)
        x0=outline_coords[0]
        y0=outline_coords[1]
        x1=outline_coords[2]
        y1=outline_coords[3]

        for j in index2:
            label_coords.append(j)
        label_x0=label_coords[0]
        label_y0=label_coords[1]
        label_x1=label_coords[2]
        label_y1=label_coords[3]
        width=label_x1-label_x0
        height=label_y1-label_y0

        label_x=((x1-x0)-width)/2.0
        label_y=((y1-y0)-height)/2.0

        self.label = self.display.create_text(x0+label_x, y0+label_y+7, text=self.name)
        
    def __del__(self):
        self.display.delete(self.outline)
        self.display.delete(self.label)
        
class Connection:
    """ a line connecting a mover and a client"""
    def __init__(self, mover, client, display):
        self.mover_end = mover.x, mover.y + mover.height/2.0 # middle of left side
        self.client_end = client.x+client.width, client.y + client.height/2.0 #middle of right side
        self.display = display
        self.dashoffset = 0 #current offset
        self.start_offset = 0 #offset when we got most recent rate info
        self.rate = 0 #pixels/second, not MB
        self.segment_start_time = 0
        self.segment_stop_time = 0

    def draw_line(self):
        self.line = self.display.create_line(self.mover_end[0], self.mover_end[1],
                                             self.mover_end[0]-300, self.mover_end[1],
                                             self.client_end[0]+200,self.client_end[1],
                                             self.client_end[0],self.client_end[1],
                                             dash='...-',dashoffset = self.dashoffset,width=2,
                                             smooth=1)
        
    def delete(self):
        self.display.delete(self.line)

    def update_rate(self, rate):
        now = time.time()
        self.segment_start_time = now #starting time at this rate
        self.segment_stop_time = now + 5 #let the animation run 5 seconds
        self.segment_start_offset = self.dashoffset
        self.rate = rate
        
    def animate(self):
        now=time.time()
        if now >= self.segment_stop_time:
            return

        new_offset = self.segment_start_offset + self.rate * (
            now-self.segment_start_time) 
    
        if new_offset != self.dashoffset:  #we need to redraw the line
            self.dashoffset = new_offset
            self.display.itemconfigure(self.line,dashoffset=new_offset)
        
class Robot:
    pass

class Volume:
    def __init__(self, name, display):
        self.name = name
        self.display = display

    def draw(self):
        x, y = self.x, self.y
        self.outline = self.display.create_rectangle(x, y-3, x+50, y+8, fill = 'orange')
        self.label = self.display.create_text(x, y, text = None)
        index1=self.display.bbox(self.outline)
        index2=self.display.bbox(self.label)
  
        outline_coords=[]
        label_coords=[]

        for i in index1:
            outline_coords.append(i)
        x0=outline_coords[0]
        y0=outline_coords[1]
        x1=outline_coords[2]
        y1=outline_coords[3]

        for j in index2:
            label_coords.append(j)
        label_x0=label_coords[0]
        label_y0=label_coords[1]
        label_x1=label_coords[2]
        label_y1=label_coords[3]
        width=label_x1-label_x0
        height=label_y1-label_y0

        label_x=((x1-x0)-width)/2.0
        label_y=((y1-y0)-height)/2.0

        self.label = self.display.create_text(x0+label_x, y0+label_y+7, text=self.name,fill='white')
        

        

    def moveto(self, x, y):
        dx, dy = x - self.x, y - self.y
        self.display.move(self.outline, dx, dy)
        self.display.move(self.label, dx, dy)
        self.x, self.y = x, y

    def delete(self):
        self.display.delete(self.outline)
        self.display.delete(self.label)

        
class Display(Canvas):
    """  The main state display """
    def __init__(self, master, **attributes):
        ##** means "variable number of keyword arguments" (passed as a dictionary)
        Canvas.__init__(self, master)
        self.configure(attributes)
        self.pack()
        self.stopped = 0
        self.width =  int(self['width'])
        self.height = int(self['height'])
        
        self.movers = {} ## This is a dictionary keyed by mover name,
                               ##value is an instance of class Mover
        self.clients = {} ## dictionary, key = client name, value is instance of class Client
        self.client_positions = {} #key is position index (0,1,-1,2,-2) and value is Client

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #use IP addressing and UDP protocol
        port = os.environ.get("ENSTORE_WATCH_PORT", DEFAULTPORT)
        myaddr = ("", port) # "" = local host
        s.bind(myaddr)
        self.inputs = [s]
        
    def create_movers(self, mover_names):
        #Create a Mover class instance to represent each mover.
        N = len(mover_names)
        angle = math.pi / (N-1)
        for k in range(N):
            mover_name = mover_names[k]
            ##These are coordinates on the unit circle
            M = Mover(mover_name, self)
            self.movers[mover_name] = M
            x, y = 0.75 + 0.8 * math.cos(math.pi/2 + angle*k), 0.85 * math.sin(math.pi/2 + angle*k)
            M.x, M.y = scale_to_display(x, y, self.width, self.height)
            M.draw()

    def handle_command(self, command):
        ## Accept commands of the form:
        # 1 word:
        # quit
        # 3 words:
        # state MOVER_NAME STATE_NAME
        # connect MOVER_NAME CLIENT_NAME
        # disconnect MOVER_NAME CLIENT_NAME
        # load MOVER_NAME VOLUME_NAME
        # unload MOVER_NAME VOLUME_NAME
        # 4 words:
        # transfer MOVER_NAME nbytes total_bytes
        # variable number of words
        # movers M1 M2 M3 ...
        # title (?)
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

        if words[0]=='title':
            print "sorry, not yet"
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
            mover.update_state(what_state)
            return
        
        if words[0]== 'connect':
            #print "CONNECT", words
            client_name = words[2]
            client = self.clients.get(client_name)
            if not client: ## New client, we must add it
                client = Client(client_name, self)
                self.clients[client_name] = client
                client.draw()
            client.last_activity_time = now
            connection = Connection(mover, client, self)
            mover.t0 = now
            mover.b0 = 0
            connection.update_rate(0)
            connection.draw_line()
            mover.client = client
            mover.connection = connection
            client.n_connections = client.n_connections + 1
            return
        
        if words[0]=='disconnect':
            client_name = words[2]
            client = self.clients.get(client_name)
            if not client: ## this client is not displayed
                return
            if mover.connection:
                mover.connection.delete()
            mover.client = None
            mover.connection = None
            client.n_connections = client.n_connections - 1
            mover.t0 = time.time()
            mover.b0 = 0
            mover.show_progress(None)
            return
        
        if words[0]=='load':
            what_volume = words[2]
            mover.load_tape(what_volume)
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
            percent_done = int(100 * num_bytes/total_bytes)
            mover.show_progress(percent_done)
            rate = mover.transfer_rate(num_bytes, total_bytes) / (256*1024)
            mover.connection.update_rate(rate)
            mover.client.last_activity_time = time.time()

        if words[0]=='add_mover':
            pass

            
    def mainloop(self):
        # Our mainloop is different from the normal Tk mainloop in that we have
        # (A) an interval timer to control animations and
        # (B) we check for commands coming from standard input
        while not self.stopped:
            #test whether there is a command ready to read, timeout in 1/30 second.
            readable, junk, junk = select.select(self.inputs, [], [], 1.0/30)
            for r in readable:
##                try:
                    command = r.recv(1024)
                    self.handle_command(command)
##                except (socket.error, exceptions.Exception), detail: #catches all possible errors
##                    print detail
                    
            ## Here is where we handle periodic things
            now = time.time()
            #### Update all mover timers
            for mover in self.movers.values():
                seconds = int(now - mover.timer_started)
                if seconds != mover.timer_seconds:
                    mover.update_timer(seconds)           #We must advance the timer
                if mover.connection:
                    mover.connection.animate()
                    
            #### Check for unconnected clients
            for client_name, client in self.clients.items():
                if client.n_connections > 0:
                    continue
                if now - client.last_activity_time >  5: # grace period
                    del client

            ####force the display to refresh
            self.update()
        

if __name__ == "__main__":
    display = Display(None, width=1000, height=700, background='lightblue')
    display.mainloop()

    





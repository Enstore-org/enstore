#!/usr/bin/env python

import Tkinter
import tkMessageBox
import ScrolledText
import time
import string
import os
import sys

class InfoBox(Tkinter.Frame):

	def __init__(self, master, w = 200, h = 20, heading="InfoBox"):
		Tkinter.Frame.__init__(self, master)
		self.label = Tkinter.Label(self, text=heading, bd=1, relief=Tkinter.SUNKEN, anchor=Tkinter.W)
		self.label.configure(foreground="#FFFFFF", background="#0000FF", font = ("Times", 12, "bold"))
		self.label.pack(fill = Tkinter.X)
		self.s = ScrolledText.ScrolledText(self, height = h, width = w)
		#self.s = ScrolledText.ScrolledText(self)
		self.s.pack()
		self.s.config(state=Tkinter.DISABLED)

	def printline(self, s):
		self.s.config(state=Tkinter.NORMAL)
		self.s.insert(Tkinter.END, s)
		self.s.see(Tkinter.END)
		self.s.config(state=Tkinter.DISABLED)

class StatusBar(Tkinter.Frame):
	def __init__(self, master):
		Tkinter.Frame.__init__(self, master)
		self.label = Tkinter.Label(self, text="", bd=1, relief=Tkinter.SUNKEN, anchor=Tkinter.W)
		self.label.pack(fill=Tkinter.X)

	def set(self, format, *args):
		self.label.config(text = format % args)
		self.label.update_idletasks()

	def set(self, s):
		self.label.config(text = s)
		self.label.update_idletasks()

	def clear(self):
		self.label.config(text = "")
		self.label.update_idletasks()


#FILE = "M.log"
FILE = "/data1/Migration_tmp/archive/EAGLE_MIGRATION_2004/MigrationLog@2004-10-26.20:42:59#20233"

quit_now = 0
paused = 0

def set_quit():
	global quit_now
	quit_now = 1
	sys.exit(0)

def set_pause():
	global paused
	paused = 1

def set_resume():
	global paused
	paused = 0

def show_help():
	# God helps those who help themselves
	tkMessageBox.showinfo(
		"God helps those who help themselves",
		"Call Chih-Hao at x8076")

if __name__ == "__main__":
	if len(sys.argv) > 1:
		f = open(sys.argv[1])
	else:
		f = sys.stdin
	root = Tkinter.Tk()
	root.title("Migration")
	root.protocol("WM_DELETE_WINDOW", set_quit)
	menu = Tkinter.Menu(root)
	root.config(menu=menu)
	filemenu = Tkinter.Menu(menu)
	menu.add_cascade(label="File", menu=filemenu)
	filemenu.add_command(label="Exit", command = set_quit)
	action_menu = Tkinter.Menu(menu)
	menu.add_cascade(label="Action", menu=action_menu)
	action_menu.add_command(label="Pause", command = set_pause)
	action_menu.add_command(label="Resume", command = set_resume)
	helpmenu = Tkinter.Menu(menu)
	menu.add_cascade(label="Help", menu=helpmenu)
	helpmenu.add_command(label="Help me!", command = show_help)

	frame1 = Tkinter.Frame(root)
	command_line = InfoBox(frame1, h = 5, w = 150, heading = "Command Line")
	command_line.pack(side=Tkinter.LEFT)
	volume_status = InfoBox(frame1, h = 5, w = 46, heading = "Volume Status")
	volume_status.pack(side=Tkinter.LEFT)
	frame1.pack()
	error_info = InfoBox(root, h = 10,  heading = "Errors")
	error_info.s.config(foreground="#FF0000")
	error_info.pack()
	progress = InfoBox(root, h = 20, heading = "Progress")
	progress.pack()
	copy_to_disk = InfoBox(root, h = 10, heading = "Copying to Disk")
	copy_to_disk.pack()
	copy_to_tape = InfoBox(root, h = 10, heading = "Copying to Tape")
	copy_to_tape.pack()
	swap_metadata = InfoBox(root, h = 10, heading = "Swapping Meta Data")
	swap_metadata.pack()

	# f = open(FILE)
	# read command
	l = f.readline()
	cnl = 1
	while l:
		if quit_now:
			sys.exit(0)
		if paused:
			time.sleep(0.1)
			root.update()
			continue

		l = string.strip(l)
		progress.printline(l+'\n')
		# processing line
		part = string.split(l)
		key = part[5]
		if key == "COMMAND":
			ll = string.join(part[7:])
			command_line.printline(ll+'\n')
		elif key == "COPYING_TO_DISK":
			if part[6] != "processing" and part[6] != "no":
				if part[6] == "tmp":
					ll = string.join(part[9:])
				elif part[6] == "failed":
					ll = string.join(part[6:])
				else:
					ll = string.join(part[7:])
				if part[-1] != 'OK' and part[-1] != 'ERROR':
					copy_to_disk.printline(ll+' ... ')
				else:
					copy_to_disk.printline(ll+'\n')
		elif key == "COPYING_TO_TAPE":
			if part[6] != "removing":
				if part[6] == "copying":
					tf = string.split(os.path.basename(part[-1]), ":")
					ll = part[7] + ' ' + tf[0] + ' ' + tf[1] + ' ... '
						
				elif part[9] == 'is':
					ll = string.join(part[12:])
				else:
					ll = string.join(part[7:])
				if part[-1] != 'OK' and part[-1] != 'ERROR':
					if part[6] == "copying" and not cnl:
						copy_to_tape.printline('\n')
					copy_to_tape.printline(ll)
					cnl = 0
				else:
					copy_to_tape.printline(ll+'\n')
					cnl = 1
		elif key == "SWAPPING_METADATA":
			if part[6] != "swapping":
				if part[11] == "been":
					ll = part[6]+' <--> '+part[8]+' '+part[-2]+' '+part[-1]
				elif part[11] == "already":
					ll = part[6]+' <--> '+part[8]+' have already been swapped ... OK'
				else:
					ll = string.join(part[6:])
				swap_metadata.printline(ll+'\n')
		elif key == "MIGRATING_VOLUME":
			ll = string.join(part[6:])
			volume_status.printline(ll+'\n')
		else:
			pass
		if part[-1] == "ERROR":
			error_info.printline(l+'\n')
		root.update()
		# time.sleep(1)
		l = f.readline()
	root.mainloop()
	


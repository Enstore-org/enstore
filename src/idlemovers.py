#!/usr/bin/python

idle_movers = []	# list of mover tickets

def queue_idle_mover(ticket):
	idle_movers.append(ticket)

def have_idle_mover():
	if not idle_movers:
		return 0
	return 1

def get_idle_mover() :
	# it is the caller's job to call have_idle_mover()
	# to make sure the list is not empty
	m = idle_movers[0]
	idle_movers.remove(m)
	return m

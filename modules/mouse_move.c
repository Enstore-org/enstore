/*
 * Copyright 1989 O'Reilly and Associates, Inc.
 * See ../Copyright for complete rights and liability information.
 */

/* 1-14-2004: This is the mouse move program.  It is based on the basicwin
   program from O'Reilly. */

#include <X11/Xlib.h>
#include <X11/Xutil.h>
#include <X11/Xos.h>
#include <X11/Xatom.h>

#include <stdio.h>

/* These are used as arguments to nearly every Xlib routine, so it saves 
 * routine arguments to declare them global.  If there were 
 * additional source files, they would be declared extern there. */
Display *display;
int screen_num;

static char *progname; /* name this program was invoked by */

int main(argc, argv)
int argc;
char **argv;
{
        unsigned int display_width, display_height;
        char *display_name = NULL;

	progname = argv[0];

	/* connect to X server */
	if ( (display=XOpenDisplay(display_name)) == NULL )
	{
	   (void) fprintf( stderr, "%s: cannot connect to X server %s\n", 
	   		progname, XDisplayName(display_name));
		exit( -1 );
	}

	/* get screen size from display structure macro */
	screen_num = DefaultScreen(display);
	display_width = DisplayWidth(display, screen_num);
	display_height = DisplayHeight(display, screen_num);

	/* move the mouse off of the screen */
	XWarpPointer(display, None, None, 0, 0, display_width, display_height,
		     display_width, display_height);

	/* close the X connection */
	XCloseDisplay(display);

	return 0;
}

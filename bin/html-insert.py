###############################################################################
# src/$RCSfile$   $Revision$
#
"""
	This program finds the first occurance of matching adjacent 
	(except for white space) SGML tags in a file and inserts
	a file specified on the command line between the tags.
 
	The tag body is passed as a command line argument number 1.

	The file to be inserted is passed as command line argument 2.

	Example:

	html-insert BERMANHEADER header.html < dog.html > WWW_new/dog.html

	inserts the contents of the file header.html into the body of
	dog.html between the SGML tags <BERMANHEADER> </BERMANHEADER>

"""
import sys
import regsub

whitespat = "[ \t\n]*"   # pattern for whitespace
tagbase =  sys.argv[1]   # the meaty part fo the tag
tagfirst = "<"  + tagbase + ">"      #i.e  <BERMANHEADER>
taglast  = "</" + tagbase + ">"	     #i.e </BERMANHEADER>
tagfirstpat = "<"                    + whitespat + tagbase +  whitespat + ">"
taglastpat  = "<"  + whitespat + "/" + whitespat + tagbase +  whitespat + ">"

#get the whole text of the inserted file and the input file 
insert = open(sys.argv[2], "r").read()
htmlin = sys.stdin.read()

# Substitute for the  first occurance of a tag pair with only whitespace
# between the tag pair. "htmlout is the whle output file. 

htmlout = regsub.sub( 
		tagfirstpat + whitespat + taglastpat, 
		tagfirst + insert + taglast,
		htmlin)

# write the whole file out to stdout.
sys.stdout.write(htmlout)

# warn the user if nothing was changed.
if htmlin == htmlout :
	sys.stdout.write("Warning: input was not modified\n")







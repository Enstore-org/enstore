import sys
import getopt
import string

def ndup(infile,outfile,n):

    inf = open(infile,'r')
    data =inf.read()
    inf.close()

    out = open(outfile,'wb')
    for copy in range(0,n):
        out.write(data)
    out.close

if __name__ == "__main__" :
    count = 0
    infile = ""
    outfile = ""

    # see what the user has specified. bomb out if wrong options specified
    options = ["infile=","outfile=","count=","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--infile" :
            infile = value
        elif opt == "--outfile" :
            outfile = value
        elif opt == "--count" :
            count = string.atoi(value)
        elif opt == "--list" or opt == "--verbose":
            list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    if count>0 and len(infile)>0 and len(outfile)>0:
        ndup(infile,outfile,count)
    else:
        print "python ",sys.argv[0], options
        print "   do not forget the '--' in front of each option"
        sys.exit(1)


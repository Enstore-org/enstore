import sys
import whrandom
import array
import getopt
import string

def make_random_file(file,size) :
    f = open(file,'wb')
    a = array.array('B')
    for i in range(0,size) :
	a.append(whrandom.randint(0,255))
    a.tofile(f)
    f.close()

if __name__ == "__main__" :
    # defaults
    size = 1000
    file = "random.fake"
    list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["size=","file=","list=","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--size" :
            size = string.atoi(value)
        elif opt == "--file" :
            file = value
        elif opt == "--list" :
            list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    make_random_file(file,size)

import string

charset=string.join((string.letters,string.digits,'_','-'),'')

def is_in_charset(string):
    #print "charset",charset
    for ch in string:
        if not ch in charset:
            break
    else:
        return 1
    return 0

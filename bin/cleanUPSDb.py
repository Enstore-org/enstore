import re
import string


def remove_pdp(pattern, value):
    match = re.match(pattern, value)
    if not match is None:
        return re.sub(pattern, "", value)
    return value

def set_quals(QUALS):
    if QUALS != "":
        QUALS = "with qualifiers %s"%(QUALS,)
    return QUALS

def format(values):
    newvalues = []
    for value in values:
        newvalues.append(string.replace(string.strip(value), '"', ''))
    return newvalues

if __name__ == "__main__" :

    import sys
    import os

    # pull out the argvs
    PROD_DIR_PREFIX = sys.argv[1]
    DB = sys.argv[2]

    # just in case the qualifiers is "", make this at least a space
    info = string.strip(sys.argv[3])
    (PRODUCT, VERSION, FLAVOR, QUALS, TABLE_DIR, PROD_DIR) = \
              string.split(info, " ")

    # remove quotes and extra spaces
    [PROD_DIR_PREFIX, DB, PRODUCT, VERSION, FLAVOR, QUALS, TABLE_DIR,
     PROD_DIR] = format([PROD_DIR_PREFIX, DB, PRODUCT, VERSION, FLAVOR, QUALS,
                         TABLE_DIR, PROD_DIR])

    # check if the TABLE_DIR or PROD_DIR has the PROD_DIR_PREFIX in it
    pattern = string.strip("^%s"%(PROD_DIR_PREFIX,))
    if pattern[-1] != "/":
        pattern = "%s/"%(pattern,)
    
    TABLE_DIR_NEW = remove_pdp(pattern, TABLE_DIR)
    PROD_DIR_NEW = remove_pdp(pattern, PROD_DIR)

    # if the TABLE_DIR_NEW is just PROD_DIR_NEW/ups, then set it to just 'ups'
    if TABLE_DIR_NEW == "%s/ups"%(PROD_DIR_NEW,):
        TABLE_DIR_NEW = "ups"

    # if we have actually changed something we must do a ups modify
    QUALS = set_quals(QUALS)
    do_it = 0
    if not TABLE_DIR_NEW == TABLE_DIR:
        do_it = 1
        print "please change TABLE_DIR to %s"%(TABLE_DIR_NEW,),
    if not PROD_DIR_NEW == PROD_DIR:
        if do_it:
            print "and",
        print "please change PROD_DIR to %s "%(PROD_DIR_NEW,),
        do_it = 1
    if do_it:
        print "for %s %s %s %s"%(PRODUCT, VERSION, FLAVOR, QUALS)

        
        

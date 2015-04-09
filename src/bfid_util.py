import re
import string
import threading
import time
import types

MAGIC_NUMBER = 100000

def is_bfid(bfid):
    """
    Check if input string is a bfid

    :type bfid: :obj:`str`
    :arg  bfid: bfid to check

    :rtype: :obj:`bool` - True/False

    """
    if type(bfid) == types.StringType:

        #Older files that do not have bfid brands should only be digits.
        result = re.search("^[0-9]{11,16}$", bfid)
        if result != None:
            return 1

        #The only part of the bfid that is of constant form is that the last
        # n characters are all digits.  There are no restrictions on what
        # is in a brand or how long it can be (though there should be).
        # Since, the bfid is based on its creation time, as time passes the
        # number of digits in a bfid will grow.  (Assume 12 as minumum).
        result = re.search("^[a-zA-Z0-9]*[0-9]{11,16}$", bfid)
        if result != None:
            return 1

        #Allow for bfids of file copies.
        #result = re.search("^[a-zA-Z0-9]*[0-9]{13,15}_copy_[0-9]+$", bfid)
        #if result != None:
        #    return 1

        #Some older files (year 2000) have a long() "L" appended to
        # the bfid.  This seems to be consistant between the file
        # database and layers one & four.  So, return true in these cases.
        result = re.search("^[0-9]{11,16}L{1}$", bfid)
        if result != None:
            return 1

        #6 files on D0en have brands and the "L" appended to them.  They
        # belong to PRF355, PRF532 and PRF545.
        ## This part of the function should go away when those bfids go.
        result = re.search("^[a-zA-Z0-9]*[0-9]{11,16}L{1}$", bfid)
        if result != None:
            return 1
    return 0

def extract_brand(bfid):
    """
    Extract brand from bfid

    :type bfid: :obj:`str`
    :arg  bfid: bfid

    :rtype: :obj:`str` - brand

    """
    if is_bfid(bfid):
        #Older files that do not have bfid brands should only be digits.
        #
        #Some older files (year 2000) have a long() "L" appended to
        # the bfid.  This seems to be consistant between the file
        # database and layers one & four.  So, return true in these cases.
        result = re.search("^[0-9]{13,15}L{0,1}$", bfid)
        if result != None:
            return ""

        #The only part of the bfid that is of constant form is that the last
        # n characters are all digits.  There are no restrictions on what
        # is in a brand or how long it can be (though there should be).
        # Since, the bfid is based on its creation time, as time passes the
        # number of digits in a bfid will grow.  (Assume 14 as minumum).
        result = re.search("[0-9]{13,15}$", bfid)
        if result != None:
            brand = bfid[:-(len(result.group()))]
            if brand.isalnum():
                return brand

    return None

def strip_brand(bfid):
    """
    Strip brand off of bfid string

    :type bfid: :obj:`str`
    :arg  bfid: bfid

    :rtype: :obj:`str` - bfid without brand substring

    """
    if is_bfid(bfid):
        #Older files that do not have bfid brands should only be digits.
        #
        #Some older files (year 2000) have a long() "L" appended to
        # the bfid.  This seems to be consistant between the file
        # database and layers one & four.  So, return true in these cases.
        result = re.search("^[0-9]{13,15}L{0,1}$", bfid)
        if result != None:
            return bfid

        #The only part of the bfid that is of constant form is that the last
        # n characters are all digits.  There are no restrictions on what
        # is in a brand or how long it can be (though there should be).
        # Since, the bfid is based on its creation time, as time passes the
        # number of digits in a bfid will grow.  (Assume 14 as minumum).
        result = re.search("[0-9]{13,15}$", bfid)
        if result != None:
            brand = bfid[:-(len(result.group()))]
            if brand.isalnum():
                return bfid[-(len(result.group())):]

    return None

def bfid2time(bfid):
    """
    Extract time from bfid

    :type bfid: :obj:`str`
    :arg  bfid: bfid to check

    :rtype: :obj:`int` - time part of bfid sequence

    """

    if bfid[-1] == "L":
        e = -6
    else:
        e = -5

    if bfid[0].isdigit():
        i = 0
    elif bfid[3].isdigit():
        i = 3
    else:
        i = 4

    t = int(bfid[i:e])
    if t > 1500000000:
        t = t - 619318800
    return t


class BfidGenerator:
    """
    Singleton class that handles bfid generation in thread safe way
    bfid is defined as brand + str(time.time()*MAGIC_NUMBER+counter)

    """

    singleton = False

    def __init__(self,brand):
        """
        :type brand: :obj:`str`
        :arg  brand: bfid brand
        """
        if BfidGenerator.singleton:
            raise Exception("more than one instance of singleton disallowed")

        self.__brand      = brand.upper()
        self.__timestamp  = int(time.time())
        self.__counter    = 0
        self.__lock       = threading.Lock()
        BfidGenerator.singleton = True

    def get_brand(self):
        """
        return value of brand

        :rtype: :obj:`str` - string value of brand

        """
        return self.__brand

    def set_brand(self,brand):
        """
        set brand

        :type brand: :obj:`str`
        :arg  brand: bfid brand a four letter word

        """
        self.__brand = brand

    def check(self,bfid):
        """
        check bfid

        :type bfid: :obj:`str`
        :arg  bfid: bfid to check

        :rtype: :obj:`tuple` (True/False, reason)

        """
        sbfid = str(bfid)
        if sbfid[:len(self.__brand)] != self.__brand:
            return (False,"wrong brand {} ({})".format( sbfid[:len(self.__brand)],self.__brand))
        if not is_bfid(sbfid):
            return (False,"invalid bfid {}".format(sbfid))
        else:
            return (True, "good")

    def create(self):
        """
        create unique bfid

        :rtype: :obj:`Bfid` Bfid

        """
        try:
            self.__lock.acquire()
            bfid = int(time.time())
            if bfid > self.__timestamp:
                self.__counter = 0
                self.__timestamp = bfid
            else:
                self.__counter += 1
            bfid = self.__timestamp*MAGIC_NUMBER+\
                   self.__counter%MAGIC_NUMBER
            return self.__brand+str(bfid)
        finally:
            self.__lock.release()

if __name__ == "__main__":
    generator = BfidGenerator("CMS")
    bfid = generator.create()
    print bfid, str(bfid), bfid[:10]
    print generator.check("GCMS14261923030000x1")
    print generator.check("CMS1426192303000001")


    bfid = generator.create()
    print "----->", bfid
    generator.set_brand("")
    bfid = generator.create()
    print "----->", bfid

    try:
        generator = BfidGenerator("ABC")
    except Exception, msg:
        print "Failed :",str(msg)

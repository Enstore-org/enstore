###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import regex

# enstore imports
import Trace

class NumOper:

  def __init__(self,str=""):
    Trace.trace(10,'{__init__ NumOper str='+repr(str))
    if len(str)==0:
        return
    self.val=[]
    self.parse_oper(str)
    Trace.trace(10,'}__init__ NumOper')

  def parse_oper(self,str):
    Trace.trace(16,'{parse_oper str='+repr(str))
    index1=regex.search("[<>=!]",str)
    if index1!=0:
        self.val.append(str[:index1])
    index2=regex.search("[^<>=!]",str[index1:])
    self.operator=str[index1:index2+index1]
    self.val.append(str[index2+index1:])
    Trace.trace(16,'}parse_oper')

  def numcmp(self,value):
     Trace.trace(16,'{numcmp value='+repr(value))
     if len(self.val)==2 and self.operator=="<>":
        if value >= self.val[0] and value <= self.val[1]:
          Trace.trace(16,'}numcmp 0')
          return 0
     elif self.operator == ">=":
        if value >= self.val[0]:
          Trace.trace(16,'}numcmp 0')
          return 0
     elif self.operator == ">":
        if value > self.val[0]:
          Trace.trace(16,'}numcmp 0')
          return 0
     elif self.operator == "<=":
        if value <= self.val[0]:
          Trace.trace(16,'}numcmp 0')
          return 0
     elif self.operator == "<":
        if value < self.val[0]:
          Trace.trace(16,'}numcmp 0')
          return 0
     elif self.operator == "==":
        if value ==self.val[0]:
          Trace.trace(16,'}numcmp 0')
          return 0
     elif self.operator == "!=":
        if value != self.val[0]:
          Trace.trace(16,'}numcmp 0')
          return 0

     Trace.trace(16,'}numcmp 0')
     return 1


class TimeOper(NumOper):

  def __init__(self,str=""):
    Trace.trace(10,'{__init__ TimeOper str='+repr(str))
    NumOper.__init__(self,str)
    self.secs()
    Trace.trace(10,'}__init__ TimeOper')

  def secs(self):
    Trace.trace(16,'{secs')
    j=0
    for value in self.val:
       dt=value[:4]+","
       k=1
       for i in range(4,len(value),2):
          dt=dt+self.zstrip(value[i:i+2])+','
          k=k+1
       for i in range(k,9):
          dt=dt+"0,"
       dt=dt[:-1]
       self.val[j]=time.mktime(eval(dt))-60*60
       j=j+1
    Trace.trace(16,'}secs')

  def zstrip(self,value):
    if 0: print self #quiet the linter
    Trace.trace(16,'{zstrip value='+repr(value))
    if value[0]=='0':
        return value[1:]
    Trace.trace(16,'}zstrip value='+repr(value))
    return value

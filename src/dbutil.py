###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import regex

class numOper:
  def __init__(self,str=""):
    if len(str)==0:
	return
    self.val=[]
    self.parseOper(str)
  def parseOper(self,str):
    index1=regex.search("[<>=!]",str)
    if index1!=0:
	self.val.append(str[:index1])
    index2=regex.search("[^<>=!]",str[index1:])
    self.operator=str[index1:index2+index1]
    self.val.append(str[index2+index1:])
  def numcmp(self,value):
     if len(self.val)==2 and self.operator=="<>":
	if value >= self.val[0] and value <= self.val[1] : return 0
     elif self.operator == ">=" :
	if value >= self.val[0] : return 0
     elif self.operator == ">" :
	if value > self.val[0] : return 0
     elif self.operator == "<=" :
	if value <= self.val[0] : return 0
     elif self.operator == "<" :
	if value < self.val[0] : return 0
     elif self.operator == "==" :
	if value ==self.val[0] : return 0
     elif self.operator == "!=" :
	if value != self.val[0] : return 0
     return 1


class timeOper(numOper):
  def __init__(self,str=""):
    numOper.__init__(self,str)
    self.secs()
  def secs(self):
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
  def zstrip(self,value):
    if value[0]=='0':
	return value[1:]
    return value






#!/usr/bin/python

# import modules
import sys
import os
import pdb
import string
import re

NAME  = "name"
CABLE = "cable"
PROD  = "prod"	
YES   = "yes"
NO    = "no"
ORDER = "order"

#set up the nodes dictionary
# 1st South Cable:
# srv1  srv2   srv4   mvr3a  mvr2a  mvr1a    4/1  4/2  4/3  4/4  4/5  4/6
# srv3  unused mvr4a  mvr4b  mvr5a  mvr5b    4/7  4/8  4/9  4/10 4/11 4/12

# 2nd South Cable:
# mvr9a  mvr9b mvr10a mvr10b mvr11a mvr11b  4/13 4/14 4/15 4/16 4/17 4/18
# mvr6a  mvr6b mvr7a  mvr7b  mvr8a  mvr8b   4/19 4/20 4/21 4/22 4/23 4/24

# 3rd South Cable:
# debug1 debug2 spr1a  spr1b  spr2a  spr2b   4/25 4/26 4/27 4/28 4/29 4/30
# pwr1   pwr2   unused unused unused unused  4/31 4/32 4/33 4/34 4/35 4/36

# 1st North Cable:
# mvr12a mvr12b mvr13a mvr13b mvr14a mvr14b  4/37 4/38 4/39 4/40 4/41 4/42
# mvr15a mvr15b mvr16a mvr16b mvr17a mvr17b  4/43 4/44 4/45 4/46 4/47 4/48

# 2nd Cable:
# mvr18a mvr18b srv5 unused pwr3  (pwr4)     5/41 5/42 5/43 5/44 5/45 5/46
# debug3 debug4 dead dead   dead  dead       5/47 5/48

# 1st RIP Cable:
# rip3  rip4  rip1  rip9   rip7    rip10     5/9  5/10 5/11 5/12 5/13 5/14
# rip8  rip6  rip5  ?????  rippwr1 ripsgi    5/15 5/16 5/17 5/18 5/19 5/20

# 2nd RIP Cable:
# rip2   ripcon unused unused unused unused  5/21 5/22 5/23 5/24 5/25 5/26
# unused dead   dead   dead   dead   dead    5/27

# If you want to add a new node to monitor, add it in the node dictionary
# node_d with initiated node key, node NAME, node PRODucte state, node
# CABLE number and node ORDER here.

node_d = {}
    
node_d["srv1"] = { NAME:'D0Ensrv1',
                       PROD:YES,
                       CABLE:107,
                       ORDER:1}

node_d["srv2"] = { NAME:'D0Ensrv2',
                       PROD:YES,
                       CABLE:108,
                       ORDER:2}

node_d["srv3"] = { NAME:'D0Ensrv3',
                       PROD:YES,
                       CABLE:113,
                       ORDER:3}
 
node_d["srv4"] = { NAME:'D0Ensrv4',
                       PROD:YES,
                       CABLE:109,
                       ORDER:4}
     
node_d["srv5"] = { NAME:'D0Ensrv5',
                       PROD:NO,
                       CABLE:53,
                       ORDER:5}


node_d["pwr1"] = { NAME:'D0EnPwr1',
                       PROD:YES,
                       CABLE:137,
                       ORDER:6}

node_d["pwr2"] = { NAME:'D0EnPwr2',
                       PROD:YES,
                       CABLE:138,
                       ORDER:7}

node_d["pwr3"] = { NAME:'D0EnPwr3',
                       PROD:NO,
                       CABLE:55,
                       ORDER:8}

node_d["rippwr1"] = { NAME:'RipPwr1',
                       PROD:NO,
                       CABLE:29,
                       ORDER:9}
    
node_d["adic2"] = { NAME:'Adic2',
                       PROD:YES,
                       CABLE:42,
                       ORDER:10}

node_d["mvr1a"] = { NAME:'D0enMvr1a',
                       PROD:YES,
                       CABLE:110,
                       ORDER:11}
 
node_d["mvr2a"] = { NAME:'D0enMvr2a',
                       PROD:YES,
                       CABLE:111,
                       ORDER:12}
 
node_d["mvr3a"] = { NAME:'D0enMvr3a',
                       PROD:YES,
                       CABLE:112,
                       ORDER:13}

node_d["mvr4a"] = { NAME:'D0enMvr4a',
                       PROD:YES,
                       CABLE:115,
                       ORDER:14}

node_d["mvr4b"] = { NAME:'D0enMvr4b',
                       PROD:YES,
                       CABLE:116,
                       ORDER:15}

node_d["mvr5a"] = { NAME:'D0enMvr5a',
                       PROD:YES,
                       CABLE:117,
                       ORDER:16}

node_d["mvr5b"] = { NAME:'D0enMvr5b',
                       PROD:YES,
                       CABLE:118,
                       ORDER:17}

node_d["mvr6a"] = { NAME:'D0enMvr6a',
                       PROD:YES,
                       CABLE:125,
                       ORDER:18}

node_d["mvr6b"] = { NAME:'D0enMvr6b',
                       PROD:YES,
                       CABLE:126,
                       ORDER:19}

node_d["mvr7a"] = { NAME:'D0enMvr7a',
                       PROD:YES,
                       CABLE:127,
                       ORDER:20}

node_d["mvr7b"] = { NAME:'D0enMvr7b',
                       PROD:YES,
                       CABLE:128,
                       ORDER:21}

node_d["mvr8a"] = { NAME:'D0enMvr8a',
                       PROD:YES,
                       CABLE:129,
                       ORDER:22}

node_d["mvr8b"] = { NAME:'D0enMvr8b',
                       PROD:YES,
                       CABLE:130,
                       ORDER:23}

node_d["mvr9a"] = { NAME:'D0enMvr9a',
                       PROD:YES,
                       CABLE:119,
                       ORDER:24} 
                       
node_d["mvr9b"] = { NAME:'D0enMvr9b',
                       PROD:YES,
                       CABLE:120,
                       ORDER:25}

node_d["mvr10a"] = { NAME:'D0enMvr10a',
                       PROD:YES,
                       CABLE:121,
                       ORDER:26} 
                       
node_d["mvr10b"] = { NAME:'D0enMvr10b',
                       PROD:YES,
                       CABLE:122,
                       ORDER:27}

node_d["mvr11a"] = { NAME:'D0enMvr11a',
                       PROD:YES,
                       CABLE:123,
                       ORDER:28} 
                       
node_d["mvr11b"] = { NAME:'D0enMvr11b',
                       PROD:YES,
                       CABLE:124,
                       ORDER:29}

node_d["mvr12a"] = { NAME:'D0enMvr12a',
                       PROD:YES,
                       CABLE:143,
                       ORDER:30} 
                       
node_d["mvr12b"] = { NAME:'D0enMvr12b',
                       PROD:YES,
                       CABLE:144,
                       ORDER:31} 

node_d["mvr13a"] = { NAME:'D0enMvr13a',
                       PROD:NO,
                       CABLE:145,
                       ORDER:32} 
                       
node_d["mvr13b"] = { NAME:'D0enMvr13b',
                       PROD:NO,
                       CABLE:146,
                       ORDER:33}

node_d["mvr14a"] = { NAME:'D0enMvr14a',
                       PROD:NO,
                       CABLE:147,
                       ORDER:34} 
                       
node_d["mvr14b"] = { NAME:'D0enMvr14b',
                       PROD:NO,
                       CABLE:148,
                       ORDER:35}

node_d["mvr15a"] = { NAME:'D0enMvr15a',
                       PROD:NO,
                       CABLE:149,
                       ORDER:36} 
                       
node_d["mvr15b"] = { NAME:'D0enMvr15b',
                       PROD:NO,
                       CABLE:150,
                       ORDER:37} 

node_d["mvr16a"] = { NAME:'D0enMvr16a',
                       PROD:NO,
                       CABLE:151,
                       ORDER:38} 
                       
node_d["mvr16b"] = { NAME:'D0enMvr16b',
                       PROD:NO,
                       CABLE:152,
                       ORDER:39} 
 
node_d["mvr17a"] = { NAME:'D0enMvr17a',
                       PROD:NO, 
                       CABLE:153,
                       ORDER:40} 
                       
node_d["mvr17b"] = { NAME:'D0enMvr17b',
                       PROD:NO, 
                       CABLE:156,
                       ORDER:41}

node_d["mvr18a"] = { NAME:'D0enMvr18a',
                       PROD:NO, 
                       CABLE:51,
                       ORDER:42} 
                       
node_d["mvr18b"] = { NAME:'D0enMvr18b',
                       PROD:NO, 
                       CABLE:52,
                       ORDER:43}
    
node_d["spr1a"] = { NAME:'D0EnSpr1a',
                       PROD:NO,    
                       CABLE:133,
                       ORDER:44}
    
node_d["spr1b"] = { NAME:'D0EnSpr1b',
                       PROD:NO,
                       CABLE:134,
                       ORDER:45}
    
node_d["spr2a"] = { NAME:'D0EnSpr2a',
                       PROD:NO,
                       CABLE:135,
                       ORDER:46}

node_d["spr2b"] = { NAME:'D0EnSpr2b',
                       PROD:NO,
                       CABLE:136,
                       ORDER:47}

node_d["rip1"] = { NAME:'Rip1',
                       PROD:NO,    
                       CABLE:21,
                       ORDER:48}

node_d["rip2"] = { NAME:'Rip2',
                       PROD:NO,
                       CABLE:31,
                       ORDER:49}
 
node_d["rip3"] = { NAME:'Rip3',
                       PROD:NO,
                       CABLE:19,
                       ORDER:50}

node_d["rip4"] = { NAME:'Rip4',
                       PROD:NO,
                       CABLE:20,
                       ORDER:51}

node_d["rip5"] = { NAME:'Rip5',
                       PROD:NO,
                       CABLE:27,
                       ORDER:52}

node_d["rip6"] = { NAME:'Rip6',
                       PROD:NO,
                       CABLE:26,
                       ORDER:53}

node_d["rip7"] = { NAME:'Rip7',
                       PROD:NO,
                       CABLE:23,
                       ORDER:54}

node_d["rip8"] = { NAME:'Rip8',
                       PROD:NO,
                       CABLE:25,
                       ORDER:55}

node_d["ripsgi"] = { NAME:'RipSGI',
                       PROD:NO,    
                       CABLE:30,
                       ORDER:56}
 
node_d["ripcon"] = { NAME:'RipCon',
                       PROD:NO,
                       CABLE:32,
                       ORDER:57}
   
node_d["pwr4"] = { NAME:'D0EnPwr4',
                       PROD:NO,
                       CABLE:56,
                       ORDER:58}

node_d["adic3"] = { NAME:'D0EnAdic3',
                       PROD:NO,
                       CABLE:43,
                       ORDER:59}


























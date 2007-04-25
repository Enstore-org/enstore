#!/usr/bin/env python

import os
import sys
import configuration_server
import pprint

# flatten(x) -- flatten a structure
#       flatten() is a prettier interface of flatten2; it hides the
#       temporary storage for working prefix and partial result.
def flatten(s):
        flat_dict = {}
        if type(s) == type({}):
                for i in s.keys():
                        flatten2(i, s[i], flat_dict)
        elif type(s) == type([]) or type(s) == type(()):
                for i in range(len(s)):
                        flatten2(str(i), s[i], flat_dict)
        else:   # return non structured value directly
                return s

        return flat_dict

def flatten2(prefix, value, flat_dict):
        if type(value) == type({}):
                for i in value.keys():
                        flatten2(prefix+'.'+str(i), value[i], flat_dict)
        elif type(value) == type([]) or type(value) == type(()):
                for i in range(len(value)):
                        flatten2(prefix+'.'+str(i), value[i], flat_dict)
        else:
                flat_dict[prefix] = value



if __name__ == '__main__':      # testing
        # get configuration file path from ENSTORE_CONFIG_FILE
        config_file = os.environ['ENSTORE_CONFIG_FILE']

        # need a ConfigurationDict to read configuration file
        cd = configuration_server.ConfigurationDict()
        cd.read_config(config_file)

        falttened_configdict = flatten(cd.configdict)

        pprint.pprint(falttened_configdict)


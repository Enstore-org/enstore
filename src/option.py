#!/usr/bin/env python

############################################################################
"""
Example option group dictionaries:

To set a simple value: option.py --opt
   example_options = {
       'opt':{HELP_STRING:"some string text"}
       }
The preceding will do the same thing as the full dictionary example:
   example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:1,
              DEFAULT_TYPE:option.STRING,
              VALUE_USAGE:option.IGNORED,
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:0,
              EXTRA_VALUES:[],
        }
    }

To set two values, one saying the option was specifed as a boolean and another
to hold the actuall value (a filename for example): option.py --opt <filename>
    example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:1,
              DEFAULT_TYPE:option.INTEGER,
              VALUE_NAME:'filename'
              VALUE_TYPE:option.STRING,
              VALUE_USAGE:option.REQUIRED,
              VALUE_LABEL:"filename",
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:1, #This means force setting the default too.
              EXTRA_VALUES:[],
        }
    }

To accept multiple values: option.py --opt <filename> [filename2]
    example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:1,
              DEFAULT_TYPE':option.INTEGER,
              VALUE_NAME:'filename'
              VALUE_TYPE:option.STRING,
              VALUE_USAGE:option.REQUIRED,
              VALUE_LABEL:"filename",
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:1,
              EXTRA_VALUES:[{DEFAULT_NAME:"filename2",
                             DEFAULT_VALUE:"",
                             DEFAULT_TYPE:option.STRING,
                             VALUE_NAME:"filename2",
                             VALUE_TYPE:option.STRING,
                             VALUE_USAGE:option.OPTIONAL,
                             VALUE_LABEL:"filename2",
                               }]
        }
    }

"""
############################################################################

import os
import sys
import string
import pprint
import getopt
import fcntl
import TERMIOS

REQUIRED = "required"
OPTIONAL = "optional"
IGNORED  = "ignored"

USER = "user"
ADMIN = "admin"

INTEGER = "integer"
STRING = "string"
FLOAT = "float"

HELP_STRING = "help string"
DEFAULT_NAME = "default name"
DEFAULT_VALUE = "default value"
DEFAULT_TYPE = "default type"
VALUE_NAME = "value name"
VALUE_TYPE = "value type"
VALUE_USAGE = "value usage"
VALUE_LABEL = "value lable"
USAGE_LEVEL = "user level"
SHORT_OPTION = "short option"
FORCE_SET_DEFAULT = "force set default"
USER_LEVEL = "user level"
EXTRA_VALUES = "extra values"

############################################################################

#Note: This list is in alphabetical order, please keep it that way.
ALIVE = "alive"
BFID = "bfid"
CONST = "const"
COUNTERS = "counters"
COUNTERSN = "countersN"
CP = "cp"
CURSOR = "cursor"
DATABASE = "database"
DATABASEN = "databaseN"
DOWN = "down"
DUMP = "dump"
DUPLICATE = "duplicate"
ECHO = "echo"
ENSTORE_STATE = "enstore-state"
FILE_FAMILY = "file-family"
FILE_FAMILY_WIDTH = "file-family-width"
FILE_FAMILY_WRAPPER = "file-family-wrapper"
FILES = "files"
HELP = "help"
ID = "id"
IO = "io"
LAYER = "layer"
LIBRARY = "library"
LS = "ls"
NAMEOF = "nameof"
PARENT = "parent"
PATH = "path"
PNFS_STATE = "pnfs-state"
POSITION = "position"
RETRIES ="retires"
RM = "rm"
SHOWID = "showid"
SIZE = "size"
STORAGE_GROUP = "storage-group"
TAG = "tag"
TAGECHO = "tagecho"
TAGRM = "tagrm"
TAGS = "tags"
TIMEOUT = "timeout"
UP = "up"
USAGE = "usage"
VOLUME = "volume"
XREF = "xref"

#This list is the master list of options allowed.  This is in an attempt
# to keep the different spellings of options (ie. --host vs. --hostip vs --ip)
# in check.
valid_option_list = [ALIVE, BFID, CONST,
                     COUNTERS, COUNTERSN, CP, CURSOR,
                     DATABASE, DATABASEN, DOWN, DUMP, DUPLICATE, ECHO,
                     ENSTORE_STATE, FILE_FAMILY, FILE_FAMILY_WIDTH,
                     FILE_FAMILY_WRAPPER, FILES, HELP, ID, IO,
                     LAYER, LIBRARY, LS, NAMEOF, PARENT, PATH,
                     PNFS_STATE, POSITION, RETRIES, RM, SHOWID, SIZE,
                     STORAGE_GROUP, TAG, TAGECHO, TAGRM,
                     TAGS, TIMEOUT, UP, USAGE, VOLUME, XREF,
                     ]

############################################################################

class Interface:

    def __init__(self, args=sys.argv, user_mode=0):
        if not user_mode: #Admin
            self.user_level = ADMIN
        else:
            self.user_level = USER

        self.argv = args
        
        apply(self.compile_options_dict, self.valid_dictionaries())
        
        self.check_option_names()
        
        self.parse_options()
      
############################################################################

    options = {}
    option_list = []
    args = []

    alive_options = {
        'alive':{DEFAULT_VALUE:1,
                 HELP_STRING:"prints message if the server is up or down.",
                 VALUE_NAME:"alive",
                 VALUE_USAGE:IGNORED,
                 SHORT_OPTION:"a"
                 },
        'timeout':{VALUE_USAGE:REQUIRED,
                   VALUE_TYPE:INTEGER},
        'retries':{VALUE_USAGE:REQUIRED,
                   VALUE_TYPE:INTEGER},
        }

    help_options = {
        'help':{DEFAULT_VALUE:1,
                HELP_STRING:"prints this messge",
                SHORT_OPTION:"h"},
        'usage':{DEFAULT_VALUE:1,
                 VALUE_USAGE:IGNORED}
        }

    trace_options = {
        'do-print':{VALUE_USAGE:REQUIRED,
                    HELP_STRING:"turns on more verbose output"},
        'dont-print':{VALUE_USAGE:REQUIRED,
                      HELP_STRING:"turns off more verbose output"},
        'do-log':{VALUE_USAGE:REQUIRED,
                  HELP_STRING:"turns on more verbose logging"},
        'dont-log':{VALUE_USAGE:REQUIRED,
                    HELP_STRING:"turns off more verbose logging"},
        'do-alarm':{VALUE_USAGE:REQUIRED,
                    HELP_STRING:"turns on more alarms"},
        'dont-alarm':{VALUE_USAGE:REQUIRED,
                      HELP_STRING:"turns off more alarms"}
        }

    test_options = {
        'test':{DEFAULT_VALUE:2,
                DEFAULT_TYPE:INTEGER,
                HELP_STRING:"test",
                VALUE_USAGE:OPTIONAL,
                SHORT_OPTION:"t",
                USER_LEVEL:ADMIN
                }
        }

############################################################################

    def print_help(self):
        list = self.options.keys()
        list.sort()
        for opts in list:

            #Don't even print out options that the user doesn't have access to.
            if self.options[opts].get(USER_LEVEL, USER) == ADMIN \
               and self.user_level == USER:
                continue

            #Snag all optional/required values that belong to this option.
            # Do this by getting the necessary fields from the dictionary.
            # (ie. "value_name"/"default_name" and "value_usage".)  Get the
            # list of extra options, if any.  Insert the values at the
            # beginning of the extras_args list.  Loop through the list
            # generating the has_value string.
            opt_arg = self.options[opts].get(
                VALUE_NAME,
                self.options[opts].get(DEFAULT_NAME, opts))
            opt_value = self.options[opts].get(VALUE_USAGE, IGNORED)
            opt_label = self.options[opts].get(VALUE_LABEL, opt_arg)
            extra_args = self.options[opts].get(EXTRA_VALUES, [])
            extra_args.insert(0, {VALUE_NAME:opt_arg,
                                  VALUE_USAGE:opt_value,
                                  VALUE_LABEL:opt_label})
            has_value = ""
            for opt_arg in extra_args:
                arg = string.upper(opt_arg.get(VALUE_LABEL,
                                               opt_arg.get(VALUE_NAME, "")))
                value = opt_arg.get(VALUE_USAGE, IGNORED)
                                  
                if value == REQUIRED:
                    has_value = has_value + "=" + arg + " "
                elif value == OPTIONAL:
                    has_value = has_value + "[=" + arg + "] "

            #If option has a short argument equivalent.
            if self.options[opts].get(SHORT_OPTION, None):
                option_names = "   -%s, --%-20s" % \
                               (self.options[opts][SHORT_OPTION],
                                opts + has_value)
            #If option does not have a short argument equivalent.
            else:
                option_names = "       --%-20s" % (opts + has_value,)

            #Get and calculate various variables needed to format the output.
            # help_string - shorter than accessing the dictionary
            # num_of_cols - width of the terminal
            # COMM_COLS - length of option_names (aka "       --%-20s")
            help_string = self.options[opts].get(HELP_STRING, "")
            num_of_cols = 80 #Assume this until python 2.1
            COMM_COLS = 29

            #If the command listing takes up over half of the width, just
            # start with the description on the next line.  The lenght of
            # option_names idealy will be less than 29 (9 + 20).  Notice
            # that (80 - 50) is (29 + 1).
            if len(option_names) < (num_of_cols / 2):
                help_string_length = num_of_cols - len(option_names) - 1
                filler_string_length = len(option_names)
            else:
                help_string_length = 50
                filler_string_length = COMM_COLS

            #Calculate the ending index for printing the help string.
            if len(option_names) > (num_of_cols / 2): #Start on next line.
                index = 0
            elif len(help_string) <= help_string_length: #Entire string fits.
                index = len(help_string)
            else: #Start breaking up the string.
                index = string.rfind(help_string, " ", 0, help_string_length)

            #Print the first line of the help line for the command.
            print "%s %s" % (option_names,
                             help_string[0:index])

            #If the help string is long, finish printing it.
            while index < len(help_string) :
                if len(help_string[index:]) < help_string_length:
                    new_index = len(help_string)
                else:
                    new_index = string.rfind(help_string, " ", index,
                                             index + help_string_length)
                print " " * filler_string_length,
                print string.strip(help_string[index:new_index])
                index = new_index
                
        sys.exit(0)

    def print_usage(self, message=None):
        if message:
            print message
        
        print "USAGE:", sys.argv[0], "[",
        print "-" + self.getopt_short_options(),

        list = self.options.keys()
        list.sort()
        for key in list:

            #Deterimine if the option needs an "=" or "[=]" after it.
            has_value = self.options[key].get(VALUE_USAGE, IGNORED)
            if has_value == REQUIRED:
                has_value = "="
            elif has_value == OPTIONAL:
                has_value = "[=]"
            else:
                has_value = ""
                
            print "--" + key + has_value,
        print "]"
        sys.exit(0)

############################################################################

    #This function returns the tuple containing the valid dictionaries used
    # in compile_options_dict().  Simply overload this function to
    # correctly set the valid option groups.
    def valid_dictionaries(self):
        return (self.help_options, self.test_options)

    #Compiles the dictionary groups into one massive dictionary named options.
    def compile_options_dict(self, *dictionaries):
        for i in range(0, len(dictionaries)):
            if type(dictionaries[i]) != type({}):
                raise TypeError, "Dictionary required, not %s." % \
                      type(dictionaries[i])
            for key in dictionaries[i].keys():
                if not self.options.has_key(key):
                    self.options[key] = dictionaries[i][key]

    #Verifies that the options used are in the list of options.  This is to
    # help cut down on the different combinations of spellings.
    def check_option_names(self):
        for opt in self.options.keys():
            if opt not in valid_option_list:
                msg = "Developer error.  Option '%s' not in valid option list."
                print msg % (opt,)
                sys.exit(1)

############################################################################

    #Goes through the compiled option dictionary looking for short options
    # to format in the getopt.getopt() format.
    def getopt_short_options(self):
        temp = ""
        for opt in self.options.keys():
            short_opt = self.options[opt].get(SHORT_OPTION, None)
            if short_opt:
                temp = temp + short_opt
                
                if self.options[opt].get(VALUE_USAGE, None) in [REQUIRED]:
                    temp = temp + "="
                
        return temp

    #Goes through the compiled option dictionary pulling out long options
    # to format in the getopt.getopt() format.
    def getopt_long_options(self):
        temp = []
        for opt in self.options.keys():
            if self.options[opt].get(VALUE_USAGE, None) in [REQUIRED]:
                temp.append(opt + "=")
            else:
                temp.append(opt)
                
        return temp

############################################################################

    #Parse the command line.
    def parse_options(self):

        long_opts = self.getopt_long_options()
        short_opts = self.getopt_short_options()

        #If an argument uses an = for a value seperate it into two entries.
        self.split_on_equals(self.argv)
        argv = self.argv[1:]

        #There is a major problem with this method. Multiple entries on the
        # command line of the same command are not parsed properly.
        try:
            optlist, argv = getopt.getopt(argv, short_opts, long_opts)
        except getopt.GetoptError, detail:
            self.print_usage(detail.msg)

        #copy in this way, to keep self.args out of a dir() listing.
        for arg in argv:
            self.args.append(arg)

        for arg in optlist:
            opt = arg[0]
            value = arg[1]

            if self.is_admin_option(opt) and self.user_level == USER:
                self.print_usage("option %s is an administrator option" %
                                 (opt,))

            if self.is_long_option(opt):
                #Option is a long option.  This means that the option is
                # preceded by two dashes and can be any length.
                self.long_option(opt[2:], value)

            elif self.is_short_option(opt):
                #Option is a short option.  This means it is only
                # one letter long and has one dash at the beginning
                # of the option group.
                self.short_option(opt[1:], value)

############################################################################

    #The option is a long option with value possible VALUE_USAGE.  Determine
    # if the option has been used in the correct manner.  If so, set the
    # value accordingly, if not print an error message.
    def long_option(self, long_opt, value):

        self.option_list.append(long_opt) #Rember this order.

        if self.options[long_opt].get(VALUE_USAGE, None) == REQUIRED:
            #First, determine if the option, which has been determined to
            # require a sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)
            else:
                self.print_usage()
                
        elif self.options[long_opt].get(VALUE_USAGE, None) == OPTIONAL:
            next = self.next_argument(long_opt) #Used for optional.

            #First, determine if the option, which may or may not have a 
            # sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)
                
            #If the option has an optional value and it is present then
            # find the value (albeit the hard way), set the value and then
            # remove the value from the list of previously unprocessed
            # arguments (self.args).
            elif next != None and not self.is_option(next):
                self.set_value(long_opt, next)
                self.args.remove(next)
            else:
                self.set_value(long_opt, None) #Uses 'default'
                
        else: #INGORED
            self.set_value(long_opt, None) #Uses 'default'

    #Do the same thing with the short options that is done with the long
    # options.  For all intensive purposes, this gets the long opt that
    # is the short option equivalet and uses the long opt.
    def short_option(self, short_opt, value):
        long_opt = self.short_to_long(short_opt)

        self.option_list.append(long_opt) #Rember this order.

        if self.options[long_opt].get(VALUE_USAGE, None) == REQUIRED:
            #First, determine if the option, which has been determined to
            # require a sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)
            else:
                self.print_usage()

        elif self.options[long_opt].get(VALUE_USAGE, None) == OPTIONAL:
            #First, determine if the option, which may or may not have a 
            # sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)
                
            #If the option has an optional value and it is present then
            # find the value (albeit the hard way), set the value and then
            # remove the value from the list of previously unprocessed
            # arguments (self.args).
            elif self.next_argument(short_opt) != None and \
                 not self.is_option(self.next_argument(short_opt)):
                next = self.next_argument(short_opt)
                self.set_value(long_opt, next)
                self.args.remove(next)
            else:
                self.set_value(long_opt, None) #Uses 'default'
                
        else: #INGORED
            self.set_value(long_opt, None) #Uses 'default'

############################################################################

    #Options can be entered like "--option value" or "--option=value".
    # Parse the argv passed in and split the "=" values into spaced value.
    def split_on_equals(self, argv):
        for i in range(len(argv)):
            list = string.split(argv[i], "=")
            argv[i:i + 1] = list

    #Take the passed in short option and return its long option equivalent.
    def short_to_long(self, short_opt):
        if not self.is_short_option(short_opt):
            return None
        for key in self.options.keys():
            if self.trim_short_option(short_opt) == \
               self.options[key].get(SHORT_OPTION, None):
                return key #in other words return the long opt.
        return None

    some_args = sys.argv[1:]
    #Return the next argument in the argument list after the one specified
    # as argument.  If one does not exist, return None.
    #some_args is used to avoid problems with duplicate arguments on the
    # command line.
    def next_argument(self, argument):
        #Get the next option after the option passed in.

        for arg in self.some_args:
            if string.find(arg, argument) != -1:
                index = self.some_args.index(arg)
                if index == len(self.some_args[1:]): #Nothing can follow.
                    return None
                rtn = self.some_args[index + 1]

                self.some_args = self.some_args[index + 1:]

                return rtn

        return None
            
############################################################################
    #These options remove leading "-" or "--" as appropriate from opt
    # and return.

    def trim_option(self, opt):
        if self.is_long_option(opt):
            return self.trim_long_option(opt)
        elif self.is_short_option(opt):
            return self.trim_short_option(opt)
        else:
            return opt
        
    def trim_long_option(self, opt):
        if opt[:2] == "--" and (opt[2] in string.letters or
                                opt[2] in string.digits):
            return opt[2:]
        else:
            return opt
            
    def trim_short_option(self, opt):
        if opt[0] == "-" and (opt[1] in string.letters or
                              opt[1] in string.digits):
            return opt[1:]
        else:
            return opt

############################################################################
    #These options return 1 if opt is the correct type of option,
    # otherwise it return 0.

    def is_option(self, opt):
        return self.is_long_option(opt) or self.is_short_option(opt)

    def is_long_option(self, opt):
        opt_check = self.trim_long_option(opt)
        try:

            return opt_check in self.options.keys()
        except TypeError:
            return 0
    
    def is_short_option(self, opt):
        opt_check = self.trim_short_option(opt)
        try:
            for key in self.options.keys():
                if self.options[key].get(SHORT_OPTION, None) == opt_check:
                    return 1
            return 0
        except TypeError:
            return 0

############################################################################
    #Returns whether the option is a user or admin command.  A return
    # value of one means it is that type, 0 otherwise.

    def is_user_option(self, opt):
        if self.is_long_option(opt):
            if self.options[self.trim_option(opt)].get(
                USER_LEVEL, USER) == USER:
                return 1
        elif self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
            if self.options[self.trim_option(long_opt)].get(
                USER_LEVEL, USER) == USER:
                return 1
        return 0
        
    def is_admin_option(self, opt):
        if self.is_long_option(opt):
            if self.options[self.trim_option(opt)].get(
                USER_LEVEL, USER) == ADMIN:
                return 1
        elif self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
            if self.options[self.trim_option(long_opt)].get(
                USER_LEVEL, USER) == ADMIN:
                return 1
        return 0

############################################################################
    #These option return the correct value from the options dictionary
    # for a given long option long_opt and its dictionary opt_dict.  If
    # the dictionary doesn't have that particular value at hand, then
    # correctly determines the default value.

    def get_value_name(self, opt_dict, long_opt):
        # Determine what the variable's name is.  Use the command string
        # as the default if a "value_name" field is not specified.
        opt_name = opt_dict.get(VALUE_NAME, long_opt)
        
        #Convert command dashes to variable name underscores.
        opt_name = string.replace(opt_name, "-", "_") 
        
        return opt_name

    def get_default_name(self, opt_dict, long_opt):
        # Determine what the default's name is.  Use the command string
        # as the default if a "default_name" field is not specified.
        opt_name = opt_dict.get(DEFAULT_NAME, long_opt)
        
        #Convert command dashes to variable name underscores.
        opt_name = string.replace(opt_name, "-", "_") 
        
        return opt_name

    def get_default_value(self, opt_dict, value):
        #Return the DEFAULT_VALUE for an option that takes no value.
        if opt_dict.get(VALUE_USAGE, IGNORED) == IGNORED:
            return opt_dict.get(DEFAULT_VALUE, 1)
        #Return the DEFAULT_VALUE for an option that takes an optional value.
        elif opt_dict.get(VALUE_USAGE, IGNORED) == OPTIONAL:
            if value == None and opt_dict.get(DEFAULT_VALUE, None):
                return opt_dict[DEFAULT_VALUE]
            elif value == None and opt_dict.get(FORCE_SET_DEFAULT, None):
                return opt_dict.get(DEFAULT_VALUE, 1)
            else:
                return value
        #Return the DEFAULT_VALUE for an option that must take a value.
        # Usually this will be set to the value passed in, unless
        # FORCE_SET_DEFAULT forces the setting of both values.
        else: #REQUIRED
            if opt_dict.get(FORCE_SET_DEFAULT, None):
                return opt_dict.get(DEFAULT_VALUE, 1)
            else:
                return value
    
    def get_value_type(self, opt_dict, value):
        if opt_dict.get(VALUE_TYPE, STRING) == INTEGER:
            return int(value)
        elif opt_dict.get(VALUE_TYPE, STRING) == FLOAT:
            return float(value)
        else: #STRING
            return str(value)

    def get_default_type(self, opt_dict, value):
        if opt_dict.get(DEFAULT_TYPE, STRING) == INTEGER:
            return int(value)
        elif opt_dict.get(DEFAULT_TYPE, STRING) == FLOAT:
            return float(value)
        else: #STRING
            return str(value)

############################################################################
    #Thse options set the values in the interface class.  set_value() is
    # the function that calls the others.  set_from_dcitionary() takes the
    # specific dictionary (which is important when multiple arguments for
    # a single option exist) and sets the interface variables.  The last
    # function, set_extra_values(), handles when more than one argument
    # is parsed for an option.
       
    def set_value(self, long_opt, value):
        #Make sure the name gets put inside if it isn't there already.
        if not self.options[long_opt].get(VALUE_NAME, None):
            self.options[long_opt][VALUE_NAME] = long_opt

        #Pass in the long option dictionary, long option and its value to
        # use them to set this interfaces variable.
        self.set_from_dictionary(self.options[long_opt], long_opt, value)

        #Some options may require more than one value.
        self.set_extra_values(long_opt, value)

    def set_from_dictionary(self, opt_dict, long_opt, value):
        if value:
            #Get the name to set.
            opt_name = self.get_value_name(opt_dict, long_opt)

            #Get the value in the correct type to set.
            try:
                #opt_value = self.get_value_value(opt_dict, value)
                opt_typed_value = self.get_value_type(opt_dict, value)
            except ValueError, detail:
                msg = "option %s requires type %s" % \
                      (long_opt,
                       opt_dict.get(VALUE_TYPE, STRING))
                self.print_usage(msg)

            setattr(self, opt_name, opt_typed_value)
            
        if not value or opt_dict.get(FORCE_SET_DEFAULT, None):
            #Get the name to set.
            opt_name = self.get_default_name(opt_dict, long_opt)

            #Get the value in the correct type to set.
            try:
                opt_value = self.get_default_value(opt_dict, value)
                opt_typed_value = self.get_default_type(opt_dict, opt_value)
            except ValueError, detail:
                msg = "option %s requires type %s" % \
                      (long_opt,
                       opt_dict.get(DEFAULT_TYPE, STRING))
                self.print_usage(msg)

            setattr(self, opt_name, opt_typed_value)

    def set_extra_values(self, opt, value):
        if self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
        else:
            long_opt = opt

        extras = self.options[self.trim_option(long_opt)].get(
            EXTRA_VALUES, [])

        next = None
        
        for extra_option in extras:
            if next:
                next = self.next_argument(next)
            elif value:
                next = self.next_argument(value)
            else:
                next = self.next_argument(opt)

            self.set_from_dictionary(extra_option, long_opt, next)

############################################################################
    
if __name__ == '__main__':
    intf = Interface()

    for arg in dir(intf):
        if string.replace(arg, "_", "-") in intf.options.keys():
            print arg, type(getattr(intf, arg)), ": ",
            pprint.pprint(getattr(intf, arg))

    if intf.args:
        print "unprocessed args:", intf.args

    if getattr(intf, "help", None):
        intf.print_help()
    if getattr(intf, "usage", None):
        intf.print_usage()


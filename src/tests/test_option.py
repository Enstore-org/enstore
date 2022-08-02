""" Basic unit tests for option.Interface
    Dennis Box, dbox@fnal.gov
"""

import StringIO
import mock
import unittest
import option
import os
import sys
import string
import pprint
import getopt
import types
import hostaddr
import Trace
import e_errors
import enstore_constants
import enstore_functions2


###### some dictionaries from the doc string #######
###########  use for testing #####################

TD0  = { 'opt':{option.HELP_STRING:"some string text"}}
TD1  = {
       'opt':{option.HELP_STRING:"some string text"},
              option.DEFAULT_NAME:'opt',
              option.DEFAULT_VALUE:1,
              option.DEFAULT_TYPE:option.STRING,
              option.VALUE_USAGE:option.IGNORED,
              option.USER_LEVEL:option.USER,
              option.FORCE_SET_DEFAULT:0,
              option.EXTRA_VALUES:[]
        }
TD2 = {
       'opt':{option.HELP_STRING:"some string text"},
              option.DEFAULT_NAME:'opt',
              option.DEFAULT_VALUE:option.DEFAULT,
              option.DEFAULT_TYPE:option.INTEGER,
              option.VALUE_NAME:'filename',
              option.VALUE_TYPE:option.STRING,
              option.VALUE_USAGE:option.REQUIRED,
              option.VALUE_LABEL:"filename",
              option.USER_LEVEL:option.USER,
              option.FORCE_SET_DEFAULT:option.FORCE,
              option.EXTRA_VALUES:[]
        }

TD3  = {
        'opt':{option.HELP_STRING:"some string text"},
              option.DEFAULT_NAME:'opt',
              option.DEFAULT_VALUE:1,
              option.DEFAULT_TYPE:option.INTEGER,
              option.VALUE_NAME:'filename',
              option.VALUE_TYPE:option.STRING,
              option.VALUE_USAGE:option.REQUIRED,
              option.VALUE_LABEL:"filename",
              option.USER_LEVEL:option.USER,
              option.FORCE_SET_DEFAULT:1,
              option.EXTRA_VALUES:[{option.DEFAULT_NAME:"filename2",
                             option.DEFAULT_VALUE:"",
                             option.DEFAULT_TYPE:option.STRING,
                             option.VALUE_NAME:"filename2",
                             option.VALUE_TYPE:option.STRING,
                             option.VALUE_USAGE:option.OPTIONAL,
                             option.VALUE_LABEL:"filename2"
                               }]
        }

TD4  = {
        'opt':{option.HELP_STRING:"some string text"},
               option.DEFAULT_TYPE:option.INTEGER,
               option.DEFAULT_VALUE:1,
               option.VALUE_USAGE:option.IGNORED,
               option.USER_LEVEL:option.ADMIN,
               #This will set an addition value.  It is weird
               # that DEFAULT_TYPE is used with VALUE_NAME,
               # but that is what will make it work.
               option.EXTRA_VALUES:[{option.DEFAULT_VALUE:0,
                                     option.DEFAULT_TYPE:option.INTEGER,
                                     option.VALUE_NAME:option.PRIORITY,
                                     option.VALUE_USAGE:option.IGNORED,
                                     }]
        }

TYPE_OPTION_LIST = [  option.INTEGER, option.LONG, 
                      option.FLOAT, option.RANGE, 
                      option.STRING, option.LIST ]

TYPES_LIST = [ type(0), type(0L), type(0.0), type(range(3)), type(''), type([])]

DICT_LIST = [ TD0, TD1, TD2, TD3, TD4 ]

########### end of global test data ##############################


@mock.patch('Trace.log')
def test_log_using_default(mock_trace):
    option.log_using_default('foo','bar')
    msg = "foo not set in environment or command line - reverting to bar"
    mock_trace.assert_called_with(e_errors.INFO, msg)


@mock.patch('Trace.log')
def test_check_for_config_defaults(mock_trace):
    option.check_for_config_defaults()
    mock_trace.assert_called_with('ENSTORE_CONFIG_HOST', enstore_constants.DEFAULT_CONF_HOST)
    mock_trace.assert_called_with('ENSTORE_CONFIG_PORT', enstore_constants.DEFAULT_CONF_PORT)

def test_list2():
    l = option.list2('foo')
    assertEqual(l, ['foo'])

class TestInterface(unittest.TestCase):

    def setUp(self):
        self.intf = option.Interface()
        
    @mock.patch("sys.stdout", new_callable=StringIO.StringIO )
    def test_contents(self, stdout_mock):

        #print the options value
        for arg in dir(self.intf):
            if string.replace(arg, "_", "-") in self.intf.options.keys():
                stdout_mock.write( "%s %s :  %s \n " %\
                (arg, type(getattr(self.intf, arg)), getattr(self.intf, arg)))
    
        #every other matched value
        for arg in dir(self.intf):
            if string.replace(arg, "_", "-") not in self.intf.options.keys():
                stdout_mock.write( "%s %s :  %s \n " %\
                (arg, type(getattr(self.intf, arg)), getattr(self.intf, arg)))

        self.assertNotEqual(stdout_mock.getvalue(), "")

    def test_check_host(self):
        self.intf.check_host('localhost')
        self.assertEqual('127.0.0.1', self.intf.config_host)

    def test___init__(self):
        self.assertTrue(isinstance(self.intf, option.Interface))


    @mock.patch("sys.exit")
    @mock.patch("sys.stdout")
    def test_print_help(self,stdout_mock,exit_mock):
        self.intf.print_help()
        exit_mock.assert_called_with(0)

    def test_format_parameters(self):
        self.intf.parameters = [ '--foo', '<bar>']
        fprm = self.intf.format_parameters()
        self.assertEqual(' --foo <bar>', fprm)

    @mock.patch("sys.exit")
    @mock.patch("sys.stdout")
    def test_print_usage(self,stdout_mock,exit_mock):
        self.intf.print_usage()
        exit_mock.assert_called_with(0)

    @mock.patch("sys.stderr", new_callable=StringIO.StringIO)
    def test_missing_parameter(self, stderr_mock):
        self.intf.missing_parameter("foo")
        self.assertEqual(stderr_mock.getvalue(), "ERROR: missing parameter foo\n")

    def test_valid_dictionaries(self):
        vd = self.intf.valid_dictionaries()
        self.assertEqual(vd[0], self.intf.help_options)
        self.assertEqual(vd[1], self.intf.test_options)

    def test_compile_options_dict(self):
        vd = self.intf.valid_dictionaries()
        self.intf.help_options['tags'] = {"argle":"bargle"} 
        self.intf.compile_options_dict(vd[0],vd[1])
        self.assertTrue( 'tags' in self.intf.options )

    @mock.patch('sys.exit')
    def test_check_option_names(self,exit_mock):
        self.intf.help_options['tags'] = {"argle":"bargle"}
        self.intf.check_option_names()
        exit_mock.assert_not_called()
        opts = self.intf.options
        keys_mock = mock.MagicMock()
        keys_mock.keys.return_value = ['deliberately_wrong']
        self.intf.options = keys_mock
        expected_err  = "Developer error.  Option "
        expected_err  += "'deliberately_wrong' not in valid option list.\n"
        with mock.patch('sys.stderr', new_callable=StringIO.StringIO) as stderr_mock:
	    self.intf.check_option_names()
            exit_mock.assert_called_with(1)
            self.assertEqual(stderr_mock.getvalue(), expected_err)
        self.intf.options = opts
 
    @mock.patch('sys.stderr', new_callable=StringIO.StringIO)
    def test_check_correct_count(self, stderr_mock):
        self.intf.args.append('lalala')
        expected_err = '2 extra arguments specified: lalala\n'
        self.intf.check_correct_count(0)
        self.assertTrue('lalala' in stderr_mock.getvalue())

    def test_getopt_short_options(self):
        shopt = self.intf.getopt_short_options()
        expected = 'th' # -t for test, -h for help
        self.assertEqual(shopt, expected)

    def test_getopt_long_options(self):
        lopt = self.intf.getopt_long_options()
        expected = ['usage', 'test', 'opt=', 'help', 'tags']
        self.assertEqual(lopt, expected)

    def test_short_to_long(self):
        lng = self.intf.short_to_long('h')
        self.assertEqual(lng, 'help')

    # most of this method cannot be reached!!!
    def test_next_argument(self):
        n = self.intf.next_argument('help')
        self.assertEqual(n,None)
        self.intf.some_args = [ 1, 2, 3, 4]
        n = self.intf.next_argument('help')
        self.assertNotEqual(n,None)

    def test_trim_option(self):
        out = self.intf.trim_option('h')
        self.assertEqual(out,'h')

    def test_option_stuff(self):
        for arg in dir(option):
            try:
                ename = "option.%s" % arg
                epart = eval(ename)
                etype=type(epart)
                if etype == type(''):
                    self.intf.is_option(epart)
                    self.intf.is_long_option(epart)
                    self.intf.is_short_option(epart)
                    self.intf.is_switch_option(epart)
                    self.intf.is_user_option(epart)
                    self.intf.is_user2_option(epart)
                    self.intf.is_admin_option(epart)
                    self.intf.is_hidden_option(epart)

            except:
                msg = sys.exc_info()[1]
                print "test_option_stuff exception: %s " % msg
                print "test_option_stuff more info:"
                ename="option.%s"%arg
                epart = eval(ename)
                print "exception on %s evaluated to %s " % (ename,epart) 
                print "test_option_stuff end of info:"




    def test_get_value_name(self):
        pass
    def test_get_default_name(self):
        pass
    def test_get_default_value(self):
        pass

    def test_get_value_type(self):
        for d in DICT_LIST:
            t = self.intf.get_value_type(d)
            self.assertTrue(t in TYPES_LIST,'t=%s TYPES_LIST=%s'%(t,TYPES_LIST))


    def test_get_default_type(self):
        for d in DICT_LIST:
            t = self.intf.get_default_type(d)
            self.assertTrue(t in TYPES_LIST,'t=%s TYPES_LIST=%s'%(t,TYPES_LIST))

    def test_set_value(self):
        self.intf.print_usage = mock.MagicMock()
        for arg in dir(option):
            try:
                ename = "option.%s" % arg
                epart = eval(ename)
                etype=type(epart)
                if etype == type(''):
                    for opt_dict in DICT_LIST:
                        self.intf.options[epart] = opt_dict['opt']
                        # test that it can be set to a string
                        self.intf.set_value(epart,epart)
                        # now test to see if it can be set without a value
                        self.intf.set_value(epart,None)
                        if self.intf.print_usage.call_count > 0:
                            # it cant be set without value, retrieve error msg 
                            self.assertEqual(self.intf.print_usage.call_count, 1)
                            msg=str(self.intf.print_usage.call_args)
                            self.assertTrue('requires a value' in msg, msg)
                            self.intf.print_usage.reset_mock()

            except:
                msg = sys.exc_info()[1]
                print "test_set_value exception: %s " % msg
                print "test_set_value more info:"
                ename="option.%s"%arg
                epart = eval(ename)
                print "exception on %s evaluated to %s " % (ename,epart) 
                print "test_set_value end of info:"




    def test_set_from_dictionary(self):
        self.intf.print_usage = mock.MagicMock()
        for arg in dir(option):
            try:
                ename = "option.%s" % arg
                epart = eval(ename)
                etype=type(epart)
                if etype == type(''):
                    for opt_dict in DICT_LIST:
                        # test that it can be set to a string
                        self.intf.set_from_dictionary(opt_dict,epart,epart)
                        # now test to see if it can be set without a value
                        self.intf.set_from_dictionary(opt_dict,epart,None)
                        if self.intf.print_usage.call_count > 0:
                            # it cant be set without value, retrieve error msg 
                            self.assertEqual(self.intf.print_usage.call_count, 1)
                            msg=str(self.intf.print_usage.call_args)
                            self.assertTrue('requires a value' in msg, msg)
                            self.intf.print_usage.reset_mock()

            except:
                msg = sys.exc_info()[1]
                print "test_set_from_dictionary exception: %s " % msg
                print "test_set_from_dictionary more info:"
                ename="option.%s"%arg
                epart = eval(ename)
                print "exception on %s evaluated to %s " % (ename,epart) 
                print "test_set_from_dictionary end of info:"


if __name__ == "__main__":
    unittest.main()

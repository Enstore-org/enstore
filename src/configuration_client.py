#!/usr/bin/env python
"""
Configuration server client.
"""

# system imports
import sys
import errno
import pprint
import os
import socket
import select
import types
import time
import imp
import getpass

# enstore imports
import generic_client
import enstore_constants
import enstore_functions2
import option
import Trace
import callback
import e_errors
import hostaddr

MY_NAME = enstore_constants.CONFIGURATION_CLIENT  # "CONFIG_CLIENT"
MY_SERVER = enstore_constants.CONFIGURATION_SERVER


class ConfigFlag:
    MSG_YES = 1
    MSG_NO = 0
    DISABLE = 1
    ENABLE = 0

    def __init__(self):
        self.new_config_file = self.MSG_NO
        self.do_caching = self.DISABLE

    def is_caching_enabled(self):
        """bool: Whether or not caching is enabled"""
        return self.do_caching == self.ENABLE

    def new_config_msg(self):
        """Set client to expect new config file"""
        self.new_config_file = self.MSG_YES

    def reset_new_config(self):
        """Set cilent not to expect new config file"""
        self.new_config_file = self.MSG_NO

    def have_new_config(self):
        """bool (0 or 1): Whether or not there is a new config to process

        I'm not sure why this returns early if caching is disabled
        """
        if self.do_caching == self.DISABLE:
            return 1
        elif self.new_config_file == self.MSG_YES:
            return 1
        else:
            return 0

    def disable_caching(self):
        self.do_caching = self.DISABLE

    def enable_caching(self):
        self.do_caching = self.ENABLE


class ConfigurationClient(generic_client.GenericClient):
    """
    Configuration client
    """

    def __init__(self, address=None):
        """
        :type address: :obj:`tuple`
        :arg address: (:obj:`str` - host, :obj:`int` - port) - configuration server address
        """
        if address is None:
            address = (enstore_functions2.default_host(),
                       enstore_functions2.default_port())
        flags = enstore_constants.NO_CSC | enstore_constants.NO_ALARM | \
            enstore_constants.NO_LOG
        generic_client.GenericClient.__init__(self, (), MY_NAME,
                                              address, flags=flags,
                                              server_name=MY_SERVER)
        self.new_config_obj = ConfigFlag()
        self.saved_dict = {}
        self.have_complete_config = 0
        self.config_load_timestamp = None

        # Values provided to 'get' are cached on the class, for some reason.
        # I'm not actually sure these are used.
        self.retry = generic_client.DEFAULT_TRIES
        self.timeout = generic_client.DEFAULT_TIMEOUT

    # Return these values when requested.
    def get_address(self):
        return self.server_address

    def get_timeout(self):
        return self.timeout

    def get_retry(self):
        return self.retry

    def is_config_current(self):
        """Needed by clients that use dump_and_save(). Determines
        if the cached configuration is the current configuration loaded into
        the configuration_server.

        Servers that use dump_and_save() also use
        have_new_config() to determine this same information in
        response to event_relay NEWCONFIGFILE messages.

        returns:
          bool: Whether the currently cached config is the same as what
          is loaded in the configuration server.
        """
        if self.config_load_timestamp is None:
            return False

        result = self.config_load_time(5, 5)
        if e_errors.is_ok(result):
            ##############################################################
            # This check added because a traceback was found 1-12-2009
            # on STKen where config_load_timestamp was not in result.
            if 'config_load_timestamp' not in result.keys():
                Trace.log(e_errors.ERROR,
                          "ticket missing config_load_timestamp is: %s" %
                          (result,))
            ##############################################################

            if result['config_load_timestamp'] <= self.config_load_timestamp:
                return True

        return False

    def get_enstore_system(self, timeout=0, retry=0):
        """Return which key in the 'known_config_servers' configuration dict
        entry refers to this client's server (if present).  If there is
        not an entry (like a developers test system) then a value is returned
        based on the configuration server's node name.

        Returns:
            string: Config server key ID from Enstore configuration or first
            element of node name based on this server's initialization.
        """

        while 1:
            ret = self.get('known_config_servers', timeout, retry)

            if e_errors.is_ok(ret):
                break
            else:
                # Return None if no response from the configuration
                # server was received.
                return None

        for item in ret.items():
            if socket.getfqdn(item[1][0]) == \
                    socket.getfqdn(self.server_address[0]):
                return item[0]

        # If we make it here, then we  received a response from the
        # configuration server, but we did not find the system this
        # is looking for in the list received.
        return socket.getfqdn(self.server_address[0]).split(".")[0]

    def do_lookup(self, key, timeout, retry):
        """
        Lookup configuration item.

        :type key: :obj:`str`
        :arg key: item in question
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: value or error
        """

        request = {'work': 'lookup', 'lookup': key, 'new': 1}
        ret = self.send(request, timeout, retry)
        if e_errors.is_ok(ret):
            try:
                # New format.  This is requested by new configuration clients
                # by adding the "'new' : 1" to the request ticket above.
                self.saved_dict[key] = ret[key]
                ret_val = ret[key]
            except KeyError:
                # Old format.
                self.saved_dict[key] = ret
                ret_val = ret
            # Trace.trace(23, "Get %s config info from server"%(key,))
        else:
            ret_val = ret

        # Keep the hostaddr allow() information up-to-date on all lookups.
        hostaddr.update_domains(ret.get('domains', {}))

        return ret_val

    def get(self, key, timeout=0, retry=0):
        """
        Return value for requested item.

        :type key: :obj:`str`
        :arg key: item in question
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """

        # Not sure why these are "remembered" specifically from here. They are
        # not referenced AFAICT except in get_timeout and get_retry methods.
        self.timeout = timeout  # Remember this.
        self.retry = retry  # Remember this.
        if key == enstore_constants.CONFIGURATION_SERVER:
            # Go ahead and return our server's information if config
            # server's information is requested.
            ret = {'hostip': self.server_address[0],
                   'port': self.server_address[1],
                   'status': (e_errors.OK, None)}
        else:
            # if we have a new_config_obj, then only go to the config server if we
            # have received a message saying a new one was loaded.
            if not self.new_config_obj or self.new_config_obj.have_new_config():
                # clear out the cached copies
                self.saved_dict = {}
                # The config cache was just clobbered.
                self.have_complete_config = 0
                self.config_load_timestamp = None
                ret = self.do_lookup(key, timeout, retry)
                if self.new_config_obj:
                    self.new_config_obj.reset_new_config()
            else:
                # there was no new config loaded, just return what we have.
                # if we do not have a stashed copy, go get it.
                if key in self.saved_dict.keys():
                    # Trace.trace(23, "Returning %s config info from saved_dict"%(key,))
                    # Trace.trace(23, "saved_dict - %s"%(self.saved_dict,))
                    ret = self.saved_dict[key]
                else:
                    ret = self.do_lookup(key, timeout, retry)

        # HACK:
        # Do a hack for the monitor server. Since it runs on all enstore
        # machines, we need to add this information before continuing.
        # i.a. we just set values to localhost.
        # Not sure why we exit early for config server but not monitor server.
        if e_errors.is_ok(ret) and key == enstore_constants.MONITOR_SERVER:
            ret['host'] = socket.gethostname()
            # ret['hostip'] = socket.gethostbyname(ret['host'])
            ret['hostip'] = hostaddr.name_to_address(ret['host'])
            ret['port'] = enstore_constants.MONITOR_PORT
        # END HACK.

        # if ret['status'][0] == e_errors.KEYERROR:
        #    import traceback
        #    Trace.log(e_errors.INFO, "Key %s requested from:" % (key,))
        #    # log it
        #    for l in traceback.format_stack():
        #        Trace.log(e_errors.INFO, l)

        return ret

    def dump(self, timeout=0, retry=0):
        """
        Return the configuration dictionary (use active protocol).
        This function does not produce any logging or output.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """
        ticket = {"work": "dump2"}
        done_ticket = self.send(ticket, rcv_timeout=timeout,
                                tries=retry)

        # Try old way if the server is old too.
        if done_ticket['status'][0] == e_errors.KEYERROR and \
                done_ticket['status'][1].startswith("cannot find requested function"):
            done_ticket = self.dump_old(timeout, retry)
            return done_ticket  # Avoid duplicate "convert to external format"
        if not e_errors.is_ok(done_ticket):
            return done_ticket
        hostaddr.update_domains(done_ticket.get("domains", {}))
        return done_ticket

    def dump_old(self, timeout=0, retry=0):
        """
        Return the configuration dictionary.
        This function does not produce any logging or output.
        This uses a deprecated `dump` function which likely no longer exists in
        production.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """

        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)

        request = {'work': 'dump',
                   'callback_addr': (host, port)}
        reply = self.send(request, timeout, retry)
        if not e_errors.is_ok(reply):
            # print "ERROR",reply
            return reply
        r, w, x = select.select([listen_socket], [], [], 15)
        if not r:
            reply = {'status': (e_errors.TIMEDOUT,
                                "timeout waiting for configuration server callback")}
            return reply
        control_socket, address = listen_socket.accept()
        hostaddr.update_domains(reply.get('domains', {}))  # Hackish.
        if not hostaddr.allow(address):
            listen_socket.close()
            control_socket.close()
            reply = {'status': (e_errors.EPROTO,
                                "address %s not allowed" % (address,))}
            return reply

        try:
            d = callback.read_tcp_obj(control_socket)
        except e_errors.EnstoreError, msg:
            d = {'status': (msg.type, str(msg))}
        except e_errors.TCP_EXCEPTION:
            d = {'status': (e_errors.TCP_EXCEPTION, e_errors.TCP_EXCEPTION)}
        listen_socket.close()
        control_socket.close()
        return d

    def dump_and_save(self, timeout=0, retry=0):
        """
        Return the configuration dictionary and save it too.
        Simply returns saved config unless there has been notification of
        a new config.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """

        if not self.new_config_obj or self.new_config_obj.have_new_config() \
                or not self.is_config_current():

            config_ticket = self.dump(timeout=timeout, retry=retry)
            if e_errors.is_ok(config_ticket):
                self.saved_dict = config_ticket['dump'].copy()
                self.saved_dict['status'] = (e_errors.OK, None)
                self.have_complete_config = 1
                self.config_load_timestamp = \
                    config_ticket.get('config_load_timestamp', None)
                if self.new_config_obj:
                    self.new_config_obj.reset_new_config()

                return self.saved_dict  # Success.

            return config_ticket  # An error occurred.

        return self.saved_dict  # Used cached dictionary.

    def config_load_time(self, timeout=0, retry=0):
        """
        Get live config timestamp from config server.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """
        request = {'work': 'config_timestamp'}
        x = self.send(request, timeout, retry)
        return x

    # get all keys in the configuration dictionary
    def get_keys(self, timeout=0, retry=0):
        """
        Get all keys in the configuration dictionary.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """
        request = {'work': 'get_keys'}
        keys = self.send(request, timeout, retry)
        return keys

    def load(self, configfile, timeout=0, retry=0):
        """
        Request the config server to load a new config file.

        :type configfile: :obj:`str`
        :arg configfile: configuration file path
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """

        request = {'work': 'load', 'configfile': configfile, 'user': getpass.getuser()}
        x = self.send(request, timeout, retry)
        return x

    def threaded(self, on=0, timeout=0, retry=0):
        """
        Set multithreading to on or off

        :type on: :obj:`int`
        :arg on: whether to set threading on or off
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """
        request = {'work': 'thread_on', 'on': on}
        x = self.send(request, timeout, retry)
        return x

    def copy_level(self, copy_level=2, timeout=0, retry=0):
        """
        Set copy level in configuration server (may affect server performance).

        :type copy_level: :obj:`int`
        :arg copy_level: 2 = deepcopy, 1 = copy, 0 = direct reference.
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """

        request = {'work': 'copy_level', 'copy_level': copy_level}
        x = self.send(request, timeout, retry)
        return x

    def get_movers(self, library_manager, timeout=0, retry=0):
        """
        Get list of the Library manager movers.

        :type library_manager: :obj:`str`
        :arg library_manager: library manager name
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict` configuration server reply
        """

        request = {'work': 'get_movers', 'library': library_manager}
        ret = self.send(request, timeout, retry)
        if e_errors.is_ok(ret):
            result = ret['movers']
        else:
            result = ret
        return result

    def get_movers2(self, library_manager=None, timeout=0, retry=0, conf_dict=None):
        """Get list of the movers for a specific library manager movers with
        full config info. If no library manager is provided, all movers are
        returned.

        :type library_manager: :obj:`str`
        :arg library_manager: (Optional) specific library_manager for which to
                                return movers. Parameter should be the full
                                "name.library_manager" style name.
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :type conf_dict: :obj:`dict`
        :arg conf_dict: (Optional) config dict to search in lieu of getting dict
                          from config server
        :rtype: :obj:`dict` configuration server reply, empty if not OK
        """

        mover_list = []

        if conf_dict is None:
            conf_dict = self.dump_and_save(timeout=timeout, retry=retry)
        if not e_errors.is_ok(conf_dict):
            return mover_list
        for item in conf_dict.items():
            if item[0][-6:] == ".mover":

                # If a library_manager was provided, make sure only
                # movers that use it are returned.
                if library_manager:
                    if isinstance(item[1]['library'], types.StringType):
                        lib_list = [item[1]['library']]
                    elif isinstance(item[1]['library'], types.ListType):
                        lib_list = item[1]['library']
                    else:
                        # Not an expected type, so it will never match.
                        continue
                    for library in lib_list:
                        if library_manager == library:
                            # Found a match for this mover to the
                            # requested library_manager.
                            break
                    else:
                        # No match.
                        continue

                item[1]['name'] = item[0]
                item[1]['mover'] = item[0][:-6]
                mover_list.append(item[1])

        return mover_list

    def get_migrators2(self, timeout=0, retry=0, conf_dict=None):
        """Get list of the migrators with full config info.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :type conf_dict: :obj:`dict`
        :arg conf_dict: (Optional) config dict to search in lieu of getting dict
                          from config server
        :rtype: :obj:`list` configuration server reply, empty if not OK
        """

        migrator_list = []

        if conf_dict is None:
            conf_dict = self.dump_and_save(timeout=timeout, retry=retry)
        if not e_errors.is_ok(conf_dict):
            return migrator_list

        for key, value in conf_dict.items():
            if key[-9:] == ".migrator":
                value['name'] = key
                migrator_list.append(value)
        return migrator_list

    def get_migrators(self, timeout=0, retry=0, conf_dict=None):
        """Get the list of migrators. Unlike get_migrators2, this returns
        only the migrator names, rather than the full config info.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :type conf_dict: :obj:`dict`
        :arg conf_dict: (Optional) config dict to search in lieu of getting dict
                          from config server
        :rtype: :obj:`list` list of migrators
        """

        migrator_list = []
        migrator_list1 = self.get_migrators2(timeout, retry, conf_dict)
        for migrator in migrator_list1:
            migrator_list.append(migrator['name'])
        return migrator_list

    def get_media_changer(self, library_manager, timeout=0, retry=0):
        """Get media changer associated with a library manager.

        :type library_manager: :obj:`str`
        :arg library_manager: library manager
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`str` media changer
         """

        request = {'work': 'get_media_changer',
                   'library': library_manager}
        ret = self.send(request, timeout, retry)

        return ret.get("media_changer", "")

    def get_library_managers(self, timeout=0, retry=0):
        """Get list of library managers. Unlike get_library_managers2, this
        returns only the library manager names, not the full config info.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`list` list of library managers
         """

        request = {'work': 'get_library_managers'}
        ret = self.send(request, timeout, retry)
        if e_errors.is_ok(ret):
            result = ret['library_managers']
        else:
            result = ret
        return result

    def get_library_managers2(self, timeout=0, retry=0, conf_dict=None):
        """Get list of library managers with full config info.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :type conf_dict: :obj:`dict`
        :arg conf_dict: (Optional) config dict to search in lieu of getting dict
                          from config server
        :rtype: :obj:`list` list of library managers
         """

        library_manager_list = []

        if conf_dict is None:
            conf_dict = self.dump_and_save(timeout=timeout, retry=retry)
        if not e_errors.is_ok(conf_dict):
            return library_manager_list

        for item in conf_dict.items():
            if item[0][-16:] == ".library_manager":
                item[1]['name'] = item[0]
                item[1]['library_manager'] = item[0][:-16]
                library_manager_list.append(item[1])

        return library_manager_list

    def get_media_changers(self, timeout=0, retry=0):
        """Get list of media changers. Unlike get_media_changers2, this returns
        only the list of media changer names, not full config info.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`list` list of media changers
         """

        request = {'work': 'get_media_changers'}
        ret = self.send(request, timeout, retry)
        if e_errors.is_ok(ret):
            result = ret['media_changers']
        else:
            result = ret
        return result

    def get_media_changers2(self, timeout=0, retry=0, conf_dict=None):
        """Get list of media changers with full config info.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :type conf_dict: :obj:`dict`
        :arg conf_dict: (Optional) config dict to search in lieu of getting dict
                          from config server
        :rtype: :obj:`list` list of media changers
         """
        media_changer_list = []

        if conf_dict is None:
            conf_dict = self.dump_and_save(timeout=timeout, retry=retry)
        if not e_errors.is_ok(conf_dict):
            return media_changer_list

        for item in conf_dict.items():
            if item[0][-14:] == ".media_changer":
                item[1]['name'] = item[0]
                item[1]['media_changer'] = item[0][:-14]
                media_changer_list.append(item[1])

        return media_changer_list

    def get_migrators_list(self, timeout=0, retry=0):
        """Get list of the migrators directly from configuration server.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`list` list of migrators
        """
        request = {'work': 'get_migrators'}
        ret = self.send(request, timeout, retry)
        if e_errors.is_ok(ret):
            result = ret['migrators']
        else:
            result = ret
        return result

    def get_proxy_servers2(self, timeout=0, retry=0, conf_dict=None):
        """Get list of proxy servers with full config info.
        Deprecated as we do not use proxy servers.
        """
        proxy_server_list = []

        if conf_dict is None:
            conf_dict = self.dump_and_save(timeout=timeout, retry=retry)
            if not e_errors.is_ok(conf_dict):
                return proxy_server_list

        for item in conf_dict.items():
            if item[0][-17:] == ".udp_proxy_server":
                item[1]['name'] = item[0]
                item[1]['udp_proxy_server'] = item[0][:-17]
                proxy_server_list.append(item[1])

        return proxy_server_list

    def get_dict_entry(self, key_value, timeout=0, retry=0):
        """
        Get the configuration dictionary element(s) that contain the specified
        key, value pair.

        :type key_value: :obj:`tuple`
        :arg key_value: key - value tuple
        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict`

        """

        request = {'work': 'get_dict_element',
                   'keyValue': key_value}
        return self.send(request, timeout, retry)

    def reply_serverlist(self, timeout=0, retry=0):
        """Get the configuration dictionary keys that refer to enstore servers.

        :type timeout: :obj:`float`
        :arg timeout: reply waiting time
        :type retry: :obj:`int`
        :arg retry: number of retries
        :rtype: :obj:`dict`
        """

        request = {'work': 'reply_serverlist'}
        return self.send(request, timeout, retry)


class ConfigurationClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=None, user_mode=1):
        # fill in the defaults for the possible options
        # self.do_parse = flag
        # self.restricted_opts = opts
        if args is None:
            args = sys.argv
        self.config_file = ""
        self.show = 0
        self.load = 0
        self.server = ""
        self.alive_rcv_timeout = generic_client.DEFAULT_TIMEOUT
        self.alive_retries = generic_client.DEFAULT_TRIES
        self.summary = 0
        self.timestamp = 0
        self.threaded_impl = None
        self.list_library_managers = 0
        self.list_media_changers = 0
        self.list_movers = 0
        self.list_migrators = 0
        self.file_fallback = 0
        self.print_1 = 0
        self.copy = None

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

        # if we are using the default host and port, warn the user
        option.check_for_config_defaults()

    def valid_dictionaries(self):
        return (self.help_options, self.alive_options, self.trace_options,
                self.config_options)

    config_options = {
        option.CONFIG_FILE: {option.HELP_STRING: "config file to load",
                             option.VALUE_USAGE: option.REQUIRED,
                             option.DEFAULT_TYPE: option.STRING,
                             option.USER_LEVEL: option.ADMIN},
        option.COPY: {option.HELP_STRING: "internal copy level",
                      option.VALUE_USAGE: option.REQUIRED,
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.USER_LEVEL: option.HIDDEN},
        option.FILE_FALLBACK: {option.HELP_STRING: "return configuration from"
                                                   " file if configuration server is down",
                               option.DEFAULT_TYPE: option.INTEGER,
                               option.USER_LEVEL: option.ADMIN},
        option.LIST_LIBRARY_MANAGERS: {option.HELP_STRING: "list all library managers in "
                                                           "configuration",
                                       option.DEFAULT_VALUE: option.DEFAULT,
                                       option.DEFAULT_TYPE: option.INTEGER,
                                       option.VALUE_USAGE: option.IGNORED,
                                       option.USER_LEVEL: option.ADMIN},
        option.LIST_MEDIA_CHANGERS: {option.HELP_STRING: "list all media changers in "
                                                         "configuration",
                                     option.DEFAULT_VALUE: option.DEFAULT,
                                     option.DEFAULT_TYPE: option.INTEGER,
                                     option.VALUE_USAGE: option.IGNORED,
                                     option.USER_LEVEL: option.ADMIN},
        option.LIST_MOVERS: {option.HELP_STRING: "list all movers in configuration",
                             option.DEFAULT_VALUE: option.DEFAULT,
                             option.DEFAULT_TYPE: option.INTEGER,
                             option.VALUE_USAGE: option.IGNORED,
                             option.USER_LEVEL: option.ADMIN},
        option.LIST_MIGRATORS: {option.HELP_STRING: "list all migrators in configuration",
                                option.DEFAULT_VALUE: option.DEFAULT,
                                option.DEFAULT_TYPE: option.INTEGER,
                                option.VALUE_USAGE: option.IGNORED,
                                option.USER_LEVEL: option.ADMIN},
        option.LOAD: {option.HELP_STRING: "load a new configuration",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.USER_LEVEL: option.ADMIN},
        option.PRINT: {option.HELP_STRING: "print the current configuration",
                       option.DEFAULT_TYPE: option.INTEGER,
                       # Default label is used for switches that take an
                       # unknown number arguments from intf.args and not
                       # from the specification in this dictionary.
                       option.DEFAULT_LABEL: "[value_name [value_name [...]]]",
                       option.USER_LEVEL: option.ADMIN,
                       option.DEFAULT_NAME: "print_1",
                       },
        option.SHOW: {option.HELP_STRING: "print the current configuration in python format",
                      option.DEFAULT_TYPE: option.INTEGER,
                      # Default label is used for switches that take an
                      # unknown number arguments from intf.args and not
                      # from the specification in this dictionary.
                      option.DEFAULT_LABEL: "[value_name [value_name [...]]]",
                      option.USER_LEVEL: option.ADMIN,
                      # option.VALUE_LABEL:"[value_name [value_name [...]]]",
                      # option.EXTRA_VALUES:[{
                      #    option.VALUE_NAME:"server",
                      #    option.VALUE_TYPE:option.STRING,
                      #    option.VALUE_USAGE:option.OPTIONAL,
                      #    option.DEFAULT_TYPE:None,
                      #    option.DEFAULT_VALUE:None
                      #    }]
                      },
        option.SUMMARY: {option.HELP_STRING: "summary for saag",
                         option.DEFAULT_TYPE: option.INTEGER,
                         option.USER_LEVEL: option.ADMIN},
        option.TIMESTAMP: {option.HELP_STRING: "last time configfile was reloaded",
                           option.DEFAULT_TYPE: option.INTEGER,
                           option.USER_LEVEL: option.ADMIN},
        option.THREADED_IMPL: {option.HELP_STRING: "Turn on / off threaded implementation",
                               option.VALUE_USAGE: option.REQUIRED,
                               option.DEFAULT_TYPE: option.INTEGER,
                               option.USER_LEVEL: option.ADMIN},
    }


# Used for --print.
def flatten2(prefix, value, flat_dict):
    if isinstance(value, types.DictType):
        for i in value.keys():
            if prefix:
                flatten2(prefix + '.' + str(i), value[i], flat_dict)
            else:
                flatten2(str(i), value[i], flat_dict)  # Avoid . for first char.
    elif isinstance(value, types.ListType) or isinstance(value, types.TupleType):
        for i in range(len(value)):
            if prefix:
                flatten2(prefix + '.' + str(i), value[i], flat_dict)
            else:
                flatten2(str(i), value[i], flat_dict)  # Avoid . for first char.
    else:
        flat_dict[prefix] = value


def print_configuration(config_dict, intf_arg, prefix=""):
    if intf_arg.show:
        # If there wasn't a problem finding the information, print it.
        if isinstance(config_dict, types.StringType):
            # Suppress the '' that pprint.pprint() wants to surround
            # native strings.
            print config_dict
        else:
            pprint.pprint(config_dict)

    elif intf_arg.print_1:
        # Make a dictionary that only contains the flattened names for the
        # keys with their values.
        flat_dict = {}
        flatten2(prefix, config_dict, flat_dict)

        # Sort the list and print the values out.
        sorted_list = flat_dict.keys()
        sorted_list.sort()
        for key in sorted_list:
            print "%s:%s" % (key, flat_dict[key])


def do_work(intf_arg): # pragma: no cover
    csc = ConfigurationClient((intf_arg.config_host, intf_arg.config_port))
    csc.csc = csc
    result = csc.handle_generic_commands(MY_SERVER, intf_arg)
    if intf_arg.alive:
        if result['status'] == (e_errors.OK, None):
            print "Server configuration found at %s." % (result['address'],)
    if result:
        pass
    elif intf_arg.show or intf_arg.print_1:
        if intf_arg.alive_rcv_timeout != generic_client.DEFAULT_TIMEOUT:
            use_timeout = intf_arg.alive_rcv_timeout
        elif intf_arg.file_fallback:
            use_timeout = 3  # Need to override in this case.
        else:
            use_timeout = generic_client.DEFAULT_TIMEOUT

        if intf_arg.alive_retries != generic_client.DEFAULT_TIMEOUT:
            use_tries = intf_arg.alive_retries
        elif intf_arg.file_fallback:
            use_tries = 3  # Need to override in this case.
        else:
            use_tries = generic_client.DEFAULT_TRIES

        # Attempt to get the configuration from the configuration server.
        try:
            result = csc.dump(use_timeout, use_tries)
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status': (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status': (e_errors.NET_ERROR, str(msg))}

        # If we didn't get the configuration from the server, attempt
        # to get it from the local copy of the configuration file.
        if result['status'][0] in [e_errors.TIMEDOUT, e_errors.NET_ERROR]:
            if intf_arg.file_fallback:
                result = {}
                try:
                    result['dump'] = configdict_from_file()
                    result['status'] = (e_errors.OK, "")
                except IOError, msg:
                    result['status'] = (e_errors.IOERROR, str(msg))
                except OSError, msg:
                    result['status'] = (e_errors.OSERROR, str(msg))

        # If there is an error it is printed out at the end of the function
        # in csc.check_ticket().  On success, work as normal.
        if e_errors.is_ok(result):
            # Loop through what the user specified (if anything) and return
            # the desired result(s).
            use_config = result['dump']
            prefix = ""  # prefix is only used if --print was given.
            for item in intf_arg.args:
                if isinstance(use_config, types.DictType):
                    try:
                        use_config = use_config[item]
                        # prefix is only used if --print was given.
                        if prefix:
                            prefix = "%s.%s" % (prefix, item)
                        else:
                            prefix = "%s" % (item,)
                    except KeyError:
                        result['status'] = (e_errors.KEYERROR,
                                            "Unable to find requested information (1).\n")
                        break
                else:
                    result['status'] = (e_errors.CONFLICT,
                                        "Unable to find requested information (2).\n")
                    break
            else:
                # Print the configuration to the terminal/stdout.
                print_configuration(use_config, intf_arg, prefix)

    elif intf_arg.load:
        result = csc.load(intf_arg.config_file, intf_arg.alive_rcv_timeout,
                          intf_arg.alive_retries)

    elif intf_arg.summary:
        result = csc.get_keys(intf_arg.alive_rcv_timeout, intf_arg.alive_retries)
        pprint.pprint(result['get_keys'])

    elif intf_arg.timestamp:
        result = csc.config_load_time(intf_arg.alive_rcv_timeout,
                                      intf_arg.alive_retries)
        if e_errors.is_ok(result):
            print time.ctime(result['config_load_timestamp'])
    elif intf_arg.threaded_impl is not None:
        result = csc.threaded(intf_arg.threaded_impl, intf_arg.alive_rcv_timeout,
                              intf_arg.alive_retries)
        print result
    elif intf_arg.copy is not None:
        result = csc.copy_level(intf_arg.copy, intf_arg.alive_rcv_timeout,
                                intf_arg.alive_retries)
        print result
    elif intf_arg.list_library_managers:
        try:
            result = csc.get_library_managers(
                timeout=intf_arg.alive_rcv_timeout, retry=intf_arg.alive_retries)
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status': (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status': (e_errors.NET_ERROR, str(msg))}

        if result.get("status", None) is None or e_errors.is_ok(result):
            msg_spec = "%25s %15s"
            print msg_spec % ("library manager", "host")
            for lm_name in result.values():
                lm_info = csc.get(lm_name['name'])
                print msg_spec % (lm_name['name'], lm_info['host'])

    elif intf_arg.list_media_changers:
        try:
            result = csc.get_media_changers(
                timeout=intf_arg.alive_rcv_timeout, retry=intf_arg.alive_retries)
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status': (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status': (e_errors.NET_ERROR, str(msg))}

        if result.get("status", None) is None or e_errors.is_ok(result):
            msg_spec = "%25s %15s %20s"
            print msg_spec % ("media changer", "host", "type")
            for mc_name in result.values():
                mc_info = csc.get(mc_name['name'])
                print msg_spec % (mc_name['name'], mc_info['host'],
                                  mc_info['type'])

    elif intf_arg.list_movers:
        movers_list = []
        try:
            movers_list = csc.get_movers(None,
                                         timeout=intf_arg.alive_rcv_timeout, retry=intf_arg.alive_retries)
            result = {'status': (e_errors.OK, None)}
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status': (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status': (e_errors.NET_ERROR, str(msg))}

        if isinstance(movers_list, types.ListType):
            msg_spec = "%15s %15s %9s %10s %15s"
            print msg_spec % ("mover", "host", "mc_device", "driver", "library")
            for mover_name in movers_list:
                mover_info = csc.get(mover_name['mover'])
                print msg_spec % (mover_name['mover'], mover_info['host'],
                                  mover_info.get('mc_device', 'N/A'), mover_info['driver'],
                                  mover_info['library'])

    elif intf_arg.list_migrators:
        try:
            result = csc.get_migrators_list(
                timeout=intf_arg.alive_rcv_timeout, retry=intf_arg.alive_retries)
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status': (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status': (e_errors.NET_ERROR, str(msg))}

        if result.get("status", None) is None or e_errors.is_ok(result):
            msg_spec = "%25s %15s"
            print msg_spec % ("migrator", "host")
            for migrator in result.values():
                mig_info = csc.get(migrator['name'])
                print msg_spec % (migrator['name'], mig_info['host'])
    else:
        intf_arg.print_help()
        sys.exit(0)

    csc.check_ticket(result)


# configdict_from_file() -- make configdict from config file
def configdict_from_file(config_file=None):
    # if no config_file, get it from ENSTORE_CONFIG_FILE
    if not config_file:
        config_file = os.environ['ENSTORE_CONFIG_FILE']
    f = open(config_file)
    res = imp.load_module("fake config", f, 'config-file', ('.py', 'r', imp.PY_SOURCE))
    f.close()
    return res.configdict


def get_config_dict(timeout=5, retry=2):
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = ConfigurationClient((config_host, config_port))
    config_dict = csc.dump_and_save(timeout, retry)
    if not e_errors.is_ok(config_dict):
        try:
            config_dict = configdict_from_file()
            print "configuration_server is not responding ..." \
                  "Get configuration from local file: %s" % \
                  (os.environ['ENSTORE_CONFIG_FILE'],)
        except KeyError:
            config_dict = {}
    return config_dict


if __name__ == "__main__":
    Trace.init(MY_NAME)

    # fill in interface
    intf = ConfigurationClientInterface(user_mode=0)
    do_work(intf)

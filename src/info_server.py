#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################

'''
Readonly access to file and volume database
'''

# system import
import Queue
import select
import socket
import string
import sys

# enstore import
import Trace
import callback
import checksum
import configuration_client
import dispatching_worker
import e_errors
import edb
import enstore_constants
import enstore_functions3
import event_relay_messages
import file_clerk
import generic_server
import hostaddr
import volume_clerk

MY_NAME = enstore_constants.INFO_SERVER  # "info_server"
SEQUENTIAL_QUEUE_SIZE = enstore_constants.SEQUENTIAL_QUEUE_SIZE
PARALLEL_QUEUE_SIZE = enstore_constants.PARALLEL_QUEUE_SIZE
MAX_CONNECTION_FAILURE = enstore_constants.MAX_CONNECTION_FAILURE
MAX_THREADS = enstore_constants.MAX_THREADS
MAX_CONNECTIONS = MAX_THREADS + 1


# err_msg(fucntion, ticket, exc, value) -- format error message from
# exceptions

def err_msg(function, ticket, exc, value, tb=None):
    return function + ' ' + `ticket` + ' ' + str(exc) + ' ' + str(value) + ' ' + str(tb)


class Interface(generic_server.GenericServerInterface):
    def __init__(self):
        generic_server.GenericServerInterface.__init__(self)

    def valid_dictionary(self):
        return (self.help_options)


class Server(volume_clerk.VolumeClerkInfoMethods,
             file_clerk.FileClerkInfoMethods,
             generic_server.GenericServer):
    def __init__(self, csc):
        self.debug = 0
        generic_server.GenericServer.__init__(self, csc, MY_NAME,
                                              function=self.handle_er_msg)
        self.csc = configuration_client.ConfigurationClient(csc)
        self.keys = self.csc.get(MY_NAME)
        if not e_errors.is_ok(self.keys):
            message = "Unable to acquire configuration info for %s: %s: %s" % \
                      (MY_NAME, self.keys['status'][0], self.keys['status'][1])
            Trace.log(e_errors.ERROR, message)
            sys.exit(1)

        dispatching_worker.DispatchingWorker.__init__(
            self, (self.keys['hostip'], self.keys['port']))

        dbInfo = None
        try:
            dbInfo = self.csc.get('database')
        except Exception, msg:
            Trace.alarm(e_errors.ERROR, str(msg), {})
            Trace.log(e_errors.ERROR, "can not find database key in configuration")
            sys.exit(1)

        file_clerk.MY_NAME = MY_NAME
        volume_clerk.MY_NAME = MY_NAME

        self.sequentialQueueSize = self.keys.get('sequential_queue_size', SEQUENTIAL_QUEUE_SIZE)
        self.parallelQueueSize = self.keys.get('parallel_queue_size', PARALLEL_QUEUE_SIZE)
        self.numberOfParallelWorkers = self.keys.get('max_threads', MAX_THREADS)
        self.max_connections = self.numberOfParallelWorkers + 1

        self.volumedb_dict = edb.VolumeDB(host=dbInfo.get('db_host', 'localhost'),
                                          port=dbInfo.get('db_port', 8888),
                                          user=dbInfo.get('dbuser_reader', 'enstore_reader'),
                                          database=dbInfo.get('dbname', 'enstoredb'),
                                          auto_journal=0,
                                          max_connections=self.max_connections,
                                          max_idle=int(self.max_connections * 0.9 + 0.5))

        self.filedb_dict = edb.FileDB(host=dbInfo.get('db_host', 'localhost'),
                                      port=dbInfo.get('db_port', 8888),
                                      user=dbInfo.get('dbuser_reader', 'enstore_reader'),
                                      database=dbInfo.get('dbname', 'enstoredb'),
                                      auto_journal=0,
                                      max_connections=self.max_connections,
                                      max_idle=int(self.max_connections * 0.9 + 0.5))

        self.volumedb_dict.dbaccess.set_retries(MAX_CONNECTION_FAILURE)
        self.filedb_dict.dbaccess.set_retries(MAX_CONNECTION_FAILURE)

        self.sequentialThreadQueue = Queue.Queue(self.sequentialQueueSize)
        self.sequentialWorker = dispatching_worker.ThreadExecutor(self.sequentialThreadQueue, self)
        self.sequentialWorker.start()

        self.parallelThreadQueue = Queue.Queue(self.parallelQueueSize)
        self.parallelThreads = []
        for i in range(self.numberOfParallelWorkers):
            worker = dispatching_worker.ThreadExecutor(self.parallelThreadQueue, self)
            self.parallelThreads.append(worker)
            worker.start()

        # setup the communications with the event relay task
        self.event_relay_subscribe([event_relay_messages.NEWCONFIGFILE])
        self.set_error_handler(self.info_error_handler)
        self.erc.start_heartbeat(enstore_constants.INFO_SERVER,
                                 self.alive_interval)

    def invoke_function(self, function, args=()):
        if function.__name__ == "quit":
            apply(function, args)
        elif function.__name__ in ("find_same_file", "find_same_file2"):
            Trace.trace(5, "Putting on sequential thread queue %d %s" % (
                self.sequentialThreadQueue.qsize(), function.__name__))
            self.sequentialThreadQueue.put([function.__name__, args])
        else:
            Trace.trace(5, "Putting on parallel thread queue %d %s" % (
            self.parallelThreadQueue.qsize(), function.__name__))
            self.parallelThreadQueue.put([function.__name__, args])

    def close(self):
        self.filedb_dict.close()
        self.volume_dict.close()

    def info_error_handler(self, exc, msg, tb):
        __pychecker__ = "unusednames=tb"
        self.reply_to_caller({'status': (str(exc), str(msg), 'error'),
                              'exc_type': str(exc), 'exc_value': str(msg)})

    # The following are local methods
    # get a port for the data transfer
    # tell the user I'm your info clerk and here's your ticket
    def get_user_sockets(self, ticket):
        try:
            addr = ticket['callback_addr']
            if not hostaddr.allow(addr):
                return 0
            info_clerk_host, info_clerk_port, listen_socket = callback.get_callback()
            listen_socket.listen(4)
            ticket["info_clerk_callback_addr"] = (info_clerk_host, info_clerk_port)
            self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.control_socket.connect(addr)
            callback.write_tcp_obj(self.control_socket, ticket)

            r, w, x = select.select([listen_socket], [], [], 15)
            if not r:
                listen_socket.close()
                return 0
            data_socket, address = listen_socket.accept()
            if not hostaddr.allow(address):
                data_socket.close()
                listen_socket.close()
                return 0
            self.data_socket = data_socket
            listen_socket.close()
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc, msg)
            return 0
        return 1

    ####################################################################

    # turn on/off the debugging
    def debugging(self, ticket):
        self.debug = ticket.get('level', 0)
        print 'debug =', self.debug

    # These need confirmation
    def quit(self, ticket):
        self.sequentialThreadQueue.put(None)
        for t in self.parallelThreads:
            self.parallelThreadQueue.put(None)
        self.sequentialWorker.join(10.)
        for t in self.parallelThreads:
            t.join(10.)
        dispatching_worker.DispatchingWorker.quit(self, ticket)

    def file_info(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return  # extract_bfid_from_ticket handles its own errors.
        self.__find_file(bfid, ticket, error_target="file_info",
                         item_name="file_info",
                         include_volume_info=True)
        self.reply_to_caller(ticket)
        return

    # Underlying function that powers find_file_by_path(),
    # find_file_by_pnfsid() and find_file_by_location().
    #
    # item_name is used by file_info() to set the information into
    # as a tuple that is a member of the ticket with item_name as the
    # key.
    #
    # error_target is a string describing what is being looked for.
    def __find_file(self, bfid, ticket, error_target, item_name=None,
                    include_volume_info=False):
        # Get the file information from the database.
        finfo = self.filedb_dict[bfid]
        if not finfo:
            ticket['status'] = (e_errors.NO_FILE,
                                "%s: %s not found" % (MY_NAME, error_target))
            Trace.log(e_errors.ERROR, "%s" % (ticket,))
            return
        if include_volume_info:
            # Get the volume information from the database.
            vinfo = self.volumedb_dict[finfo['external_label']]
            if not vinfo:
                ticket['status'] = (e_errors.NO_VOLUME,
                                    "%s: %s not found" % (MY_NAME, error_target))
                Trace.log(e_errors.ERROR, "%s" % (ticket,))
                return

        # Combine the file and volume information together.
        combined_dict = {}
        for key in finfo.keys():
            combined_dict[key] = finfo[key]
        if include_volume_info:
            for key in vinfo.keys():
                combined_dict[key] = vinfo[key]

        # Put the information into the ticket in the correct place.
        if ticket.has_key('file_list'):
            ticket['file_list'].append(combined_dict)
            return
        else:
            if item_name:
                ticket[item_name] = combined_dict
                print "ticket:", ticket
            else:
                for key in combined_dict.keys():
                    ticket[key] = combined_dict[key]
        ticket["status"] = (e_errors.OK, None)
        return

    # find_file_by_path() -- find a file using pnfs_path
    def __find_file_by_path(self, ticket):
        ticket['status'] = (e_errors.INFO_SERVER_ERROR, "lookup by path disabled, use bfid, of pnfsid")
        return

    # find_file_by_path() -- find a file using pnfs id
    def find_file_by_path(self, ticket):
        self.__find_file_by_path(ticket)
        self.reply_to_caller(ticket)
        return

    # This version can handle replying with a large number of file matches.
    def find_file_by_path2(self, ticket):
        self.__find_file_by_path(ticket)
        self.send_reply_with_long_answer(ticket)
        return

    # find_file_by_pnfsid() -- find a file using pnfs id
    def __find_file_by_pnfsid(self, ticket):
        pnfs_id = self.extract_value_from_ticket("pnfsid", ticket,
                                                 fail_None=True)
        if not pnfs_id:
            return  # extract_value_from_ticket handles its own errors.
        if not enstore_functions3.is_pnfsid(pnfs_id) and not enstore_functions3.is_chimeraid(pnfs_id):
            message = "pnfsid %s not valid" % \
                      (pnfs_id,)
            ticket["status"] = (e_errors.WRONG_FORMAT, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return
        q = "select bfid from file where pnfs_id = %s"
        res = []
        try:
            res = self.filedb_dict.query_dictresult(q, (pnfs_id,))
        #
        # edb module raises underlying DB errors as EnstoreError.
        #
        except e_errors.EnstoreError, msg:
            ticket['status'] = (msg.type,
                                "failed to find bfid for pnfs_id %s" % (pnfs_id,))

            return
        except:
            ticket['status'] = (e_errors.INFO_SERVER_ERROR,
                                "failed to find bfid for pnfs_id %s" % (pnfs_id,))
            return
        if not res:
            ticket['status'] = (e_errors.NO_FILE,
                                "%s: %s not found" % (MY_NAME, pnfs_id))
            Trace.log(e_errors.ERROR, "%s" % (ticket,))
            return

        if len(res) > 1:
            ticket['status'] = (e_errors.TOO_MANY_FILES,
                                "%s: %s %s matches found" % \
                                (MY_NAME, pnfs_id, len(res)))
            ticket['file_list'] = []
            for db_info in res:
                bfid = db_info.get('bfid')
                self.__find_file(bfid, ticket, pnfs_id)
        else:
            bfid = res[0].get('bfid')
            self.__find_file(bfid, ticket, pnfs_id)
        return ticket

    # find_file_by_pnfsid() -- find a file using pnfs id
    def find_file_by_pnfsid(self, ticket):
        self.__find_file_by_pnfsid(ticket)
        self.reply_to_caller(ticket)
        return

    # This version can handle replying with a large number of file matches.
    def find_file_by_pnfsid2(self, ticket):
        self.__find_file_by_pnfsid(ticket)
        self.send_reply_with_long_answer(ticket)
        return

    # find_file_by_location() -- find a file using pnfs_path
    def __find_file_by_location(self, ticket):

        # label = self.extract_external_label_from_ticket(ticket):
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return  # extract_external_label_from_ticket handles its own errors.

        location_cookie = self.extract_value_from_ticket(
            "location_cookie", ticket, fail_None=True)
        if not location_cookie:
            return  # extract_value_from_ticket handles its own errors.
        if not enstore_functions3.is_location_cookie(location_cookie):
            message = "volume location %s not valid" % \
                      (location_cookie,)
            ticket["status"] = (e_errors.WRONG_FORMAT, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return

        q = ("select bfid from file, volume where "
             "file.volume = volume.id and label = %s and  "
             "location_cookie = %s")
        res = []
        try:
            res = self.filedb_dict.query_dictresult(q, (external_label, location_cookie,))
        #
        # edb module raises underlying DB errors as EnstoreError.
        #
        except e_errors.EnstoreError, msg:
            ticket['status'] = (msg.type,
                                "failed to find bfid for volume:location %s:%s" % (external_label, location_cookie,))

            return ticket
        except:
            ticket['status'] = (e_errors.INFO_SERVER_ERROR,
                                "failed to find bfid for volume:location %s:%s" % (external_label, location_cookie,))
            return ticket

        if len(res) > 1:
            ticket["status"] = (e_errors.OK, None)
            ticket['file_list'] = []
            for db_info in res:
                bfid = db_info.get('bfid')
                self.__find_file(bfid,
                                 ticket,
                                 "%s:%s" %
                                 (external_label, location_cookie))
        else:
            bfid = res[0].get('bfid')
            self.__find_file(bfid, ticket, "%s:%s" % (external_label, location_cookie))
        return ticket

    # find_file_by_location() -- find a file using pnfs_path
    def find_file_by_location(self, ticket):
        self.__find_file_by_location(ticket)

        self.reply_to_caller(ticket)
        return

    # This version can handle replying with a large number of file matches.
    def find_file_by_location2(self, ticket):
        self.__find_file_by_location(ticket)

        self.send_reply_with_long_answer(ticket)
        return

    # find_same_file() -- find files that match the size and crc
    def __find_same_file(self, ticket):
        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return  # extract_bfid_from_ticket handles its own errors.
        try:
            q = ("select bfid, crc, sanity_crc from file "
                 "where size = %s and sanity_size = %s and crc = %s"
                 "order by bfid asc ")

            res = self.filedb_dict.query_dictresult(q, (record["size"],
                                                        record["sanity_cookie"][0],
                                                        record['complete_crc'],))

            if len(res) <= 1:
                q = ("select bfid, crc, sanity_crc from file "
                     "where size = %s and sanity_size = %s "
                     "order by bfid asc ")

                res = self.filedb_dict.query_dictresult(q, (record["size"],
                                                            record["sanity_cookie"][0],))

            ticket["files"] = []
            for i in res:
                crc = i.get("crc")
                sanity_crc = i.get("sanity_crc")
                if crc == record["complete_crc"] and sanity_crc == record['sanity_cookie'][0]:
                    ticket["files"].append(self.filedb_dict[i.get("bfid")])
                else:
                    """
                    Input record may contain seeded 0 or seeded 1 CRC.
                    So is database result. Take care of it:
                    """
                    if (crc <= 0 or
                        sanity_crc <= 0 or
                        record["sanity_cookie"][1] <= 0 or
                        record["complete_crc"] <= 0):
                        continue
                    crc_adler32 = checksum.convert_0_adler32_to_1_adler32(
                        crc, record["size"])
                    record_crc_adler32 = checksum.convert_0_adler32_to_1_adler32(
                        record["complete_crc"], record["size"])
                    sanity_crc_adler32 = checksum.convert_0_adler32_to_1_adler32(
                        sanity_crc, record["sanity_cookie"][0])
                    record_sanity_crc_adler32 = checksum.convert_0_adler32_to_1_adler32(
                        record["sanity_cookie"][1], record["sanity_cookie"][0])

                    if ((crc == record_crc_adler32 or
                         crc_adler32 == record["complete_crc"] or
                         crc_adler32 == record_crc_adler32)
                        and
                        (sanity_crc == record_sanity_crc_adler32 or
                         sanity_crc_adler32 == record["sanity_cookie"][1] or
                         sanity_crc_adler32 == record_sanity_crc_adler32)):
                        ticket["files"].append(self.filedb_dict[i.get("bfid")])
            ticket["status"] = (e_errors.OK, None)
        except Exception as e:
            ticket["status"] = (e_errors.ERROR, str(e))
        return ticket

    # find_same_file() -- find files that match the size and crc
    def find_same_file(self, ticket):
        self.__find_same_file(ticket)

        self.reply_to_caller(ticket)
        return

    # This version can handle replying with a large number of file matches.
    def find_same_file2(self, ticket):
        self.__find_same_file(ticket)

        self.send_reply_with_long_answer(ticket)
        return

    def __query_db_part1(self, ticket):
        try:
            q = ticket["query"]
            # only select is allowed
            qu = string.upper(q)
            query_parts = string.split(qu)

            if query_parts[0] != "SELECT" or "INTO" in query_parts:
                # Don't use e_errors.DATABASE_ERROR, since
                # this really is a situation for the info
                # server and is not a general database error.
                msg = "only simple select statement is allowed"
                ticket["status"] = (e_errors.INFO_SERVER_ERROR,
                                    msg)
                # self.reply_to_caller(ticket)
                return True
        except KeyError, detail:
            msg = "%s: key %s is missing" % (MY_NAME, detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            # self.reply_to_caller(ticket)
            ####XXX client hangs waiting for TCP reply
            return True
        return False

    def __query_db_part2(self, ticket):
        q = ticket["query"]
        result = {}
        try:
            columns, res = self.volumedb_dict.dbaccess.query_with_columns(q)
            result['fields'] = columns
            result['result'] = res
            result['ntuples'] = len(res)
            result['status'] = (e_errors.OK, None)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = 'query_db(): ' + str(exc_type) + ' ' + str(exc_value) + ' query: ' + q
            result['status'] = (e_errors.DATABASE_ERROR, msg)
        return result

    def query_db(self, ticket):

        if self.__query_db_part1(ticket):
            # Errors are send back to the client.
            self.reply_to_caller(ticket)
            return

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket, ticket)

        result = self.__query_db_part2(ticket)

        # finishing up

        callback.write_tcp_obj_new(self.data_socket, result)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket, ticket)
        self.control_socket.close()
        return

    # This is even newer and better implementation that replaces
    # query_db().  Now the network communications are done using
    # send_reply_caller_with_long_answer().
    def query_db2(self, ticket):
        # Determine if the SQL statement is allowed.
        if self.__query_db_part1(ticket):
            # Errors are send back to the client.
            self.reply_to_caller(ticket)
            return

        # start communication
        ticket["status"] = (e_errors.OK, None)
        try:
            control_socket = self.send_reply_with_long_answer_part1(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "query_db2(): %s" % (str(msg),))
            return

        # get reply
        reply = self.__query_db_part2(ticket)

        # send the reply
        try:
            self.send_reply_with_long_answer_part2(control_socket, reply)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "query_db2(): %s" % (str(msg),))
            return


if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME), "yes")
    intf = Interface()
    csc = (intf.config_host, intf.config_port)
    infoServer = Server(csc)
    infoServer.handle_generic_commands(intf)

    while 1:
        try:
            Trace.log(e_errors.INFO, "Info Server (re)starting")
            infoServer.serve_forever()
        except (e_errors.EnstoreError, ValueError):
            continue
        except SystemExit, exit_code:
            infoServer.close()
            sys.exit(exit_code)
        except:
            infoServer.serve_forever_error(string.upper(MY_NAME))
            continue

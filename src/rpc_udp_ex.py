import socket
import sys
import udp_client
import udp_server
from multiprocessing import Process

class Client:
    def __init__(self):
        self.udpc = udp_client.UDPClient()
        self.hostname = socket.gethostname()

    def send(self, ticket, addr):
        return self.udpc.send(ticket, addr, rcv_timeout=10, max_send=1)

class Server:

    def __init__(self):
        self.hostname = socket.gethostname()
	self.udps = udp_server.UDPServer(self, self.hostname, receive_timeout=60.0, use_raw=use_raw)

    def echo(self, ticket):
        print "received %s len %s"%(ticket, len(ticket))
        self.reply_to_caller(ticket)

    def serve_forever(self):
        while True:
            ticket = udps.do_request()
		# get_message
		# process_request
            self.echo(ticket)
	    if ticket['work'] == 'quit':
                exit


if __name__ == "__main__":   # pragma: no cover
    srv = Server()
    p = Process(target=srv.serve_forever, args=(queue, 1))
    p.start()

    m_len = 5
    cl = Client()
    addr = srv.get_server_address()
    ticket = {'work':'echo', 'args':"*"*m_len}
    print cl.send(ticket, addr)

    ticket = {'work':'quit'}
    print cl.send(ticket)

    p.join()

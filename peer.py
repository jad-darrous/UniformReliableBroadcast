#!/usr/bin/python

import socket
import sys, traceback

import thread, threading
import time, datetime
import random

import logging


if len(sys.argv) != 3:
	print "usage: ./peer.py <hosts_file> <peer_id>"
	sys.exit(1)


hosts_file = sys.argv[1]
peer_id = int(sys.argv[2]) - 1


logfile_name = 'process%d.log'%(peer_id)
with open(logfile_name, 'w'): pass
# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(filename=logfile_name, level=logging.DEBUG)


endpoints = map(lambda u: (u.split(":")[0], int(u.split(":")[1])), open(hosts_file).read().split())

nb_peers = len(endpoints)
peers_ids = range(nb_peers);
peer_addr = endpoints[peer_id]

print "peer started:", peer_addr, "id=", peer_id
logging.info("peer started:" + str(peer_addr))


sockets_lst = [None] * nb_peers


toHex = lambda dec: format(dec, '#04x')

sleep_alittle = time.sleep(random.random())


def send_packet(pid, msg):
	assert pid in peers_ids, "Wrong pid %d" % pid
	# logging.debug('send_packet: sending [%s] to %d' % (msg, pid))
	s = sockets_lst[pid]
	if s == None: return False
	try:
		s.send(toHex(len(msg)) + msg)
		return True
	except socket.error as e:
		logging.debug('send_packet: socket fails when sending [%s] to %d' % (msg, pid))
		s.close()
		sockets_lst[pid] = None
		return False


def init_sockets():

	#create an INET, STREAMing socket
	# --> socket.SOCK_DGRAM
	create_socket = lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	global serversocket
	serversocket = create_socket()
	serversocket.bind(peer_addr)
	serversocket.listen(nb_peers)

	logging.debug('init_sockets: connect..')
	for pid in range(nb_peers-1, peer_id, -1):
		s = create_socket()
		s.settimeout(10)
		s.connect(endpoints[pid])
		s.send(toHex(peer_id))
		sockets_lst[pid] = s

	logging.debug('init_sockets: accept..')
	for _ in range(peer_id):
		clientsocket, address = serversocket.accept()
		pid = eval(clientsocket.recv(4))
		assert pid in peers_ids, "Wrong pid %d" % pid
		sockets_lst[pid] = clientsocket


class SimpleTopicBasedPubSub():

	def __init__(self):
		self._sub = dict()

	def start_inspecting_packets(self):
		for pid in peers_ids:
			if pid != peer_id:
				packetListener = SimpleTopicBasedPubSub.ReceivePacket(self.notify, sockets_lst[pid], pid)
				packetListener.setDaemon(True)
				packetListener.start()

	def add_subscriber(self, sub, topic):
		self._sub[topic] = sub

	def notify(self, packet, sender_id):
		topic, payload = packet[0:3], packet[3:]
		self._sub[topic](payload, sender_id)

	class ReceivePacket(threading.Thread):

		def __init__(self, notify, cs, pid):
			threading.Thread.__init__(self)
			self.notify = notify
			self.cs = cs
			self.pid = pid
			self.name = datetime.datetime.now()

		def run(self):
			logging.debug('SimpleTopicBasedPubSub/ReceivePacket/run: Listen to pid = %d' % self.pid)
			while True:
				try:
					packet_size = self.cs.recv(4)
					if len(packet_size) == 0:
						logging.debug('socket closed')
						return
					packet = self.cs.recv(eval(packet_size))
					# logging.debug('SimpleTopicBasedPubSub/ReceivePacket/run: packet received from %d content:[%s]' % (self.pid, packet[0:20]))
					self.notify(packet, self.pid)
				except socket.error as e:
					logging.exception(e)
					return


class PerfectFailureDetector(threading.Thread):

	TAG = "PFD"
	HeartBeatRequest = "HeartBeatRequest"
	HeartBeatReply = "HeartBeatReply"

	def __init__(self, notify_callback, peers_ids):
		threading.Thread.__init__(self)
		self.delta = 3
		self.alive = set(filter(lambda u: u != peer_id, peers_ids))
		self.notify = notify_callback
		self.ack_buffer = set()
		self.threadLock = threading.Lock()
		self._stopevent = threading.Event()

		packetPub.add_subscriber(self.packet_received, PerfectFailureDetector.TAG)

	def run(self):
		self.send_heartbeat()
		threading.Timer(self.delta * 2, self.timeout).start()

	def get_correct(self):
		return set(self.alive)

	def timeout(self):
		# logging.debug('PerfectFailureDetector: timeout ' + str(self.ack_buffer))

		self.threadLock.acquire()
		if len(self.alive) != len(self.ack_buffer):
			failed = self.alive - self.ack_buffer
			for pid in failed:
				self.notify(pid)
			self.alive -= failed
		self.ack_buffer = set()
		self.threadLock.release()

		if len(self.alive) > 0:
			self.send_heartbeat()
			if not self._stopevent.isSet():
				threading.Timer(self.delta, self.timeout).start()

	def send_heartbeat(self):
		for pid in self.alive:
			self.send_pfd_packet(pid, PerfectFailureDetector.HeartBeatRequest)

	def send_pfd_packet(self, pid, msg):
		return send_packet(pid, PerfectFailureDetector.TAG + msg)

	def packet_received(self, msg, sender_id):

		logging.debug('PerfectFailureDetector: packet received [%s] from pid = %d' % (msg, sender_id))
		if msg.startswith(PerfectFailureDetector.HeartBeatRequest):
			self.send_pfd_packet(sender_id, PerfectFailureDetector.HeartBeatReply)
		elif msg.startswith(PerfectFailureDetector.HeartBeatReply):
			self.threadLock.acquire()
			self.ack_buffer.add(sender_id)
			self.threadLock.release()


class UniformReliableBroadcast():

	TAG = "URB"

	def __init__(self, deliverd_callback):
		self.deliverd_callback = deliverd_callback
		self.name = datetime.datetime.now()
		self.seq = 0
		self.ack_buffer = dict()
		self.pending = set()
		self.delivered_packets = set()
		self.correct = set(peers_ids)
		self.correct.remove(peer_id)

		self.seqLock = threading.Lock()
		self.ackDictLock = threading.Lock()
		self.deliverLock = threading.Lock()

		packetPub.add_subscriber(self.packet_received, UniformReliableBroadcast.TAG)

		self.P = PerfectFailureDetector(self.proc_failed, peers_ids)
		self.P.setDaemon(True)
		self.P.start()

	def broadcast(self, payload):
		self.multicast(peers_ids, payload)

	def multicast(self, peers_ids, payload):
		logging.debug('UniformReliableBroadcast: broadcast %s' % payload)
		self.seqLock.acquire()
		self.seq = self.seq + 1
		seq_nb = self.seq
		self.pending.add((peer_id, seq_nb))
		self.seqLock.release()
		self.send_to_all(peer_id, seq_nb, payload)

	def send_to_all(self, org_sender_id, seq_nb, payload):
		for pid in self.correct:
			send_packet(pid, UniformReliableBroadcast.TAG + str((org_sender_id, seq_nb, payload)))

	def packet_received(self, msg, sender_id):

		logging.debug('UniformReliableBroadcast: deliver %s' % msg)
		org_sender_id, seq_nb, payload = eval(msg)

		if not (org_sender_id, seq_nb) in self.ack_buffer:
			self.ackDictLock.acquire()
			self.ack_buffer[(org_sender_id, seq_nb)] = (set([]), payload)
			self.ackDictLock.release()

		recv_ack = self.ack_buffer[(org_sender_id, seq_nb)][0]
		# if not sender_id in recv_ack:
		if not (org_sender_id, seq_nb) in self.pending:
			self.pending.add((org_sender_id, seq_nb))
			self.send_to_all(org_sender_id, seq_nb, payload)

		self.ack_buffer[(org_sender_id, seq_nb)][0].add(sender_id)

		self.check_for_delivery((org_sender_id, seq_nb))

		logging.debug('UniformReliableBroadcast: deliver %s %s' % \
		 (str(self.ack_buffer[(org_sender_id, seq_nb)][0]), str(self.P.alive)))

	def check_for_delivery(self, entry_key):
		if entry_key in self.delivered_packets:
			return
		self.deliverLock.acquire()
		entry_value = self.ack_buffer[entry_key]
		if self.P.get_correct() <= entry_value[0]:
			self.deliverd_callback(entry_value[1], entry_key[0])
			self.delivered_packets.add(entry_key)
		self.deliverLock.release()

	def proc_failed(self, pid):
		print 'UniformReliableBroadcast: process failed %d' % pid
		logging.debug('UniformReliableBroadcast: process failed %d' % pid)
		self.correct.remove(pid)
		self.ackDictLock.acquire()
		for k in self.ack_buffer:
			self.check_for_delivery(k)
		self.ackDictLock.release()


class App():

	def __init__(self):
		self.name = datetime.datetime.now()
		self.urb = UniformReliableBroadcast(self.deliver)
		packetPub.start_inspecting_packets()

	def main(self):
		print(time.ctime())
		for k in range(2):
			msg = "Hello, I'm peer nb " + str(peer_id) + " k = " + str(k)
			self.urb.broadcast(msg)
		if peer_id == 0:
			self.urb.broadcast("Hello to all!!")
			time.sleep(.5)
			self.urb.broadcast("Hello again")
			time.sleep(1)
			self.urb.broadcast("Good-bye!")

	def deliver(self, msg, sender):
		print "delivered msg:", msg
		logging.debug('App: deliver [%s] from %d' % (msg, sender))


def terminate_peer(msg):
	if msg == "KILL":
		print "Exiting.."
		logging.debug('#'*80)
		app.urb.P._stopevent.set()
		sys.exit(0)



try:
	packetPub = SimpleTopicBasedPubSub()
	init_sockets()
	logging.info("Sockets are ready.")
	print "Sockets are ready."
	# packetPub.add_subscriber(terminate_peer, "CTL")
	app = App()
	app.main()

except Exception as e:
	print "App Level Error"
	traceback.print_exc(file=sys.stdout)
	logging.exception(e)

finally:
	serversocket.close()

# print "Press any key to continue.."
raw_input()

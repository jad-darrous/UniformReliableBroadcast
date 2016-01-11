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


def setup_logger(logger_name, log_file):
	l = logging.getLogger(logger_name)
	l.setLevel(logging.DEBUG)

	formatter = logging.Formatter('%(asctime)s (%(threadName)-10s) # %(message)s')
	logFile = logging.FileHandler(log_file, mode='w')
	logFile.setFormatter(formatter)
	logFile.setLevel(logging.DEBUG)
	l.addHandler(logFile)

	short_formatter = logging.Formatter('%(message)s')
	console = logging.StreamHandler()
	console.setFormatter(short_formatter)
	console.setLevel(logging.INFO)
	l.addHandler(console)

	return l


logger = setup_logger("main", 'process%d.log' % peer_id)
setup_logger("PFD", 'process%d_pfd.log' % peer_id)

endpoints = map(lambda u: (u.split(":")[0], int(u.split(":")[1])), open(hosts_file).read().split())

nb_peers = len(endpoints)
peers_ids = range(nb_peers);
peer_addr = endpoints[peer_id]

logger.info("peer started: ID=%d IP@=%s" % (peer_id, peer_addr))


sockets_lst = [None] * nb_peers


toHex = lambda dec: format(dec, '#04x')

sleep_alittle = time.sleep(random.random())


def send_packet(pid, msg):
	assert pid in peers_ids, "Wrong pid %d" % pid
	# logger.debug('send_packet: sending [%s] to %d' % (msg, pid))
	s = sockets_lst[pid]
	if s == None: return False
	try:
		s.send(toHex(len(msg)) + msg)
		return True
	except socket.error as e:
		logger.debug('send_packet: socket fails when sending [%s] to %d' % (msg, pid))
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

	logger.debug('init_sockets: connect..')
	for pid in range(nb_peers-1, peer_id, -1):
		s = create_socket()
		s.settimeout(10)
		s.connect(endpoints[pid])
		s.send(toHex(peer_id))
		sockets_lst[pid] = s

	logger.debug('init_sockets: accept..')
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
			self.name = "Packet receiver [%d]" % pid

		def run(self):
			logger.debug('SimpleTopicBasedPubSub/ReceivePacket/run: Listen to pid = %d' % self.pid)
			while True:
				try:
					packet_size = self.cs.recv(4)
					if len(packet_size) == 0:
						logger.debug('socket closed')
						return
					packet = self.cs.recv(eval(packet_size))
					# logger.debug('SimpleTopicBasedPubSub/ReceivePacket/run: packet received from %d content:[%s]' % (self.pid, packet[0:20]))
					self.notify(packet, self.pid)
				except socket.error as e:
					logger.exception(e)
					return


class PerfectFailureDetector(threading.Thread):

	TAG = "PFD"
	HeartBeatRequest = "HeartBeatRequest"
	HeartBeatReply = "HeartBeatReply"

	delta = 2 # in seconds, time between sending consecutive heartbeats

	def __init__(self, notify_callback, peers_ids):
		threading.Thread.__init__(self)

		self.alive = set(filter(lambda u: u != peer_id, peers_ids))
		self.notify = notify_callback
		self.ack_buffer = set()
		self.ack_buffer_lock = threading.Lock()
 		self.logger = logging.getLogger("PFD")

		packetPub.add_subscriber(self.packet_received, PerfectFailureDetector.TAG)

	def run(self):
		self.send_heartbeat()
		threading.Timer(PerfectFailureDetector.delta * 3, self.timeout).start()

	def get_correct(self):
		return set(self.alive)

	def timeout(self):
		self.logger.debug('timeout: ack_buffer = ' + str(self.ack_buffer))

		self.ack_buffer_lock.acquire()
		if len(self.alive) != len(self.ack_buffer):
			failed = self.alive - self.ack_buffer
			self.alive -= failed
			for pid in failed:
				self.notify(pid)
		self.ack_buffer = set()
		self.ack_buffer_lock.release()

		self.logger.debug('correct processes so far = ' + str(self.alive))

		if len(self.alive) > 0:
			self.send_heartbeat()
			threading.Timer(PerfectFailureDetector.delta, self.timeout).start()
		else:
			self.logger.info('All processes are dead!')


	def send_heartbeat(self):
		for pid in self.alive:
			self.send_pfd_packet(pid, PerfectFailureDetector.HeartBeatRequest)

	def send_pfd_packet(self, pid, msg):
		return send_packet(pid, PerfectFailureDetector.TAG + msg)

	def packet_received(self, msg, sender_id):
		self.logger.debug('packet received [%s] from pid = %d' % (msg, sender_id))
		if msg == PerfectFailureDetector.HeartBeatRequest:
			self.send_pfd_packet(sender_id, PerfectFailureDetector.HeartBeatReply)
		elif msg == PerfectFailureDetector.HeartBeatReply:
			self.ack_buffer_lock.acquire()
			self.ack_buffer.add(sender_id)
			self.ack_buffer_lock.release()


class UniformReliableBroadcast():

	TAG = "URB"

	def __init__(self, delivered_callback):
		self.delivered_callback = delivered_callback

		self.seq = 0
		self.ack_buffer = dict()
		self.pending = set()
		self.delivered = set()

		self.seqLock = threading.Lock()
		self.ackDictLock = threading.Lock()
		self.deliverLock = threading.Lock()

		packetPub.add_subscriber(self.packet_received, UniformReliableBroadcast.TAG)

		self.P = PerfectFailureDetector(self.peer_failed, peers_ids)
		self.P.setDaemon(True)
		self.P.start()

	def broadcast(self, payload):
		self.multicast(peers_ids, payload)

	def multicast(self, peers_ids, payload):
		logger.info('URB: broadcast [%s]' % payload)
		self.seqLock.acquire()
		self.seq = self.seq + 1
		seq_nb = self.seq
		self.pending.add((peer_id, seq_nb))
		self.seqLock.release()
		self.send_to_all(peer_id, seq_nb, payload)

	def send_to_all(self, org_sender_id, seq_nb, payload):
		for pid in self.P.get_correct():
			serialized_msg = str((org_sender_id, seq_nb, payload))
			send_packet(pid, UniformReliableBroadcast.TAG + serialized_msg)

	def packet_received(self, msg, sender_id):

		logger.debug('URB: packet_received %s' % msg)
		org_sender_id, seq_nb, payload = eval(msg)

		msg_uid = (org_sender_id, seq_nb)

		if msg_uid in self.delivered:
			logger.debug('The msg was already delivered')
			return

		if not msg_uid in self.ack_buffer:
			self.ackDictLock.acquire()
			self.ack_buffer[msg_uid] = (set([]), payload)
			self.ackDictLock.release()

		if not msg_uid in self.pending:
			self.pending.add(msg_uid)
			self.send_to_all(org_sender_id, seq_nb, payload)

		self.ack_buffer[msg_uid][0].add(sender_id)

		self.check_for_delivery(msg_uid)

		logger.debug('URB: packet_received ack=%s correct=%s' % \
		 (str(self.ack_buffer[msg_uid][0]), str(self.P.get_correct())))

	def check_for_delivery(self, msg_uid):
		if msg_uid in self.delivered:
			return
		self.deliverLock.acquire()
		acks_ids, payload = self.ack_buffer[msg_uid]
		if self.P.get_correct() <= acks_ids:
			self.delivered_callback(payload, msg_uid[0])
			self.delivered.add(msg_uid)
		self.deliverLock.release()

	def peer_failed(self, pid):
		logger.info('URB: peer with id = %d failed' % pid)
		self.ackDictLock.acquire()
		for msg_uid in self.ack_buffer:
			self.check_for_delivery(msg_uid)
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
		logger.info('App: deliver [%s] from peer with id = %d' % (msg, sender))


def terminate_peer(msg):
	if msg == "KILL":
		print "Exiting.."
		logger.debug('#'*80)
		sys.exit(0)



try:
	packetPub = SimpleTopicBasedPubSub()
	init_sockets()
	logger.info("Sockets are ready.")
	# packetPub.add_subscriber(terminate_peer, "CTL")
	app = App()
	app.main()

except Exception as e:
	print "!! App Level Error !!"
	traceback.print_exc(file=sys.stdout)
	logger.exception(e)

finally:
	# print "Press any key to continue.."
	try: raw_input() # In case of hitting Ctr-C instead of anykey.
	except: pass
	serversocket.close()
	logger.info("Exiting..")
	logging.shutdown()

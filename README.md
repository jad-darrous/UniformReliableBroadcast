# Uniform Reliable Broadcast
A mini project that implements a Uniform Reliable Broadcast on a group of processes (peers) in **Fail-Stop** distributed system model, that is means:
 - Perfect channels
 - Fail-Crash process model
 - Perfect Failure Detector

## Intro

The goal of this project is to implements a URB over a group of processes (e.i. peers). The URB algorithm relies on Perfect Failure Detector. Each peer should be provided with a configuration file and its id. The configuration file contains all the peers TCP/IP addresses. The peers are number from 1 to n (the size of the group) and their addresses appear in the configuration file in the same order as their ids.

## TCP connection initialization

In the beginning, and because we suppose that the peers form a group, a direct connection should exist between each two peers. And because the TCP channel is bidirectional, one channel is sufficient between every pairs of processes. In total we have C(n, 2) = n*(n-1)/2 TCP connections in the group. To do so, each peer accepts connections from the peers with lower id and connects to peers with higher id.

## Simple Topic-Based Pub/Sub

A simple Pub/Sub is used because different modules (in our case the URB and PFD) use the same communication channel. To forward the received packet to its final and correct receiver, each module register a *receive callback* depending on its topic, and the topic is defined as the first three characters of the packet.
Each TCP channel is handled by a separate thread.

## Perfect Failure Detector

This PFD sends each time interval a HeartBeatRequest to all correct processes. It replays with HeartBeatReply upon receiving a request. The processes with the missing replays are declared as failed processes, ad the registered notify callback is called.
The PFD runs in a separate thread as a daemon thread; the main application doesn't have to block/wait for it to finish.

## Uniform Reliable Broadcast

The *sequence number* and *original sender id* are the unique identifiers (UID) for any message sent in the group.

When broadcasting a new message, a new sequence number is generated and the msg's UID is added to the `pending` set, which is the generated seq number and the peer id. Then, a best effort broadcast is used to broadcast the message to the correct peers.

Upon receiving a message, the message is checked if it is already delivered, then an entry in `ack_buffer` dictionary is created if not already exist with the key as the msg's UID, and the value as the set of acknowledging peers and the message payload (actual content). After delivering the message this entry is no longer needed and removed to handle the infinite memory problem.  

After that, a check is performed to detect whether this message is already received or not by checking the `pending` set; if this the first time, the set is updated and a best effort broadcast is applied, and
the sender's id is added to the ack peers set in the `ack_buffer` dictionary.

At the end, the set of correct processes (supplied by the PFD) is checked against of ack peers set; in case of inclusion, the message is delivered to the higher level (the application).

A callback is registered in the PFD to notify about failing peers. When it is executed, all the entries in the `ack_buffer` dictionary are checked for delivery.

In the implementation, multiple Locks are used to prevent race conditions between threads;
- When generating a new seq number
- When iterating over `ack_buffer` dictionary
- And when updating the `delivered` set

In the current implementation, the complete message is rebroadcasted, which is not efficient in the message's payload is big. Broadcasting just the message's UID is sufficient, and in case the message was not already received by the receiver peer, the complete message can be then requested.

## Running

### Running locally

In case of running on a single machine for development, it is sufficient to run the following script with the desired number of peers, 3 in the following example:
```sh
$ sh run_local.sh 3
```

This script works on Linux and specifically on distributions that support `gnome-terminal` (e.g. Ubuntu). To run it on different distribution, the terminal command on following line should be changed to `xterm` for other.
```sh
gnome-terminal -e "python peer.py $HOSTS_FILE $i"
```

A sleep for 0.5 second is used between lunching consecutive peers to allow the peer to bind to its address and port before the next peer try to connect to it. This problem can be solved programmatically be repeating to connect until the connection is established.

### Running on different machines

In case of running on different machines, a real world scenario, a configuration file should be prepared and distributed to all machines. The the peers should be run in decreasing order by id.
The configuration file contains the addresses (ip and port separated by ':') of the peers, each on a line.

Example: running on two machines

The `hosts.conf` file should contains two lines representing the addresses of the two machines, and should be the same on the two machines.

Running the peer with the *higher* id on the first machine:
```sh
$ python peer.py hosts.conf 2
```
Running the peer with the *lower* id on the first machine:
```sh
$ python peer.py hosts.conf 1
```

## Performance evaluation

For now there is no performance evaluation model for the implementation. Theoretically, the number of exchanged messages can be used to compare several algorithms. In practice, the *Latancy* and the *throughput* should be measured. Measuring the time taken between broadcasting a message and delivering it to the same peer is not an accurate method. And, a comparison should be done concerning the same algorithm with different implementations our parameters. Comparing Reliable Broadcast with Uniform Reliable Broadcast doesn't make much sense because they perform different tasks. So nothing done to evaluate the performance yet.

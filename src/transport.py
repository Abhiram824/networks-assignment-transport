import argparse
import json
import random
import socket
import time
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

# Note: In this starter code, we annotate types where
# appropriate. While it is optional, both in python and for this
# course, we recommend it since it makes programming easier.

# The maximum size of the data contained within one packet
payload_size = 1200
# The maximum size of a packet including all the JSON formatting
packet_size = 1500

class Receiver:
    def __init__(self):
        # TODO: Initialize any variables you want here, like the receive
        # buffer, initial congestion window and initial values for the timeout
        # values
        self.buffer: List[Tuple[int, int, str]] = []
        self.start_data_ptr = 0
        pass

    def data_packet(self, seq_range: Tuple[int, int], data: str) -> Tuple[List[Tuple[int, int]], str]:
        '''This function is called whenever a data packet is
        received. `seq_range` is the range of sequence numbers
        received: It contains two numbers: the starting sequence
        number (inclusive) and ending sequence number (exclusive) of
        the data received. `data` is a binary string of length
        `seq_range[1] - seq_range[0]` representing the data.

        It should output the list of sequence number ranges to
        acknowledge and any data that is ready to be sent to the
        application. Note, data must be sent to the application
        _reliably_ and _in order_ of the sequence numbers. This means
        that if bytes in sequence numbers 0-10000 and 11000-15000 have
        been received, only 0-10000 must be sent to the application,
        since if we send the latter bytes, we will not be able to send
        bytes 10000-11000 in order when they arrive. The transport
        layer must hide hide all packet reordering and loss.

        The ultimate behavior of the program should be that the data
        sent by the sender should be stored exactly in the same order
        at the receiver in a file in the same directory. No gaps, no
        reordering. You may assume that our test cases only ever send
        printable ASCII characters (letters, numbers, punctuation,
        newline etc), so that terminal output can be used to debug the
        program.

        '''
        # check if seq_range is already in buffer
        found = False
        for interval in self.buffer:
            if seq_range[0] == interval[0] and seq_range[1] == interval[1]:
                found = True
        if not found:
            self.buffer.append((seq_range[0], seq_range[1], data))
        self.buffer = sorted(self.buffer, key= lambda x: x[0])
        remove_idxs = []
        data_sent = ""
        for i, packet in enumerate(self.buffer):
            if packet[0] > self.start_data_ptr:
                break
            elif packet[0] == self.start_data_ptr:        
                data_sent += packet[2]
                self.start_data_ptr = packet[1]
                remove_idxs.append(i)
            else:
                remove_idxs.append(i)
        # self.buffer = [pkt for i, pkt in enumerate(self.buffer) if i not in remove_idxs]

        sent = []
        for interval in self.buffer:
            assert interval[0] < interval[1]
            sent.append((interval[0], interval[1]))
        # combine tuples if start of one is equal to end of another
        i = 0
        sent = sorted(sent, key= lambda x: x[0])
        while i < len(sent)-1:
            if sent[i][1] == sent[i+1][0]:
                sent[i] = (sent[i][0], sent[i+1][1])
                sent.pop(i+1)
            else:
                i += 1
        ret = sent[:39] + [seq_range]
        return (ret, data_sent)

        
        # return ([0, 0], '')  # Replace this

    def finish(self):
        '''Called when the sender sends the `fin` packet. You don't need to do
        anything in particular here. You can use it to check that all
        data has already been sent to the application at this
        point. If not, there is a bug in the code. A real transport
        stack will deallocate the receive buffer. Note, this may not
        be called if the fin packet from the sender is locked. You can
        read up on "TCP connection termination" to know more about how
        TCP handles this.

        '''

        # TODO
        pass

class sender_status(Enum):
    FLIGHT = 0
    ACKED = 1
    NOT_SENT = 2


RTT_UPDATE_CONST = 4
RTT_ALPHA = 1/64
CWND_START = 10
class Sender:
    def __init__(self, data_len: int):
        '''`data_len` is the length of the data we want to send. A real
        transport will not force the application to pre-commit to the
        length of data, but we are ok with it.

        '''
        # TODO: Initialize any variables you want here, for instance a
        # data structure to keep track of which packets have been
        # sent, acknowledged, detected to be lost or retransmitted
        self.data_len = data_len
        pkts = data_len//payload_size
        if data_len%payload_size != 0: pkts+=1

        self.pkt_tracker = {}
        for i in range(0, pkts):
            self.pkt_tracker[(i*payload_size, min((i+1)*payload_size, self.data_len))] = sender_status.NOT_SENT
        
        self.packet_times = {}
        self.rtt_avg = 0
        self.rtt_var = 0
        self.rto = 0
        self.slow_start = False
        self.first_loss = False
        self.cwnd = 1 if self.slow_start else CWND_START
        self.first_ack = False
        pass

    def timeout(self):
        '''Called when the sender times out.'''
        # TODO: In addition to what you did in assignment 1, set cwnd to 1
        # packet
        self.cwnd = 1
        for pkt, status in self.pkt_tracker.items():
            if status == sender_status.FLIGHT:
                self.pkt_tracker[pkt] = sender_status.NOT_SENT

    def ack_packet(self, sacks: List[Tuple[int, int]], packet_id: int) -> int:
        '''Called every time we get an acknowledgment. The argument is a list
        of ranges of bytes that have been ACKed. Returns the number of
        payload bytes new that are no longer in flight, either because
        the packet has been acked (measured by the unique ID) or it
        has been assumed to be lost because of dupACKs. Note, this
        number is incremental. For example, if one 100-byte packet is
        ACKed and another 500-byte is assumed lost, we will return
        600, even if 1000s of bytes have been ACKed before this.

        '''
        start_time = self.packet_times[packet_id]
        rtt = time.time() - start_time
        self.rtt_avg = (1-RTT_ALPHA)*self.rtt_avg + RTT_ALPHA*rtt
        self.rtt_var = (1-RTT_ALPHA)*self.rtt_var + RTT_ALPHA*abs(rtt-self.rtt_avg)
        self.rto = self.rtt_avg + 4*self.rtt_var
        self.first_ack = True
        increments = []
        for seq in sacks:
            if seq[1]-seq[0] > payload_size:
                diff = seq[1]-seq[0]
                start = seq[0]
                while diff > payload_size:
                   diff -= payload_size
                   increments.append((start, start+payload_size))
                   start += payload_size
                increments.append((start, seq[1]))
            else:
                increments.append(seq)

        acked = 0

        valids = []
        for pkt in increments:
            cur = (pkt[0], pkt[1])
            if self.pkt_tracker[cur] != sender_status.ACKED:
                valids.append(pkt)

        for pkt in valids:
            cur = (pkt[0], pkt[1])
            assert cur in self.pkt_tracker, f"invalid range, {cur}"
            if self.pkt_tracker[cur] == sender_status.FLIGHT:
                self.pkt_tracker[cur] = sender_status.ACKED
                acked += seq[1] - seq[0]

        sorted_pkts = sorted(self.pkt_tracker.keys(), key= lambda x: x[0])

        lost = 0
        found_ack = False
        for i in range(len(sorted_pkts)-1, 0, -1):
            interval = sorted_pkts[i]
            if not found_ack and self.pkt_tracker[interval] == sender_status.ACKED:
                found_ack = True
            if found_ack and self.pkt_tracker[interval] == sender_status.FLIGHT:
                self.pkt_tracker[interval] = sender_status.NOT_SENT
                lost += (interval[1] - interval[0])

        if lost > 0:
            self.cwnd /= 2
            self.cwnd = max(1, self.cwnd)
            # print("loss")
        else:
            # print("no loss")
            self.cwnd += 1
        
        self.first_loss = self.first_loss or lost > 0

        return lost + acked

    def send(self, packet_id: int) -> Optional[Tuple[int, int]]:
        '''Called just before we are going to send a data packet. Should
        return the range of sequence numbers we should send. If there
        are no more bytes to send, returns a zero range (i.e. the two
        elements of the tuple are equal). Return None if there are no
        more bytes to send, and _all_ bytes have been
        acknowledged. Note: The range should not be larger than
        `payload_size` or contain any bytes that have already been
        acknowledged

        '''


        for pkt, status in self.pkt_tracker.items():
            if status == sender_status.NOT_SENT:
                self.pkt_tracker[pkt] = sender_status.FLIGHT
                self.packet_times[packet_id] = time.time()
                return pkt

        for pkt, status in self.pkt_tracker.items():
            if status == sender_status.FLIGHT: return (0,0)
        
        return None

    def get_cwnd(self) -> int:
        return self.cwnd * packet_size

    def get_rto(self) -> float:
        return .1 if len(self.packet_times) < RTT_UPDATE_CONST or not self.first_ack else self.rto

def start_receiver(ip: str, port: int):
    '''Starts a receiver thread. For each source address, we start a new
    `Receiver` class. When a `fin` packet is received, we call the
    `finish` function of that class.

    We start listening on the given IP address and port. By setting
    the IP address to be `0.0.0.0`, you can make it listen on all
    available interfaces. A network interface is typically a device
    connected to a computer that interfaces with the physical world to
    send/receive packets. The WiFi and ethernet cards on personal
    computers are examples of physical interfaces.

    Sometimes, when you start listening on a port and the program
    terminates incorrectly, it might not release the port
    immediately. It might take some time for the port to become
    available again, and you might get an error message saying that it
    could not bind to the desired port. In this case, just pick a
    different port. The old port will become available soon. Also,
    picking a port number below 1024 usually requires special
    permission from the OS. Pick a larger number. Numbers in the
    8000-9000 range are conventional.

    Virtual interfaces also exist. The most common one is `localhost',
    which has the default IP address of `127.0.0.1` (a universal
    constant across most machines). The Mahimahi network emulator also
    creates virtual interfaces that behave like real interfaces, but
    really only emulate a network link in software that shuttles
    packets between different virtual interfaces.

    '''
    print("Starting receiver")
    receivers: Dict[str, Receiver] = {}

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
        server_socket.bind((ip, port))

        while True:
            data, addr = server_socket.recvfrom(packet_size)
            # print(f"Received data from {addr}")
            if addr not in receivers:
                receivers[addr] = Receiver()

            received = json.loads(data.decode())
            if received["type"] == "data":
                # Format check. Real code will have much more
                # carefully designed checks to defend against
                # attacks. Can you think of ways to exploit this
                # transport layer and cause problems at the receiver?
                # This is just for fun. It is not required as part of
                # the assignment.
                assert type(received["seq"]) is list
                assert type(received["seq"][0]) is int and type(received["seq"][1]) is int
                assert type(received["payload"]) is str
                assert len(received["payload"]) <= payload_size

                # Deserialize the packet. Real transport layers use
                # more efficient and standardized ways of packing the
                # data. One option is to use protobufs (look it up)
                # instead of json. Protobufs can automatically design
                # a byte structure given the data structure. However,
                # for an internet standard, we usually want something
                # more custom and hand-designed.
                sacks, app_data = receivers[addr].data_packet(tuple(received["seq"]), received["payload"])
                # Note: we immediately write the data to file
                #receivers[addr][1].write(app_data)

                # Send the ACK
                server_socket.sendto(json.dumps({"type": "ack", "sacks": sacks, "id": received["id"]}).encode(), addr)


            elif received["type"] == "fin":
                receivers[addr].finish()
                del receivers[addr]

            else:
                assert False
    print("Receiver finished")

def start_sender(ip: str, port: int, data: str, recv_window: int, simloss: float):
    sender = Sender(len(data))

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
        # So we can receive messages
        client_socket.connect((ip, port))
        # When waiting for packets when we call receivefrom, we
        # shouldn't wait more than 500ms

        # Number of bytes that we think are inflight. We are only
        # including payload bytes here, which is different from how
        # TCP does things
        inflight = 0
        packet_id  = 0
        wait = False

        while True:
            # Get the congestion condow
            cwnd = sender.get_cwnd()

            # Do we have enough room in recv_window to send an entire
            # packet?
            if inflight + packet_size <= min(recv_window, cwnd) and not wait:
                seq = sender.send(packet_id)
                if seq is None:
                    # We are done sending
                    client_socket.send('{"type": "fin"}'.encode())
                    break
                elif seq[1] == seq[0]:
                    # No more packets to send until loss happens. Wait
                    wait = True
                    continue

                assert seq[1] - seq[0] <= payload_size
                assert seq[1] <= len(data)

                # Simulate random loss before sending packets
                if random.random() < simloss:
                    pass
                else:
                    # Send the packet
                    client_socket.send(
                        json.dumps(
                            {"type": "data", "seq": seq, "id": packet_id, "payload": data[seq[0]:seq[1]]}
                        ).encode())

                inflight += seq[1] - seq[0]
                packet_id += 1

            else:
                wait = False
                # Wait for ACKs
                try:
                    rto = sender.get_rto()
                    # print(f"Setting timeout to {rto}")
                    client_socket.settimeout(rto)
                    received_bytes = client_socket.recv(packet_size)
                    received = json.loads(received_bytes.decode())
                    assert received["type"] == "ack"

                    if random.random() < simloss:
                        continue

                    inflight -= sender.ack_packet(received["sacks"], received["id"])
                    assert inflight >= 0
                except socket.timeout:
                    inflight = 0
                    print("Timeout")
                    sender.timeout()


def main():
    parser = argparse.ArgumentParser(description="Transport assignment")
    parser.add_argument("role", choices=["sender", "receiver"], help="Role to play: 'sender' or 'receiver'")
    parser.add_argument("--ip", type=str, required=True, help="IP address to bind/connect to")
    parser.add_argument("--port", type=int, required=True, help="Port number to bind/connect to")
    parser.add_argument("--sendfile", type=str, required=False, help="If role=sender, the file that contains data to send")
    parser.add_argument("--recv_window", type=int, default=15000000, help="Receive window size in bytes")
    parser.add_argument("--simloss", type=float, default=0.0, help="Simulate packet loss. Provide the fraction of packets (0-1) that should be randomly dropped")

    args = parser.parse_args()

    if args.role == "receiver":
        start_receiver(args.ip, args.port)
    else:
        if args.sendfile is None:
            print("No file to send")
            return

        with open(args.sendfile, 'r') as f:
            data = f.read()
            start_sender(args.ip, args.port, data, args.recv_window, args.simloss)

if __name__ == "__main__":
    main()

import argparse
import json
from typing import Dict, List, Tuple
import socket

# Note: In this starter code, we annotate types where
# appropriate. While it is optional, both in python and for this
# course, we recommend it since it makes programming easier.

# The maximum size of the data contained within one packet
payload_size = 1200
# The maximum size of a packet including all the JSON formatting
packet_size = 1500

class Receiver:
    def __init__(self):
        # TODO: Initialize any variables you want here, like the receive buffer
        pass

    def data_packet(self, seq_range: Tuple[int, int], data: str) -> Tuple[List[Tuple[int, int]], bytes]:
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
        sent by the sender should appearbe printed exactly in the same
        order at the receiver. No gaps, no reordering. You may assume
        that our test cases only ever send printable ASCII characters
        (letters, numbers, punctuation, newline etc), so that terminal
        output can be used to debug the program.

        IMPORTANT: In your final submission, do not print anything in
        the receiver side of your program other than the one print
        statement that exists in `start_sender`, since this will mess
        with the test scripts

        '''

        # TODO
        return ([0,0], b'') # Replace this

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

class Sender:
    def __init__(self, data_len: int):
        '''`data_len` is the length of the data we want to send. A real
        transport will not force the application to pre-commit to the
        length of data, but we are ok with it.

        '''
        # TODO: Initialize any variables you want here, for instance a
        # data structure to keep track of which packets have been
        # sent, acknowledged, detected to be lost or retransmitted
        pass

    def timeout(self):
        '''Called when the sender times out.'''
        # TODO: Read the relevant code in `start_sender` to figure out
        # what you should do here
        pass

    def ack_packet(self, sacks: List[Tuple[int, int]]):
        '''Called every time we get an acknowledgment. The argument is a list
        of ranges of bytes that have been ACKed.

        '''
        # TODO: This function does not return anything, but it is a
        # good idea to update your datastructures here
        pass

    def send(self) -> Tuple[int, int]:
        '''Called just before we are going to send a data packet. Should
        return the range of sequence numbers we should send. If there
        are no more bytes to send, returns a zero range (i.e. the two
        elements of the tuple are equal). Note: The range should not
        be larger than `payload_size` or contain any bytes that have
        already been acknowledged

        '''

        # TODO
        return (0, payload_length)

def start_receiver(ip: str, port: int):
    '''Starts a receiver thread. For each source address, we start a new
    `Receiver` class. When a `fin` packet is received, we call the
    `finish` function of that class.
    
    We start listening on the given IP address and port. By setting the
    IP address to be `0.0.0.0`, you can make it listen on all
    available interfaces. A network interface is typically a device
    connected to a computer that interfaces with the physical world to
    send/receive packets. The WiFi and ethernet cards on personal
    computers are examples of physical interfaces.
    
    Virtual interfaces also exist. The most common one is `localhost',
    which has the default IP address of `127.0.0.1` (a universal
    constant across most machines). The Mahimahi network emulator also
    creates virtual interfaces that behave like real interfaces, but
    really only emulate a network link in software that shuttles
    packets between different virtual interfaces.

    '''

    receivers: Dict[str, Receiver] = {}
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
        server_socket.bind((ip, port))
            
        while True:
            data, addr = server_socket.recvfrom(packet_size)
            if addr not in receivers:
                receivers[addr] = Receiver()
                
            received = json.parse(data.decode())
            if received["type"] == "data":
                # Format check. Real code will have much more
                # carefully designed checks to defend against
                # attacks. Can you think of ways to exploit this
                # transport layer and cause problems at the receiver?
                # This is just for fun. It is not required as part of
                # the assignment.
                assert type(received["seq"]) is tuple
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
                sacks, app_data = receivers[addr].data_packet(received["seq"], received["payload"])
                # Note: we immediately print the data. This should be the ONLY output of the program
                print(app_data)

                # Send the ACK
                server_socket.sendto(json.dumps({"type": "ack", "sacks": sacks}).encode(), addr)
                
                    
            elif received["type"] == "fin":
                receivers[addr].finish()
                del receivers[addr]
                
            server_socket.sendto(response.encode(), addr)

def start_sender(ip: str, port: int, data: bytes, recv_window: int):
    sender = Sender(len(data))
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
        # When waiting for packets when we call receivefrom, we
        # shouldn't wait more than 500ms
        socket.settimeout(0.5)

        # Number of bytes that we think are inflight. We are only
        # including payload bytes here, which is different from how
        # TCP does things
        inflight = 0
        
        while True:
            # Do we have enough room in recv_window to send an entire
            # packet?
            if inflight + packet_size < recv_window:
                seq = sender.send()
                if seq[1] == seq[0]:
                    # We are done sending
                    break
                
                assert seq[1] - seq[0] <= payload_size
                assert seq[1] <= len(data)
                client_socket.sendto(
                    json.dumps(
                        {"type": "data", "seq": seq, "data": data[seq[0]:seq[1]]}
                    ).encode(),
                    (ip, port))

                inflight += seq[1] - seq[0]
            else:
                # Wait for ACKs
                try:
                    received, addr = server_socket.recvfrom(packet_size)
                    print("Received from: ", addr, type(addr))
                    if addr != ip:
                        continue

                    received = json.parse(received)
                    assert received["type"] == "data"
                    sender.ack_packet(received.sacks)

                    inflight -= received.sacks["acked"][1] - received.sacks["acked"][0]
                    assert inflight >= 0
                except socket.timeout:
                    inflight = 0
                    sender.timeout()


def main():
    parser = argparse.ArgumentParser(description="Transport assignment")
    parser.add_argument("role", choices=["sender", "receiver"], help="Role to play: 'sender' or 'receiver'")
    parser.add_argument("--ip", type=str, required=True, help="IP address to bind/connect to")
    parser.add_argument("--port", type=int, required=True, help="Port number to bind/connect to")
    parser.add_argument("--sendfile", type=str, required=False, help="If role=sender, the file that contains data to send")
    parser.add_argument("--recv_window", type=int, default=15000, help="Receive window size in bytes")
    
    args = parser.parse_args()
    
    if args.role == "receiver":
        start_server(args.ip, args.port)
    else:
        if "sendfile" not in args.__dict__:
            print("No file to send")
            return

        with open(args.sendfile, 'rb') as f:
            data = f.read()
            start_client(args.ip, args.port, data, args.recv_window)

if __name__ == "__main__":
    main()


import time
import logging
import argparse
import sys
import bintrees 
import struct

import async
import udp 

MSS = 1024
WINDOW = 1024*1024
RTO_MIN = .01
RTO_MAX = 1.0
        
class FlowRecv(object):
    def __init__(self, sink):
        # Where do I write my data to
        self.sink= sink
        # Buffer of packets
        self.pkt_buf = bintrees.RBTree()
        # Current 'head' of unwritten data
        self.head_seq = 0
        # Current ack seq number
        self.ack_seq = 0

    # Called when a new UDP packet arrives
    def on_packet(self, seq_no, data):
        # Validate packet is sensible, ignore if not
        if (seq_no + len(data) <= self.ack_seq or  # Data is too old
            seq_no > self.head_seq + WINDOW): # Data is too new
            # Throw away packet
            return
        # Add data to buffer
        self.pkt_buf[seq_no] = data
        # See if we can move ack forward
        if seq_no <= self.ack_seq:
            # Must have some new data, since seq_no + len(data) > ack and seq_no <= ack
            for (s, d) in self.pkt_buf.item_slice(seq_no, None):
                # If we are past the area of relevance, stop 
                if s > self.ack_seq:
                    break
                # Otherwise, maybe extend ack
                self.ack_seq = max(self.ack_seq, s + len(d))
        # Try to write data (if any)
        if self.head_seq < self.ack_seq:
            self.on_sink_ready()
        # Compute window
        wout = self.head_seq + WINDOW - self.ack_seq
        #if wout < MSS:
        #    wout = 0
        # Return ack + window
        return (self.ack_seq, wout)

    # Called when sink can have at least one byte written
    def on_sink_ready(self):
        # While we have packets in the buffer
        while len(self.pkt_buf) > 0:
            # Find the earliest
            (s, d) = self.pkt_buf.min_item()
            # If it's before head_seq, drop it and continue
            if (s + len(d)) <= self.head_seq:
                del self.pkt_buf[s]
                continue
            # If we've hit a gap, break, should only happen at ack
            if s > self.head_seq:
                assert self.head_seq == self.ack_seq
                break
            # Write some data, skipping first part if needed
            to_write = d[self.head_seq - s:]
            self.sink.write(to_write)
            r = len(to_write)
            if r == 0:
                # Hit a block, stop writing
                break
            # More seq forward and go again
            self.head_seq += r

    # Returns true if we have data to sink
    def has_data(self):
        return self.head_seq < self.ack_seq

class FlowSend(object):
    def __init__(self, asm, source, packet_send):
        # Record our params 
        self.asm = asm
        self.source = source
        self.packet_send = packet_send

        # Setup send side variables
        self.send_seq = 0
        # Send inital winVdow size
        self.window = WINDOW
        # Currently acked data
        self.ack_seq = 0
        # Congestion window size
        self.cwnd = 2*MSS
        # Slow start threshold
        self.sst = WINDOW
        # NewReno state
        self.dupAcks = 0
        self.in_recover = False
        self.recover = 0
        # Ack dup counter
        self.dup_acks = 0
        # Round Trip Time avg + dev
        self.rtt_avg = .2
        self.rtt_dev = .1
        # RTO timer
        self.rto = None
        self.send_timer = None
        # HORRIBLE memcpy crazed buffer
        self.in_flight = ""
    
    def send_room(self): 
        return min(self.cwnd, self.window) - (self.send_seq - self.ack_seq)

    def on_readable(self):
        # Maybe send some data
        while self.send_room() >= MSS:
            # Always require window > MSS, SWS avoidance
            buf = self.source.read(MSS)
            if len(buf) == 0:
                # If nothing right now, stop sending
                break
            # Do the actual sending
            self.packet_send(self.send_seq, buf)
            # Add to inflight data
            self.send_seq += len(buf)
            self.in_flight = self.in_flight + buf # SO BAD

        # Set timer if needed
        if self.send_timer is None and self.send_seq > self.ack_seq:
            self.rto = self.rtt_avg + 2 * self.rtt_dev
            self.rto = max(min(self.rto, RTO_MIN), RTO_MAX)
            self.send_timer = self.asm.add_timer(time.time() + self.rto, self.on_timeout)

    def on_ack(self, ack, window, rtt):
        # RTTs are always valid, since we always send an ack
        self.rtt_avg = .875 * self.rtt_avg + .125 * rtt
        rtt_err = abs(rtt - self.rtt_avg)
        self.rtt_dev = .875 * self.rtt_dev + .125 * rtt_err
    
        if ack > self.send_seq:
            # WTF, let's just presume they are acking everthing
            ack = self.send_seq

        # If ack is old, ignore
        if ack < self.ack_seq:
            return

        if ack == self.ack_seq:
            # Ack is a DUP
            self.dup_acks += 1
            if not self.in_recover and self.dup_acks == 3:
                # Start fast recovery
                self.sst = max(len(self.in_flight)/2, 2*MSS)
                self.cwnd = self.sst + 3*MSS
                self.recover = self.send_seq
                self.in_recover = True
                pkt_size = min(MSS, len(self.in_flight))
                self.packet_send(self.ack_seq, self.in_flight[0:pkt_size])
            elif self.in_recover:
                # Continue fast recovery
                self.cwnd += MSS
                self.on_readable()
            return

        # Normal case, clear dup_acks
        self.dup_acks = 0;
        # Remove acked data from the buffer
        self.in_flight = self.in_flight[ack - self.ack_seq:]

        # Remove any existing timeout
        self.asm.cancel(self.send_timer)
        self.send_timer = None

        # Check if we are in recovery mode
        if self.in_recover:
            # Still staying there?
            if ack < self.recover:
                # Adjust cwnd
                self.cwnd += MSS
                self.cwnd -= ack - self.ack_seq
                self.ack_seq = ack
                # Resend packet
                pkt_size = min(MSS, len(self.in_flight))
                self.packet_send(self.ack_seq, self.in_flight[0:pkt_size])
                # Set timer and Early exit
                self.send_timer = self.asm.add_timer(time.time() + self.rto, self.on_timeout)
                return
            else:
                # Leave recovery  + fall though
                self.cwnd = min(self.sst, len(self.in_flight) + MSS)
                self.in_recover = False

        # Update congestion window
        self.ack_seq = ack
        self.window = window
        if self.cwnd < self.sst:
            self.cwnd += MSS
        else:
            self.cwnd += max(1, int(MSS*MSS/self.cwnd))

        # Read from source
        self.on_readable()

    def on_timeout(self):
        print "***** PACKET LOST ******"
        # Update flow control goo
        self.sst = max(2*MSS, int(len(self.in_flight) / 2))
        self.cwnd = 2*MSS
        # Resend the packet
        pkt_size = min(MSS, len(self.in_flight))
        self.packet_send(self.ack_seq, self.in_flight[0:pkt_size])
        # Restart timer
        self.rto = max(min(2 * self.rto, RTO_MIN), RTO_MAX)
        self.send_timer = self.asm.add_timer(time.time() + self.rto, self.on_timeout)
        # TODO: eventually give up

class DefaultHelpParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)

def test_send(asm):
    uconn = udp.UdpPort(asm, 8001)

def main():
    # Lame simulation
    epochy = time.time()
    source = file("../envelope.tar.gz", "rb")
    sink = file("test", "wb")
    asm = async.AsyncMgr()
    # Setup receiver
    rconn = udp.UdpPort(asm, 8000)
    recv = FlowRecv(sink)
    def on_recv_recv(raw, addr):
        (xxx, stime, seq) = struct.unpack("!cLL", raw[0:9])
        data = raw[9:]
        (ack, window) = recv.on_packet(seq, data)
        out = struct.pack("!cLLL", 'X', stime, ack, window)
        rconn.sendto(out, addr)
    rconn.add_protocol('X', on_recv_recv)
    # Setup sender
    sconn = udp.UdpPort(asm, 8001)
    def on_send_send(seq, buf):
        print "Sending: %d:%d" % (seq, len(buf))
        header = struct.pack("!cLL", 'X', int((time.time() - epochy)*1000), seq)
        sconn.sendto(header + buf, ('127.0.0.1', 8000))
    send = FlowSend(asm, source, on_send_send)
    def on_send_recv(raw, addr):
        (xxx, stime, ack, window) = struct.unpack("!cLLL", raw)
        ctime = int((time.time() - epochy)*1000)
        print "ACKed: %d:%d:%d" % (ack, window, ctime - stime)
        send.on_ack(ack, window, float(ctime-stime)/1000.0)
    sconn.add_protocol('X', on_send_recv)
    # Kick off the sender
    send.on_readable()
    # Here we go
    while True:
        asm.step()
        if send.send_seq == send.ack_seq:
            break
    sink.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()



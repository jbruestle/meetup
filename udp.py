
import asyncore
import async
import socket
import logging

logger = logging.getLogger('udp') # pylint: disable=invalid-name

class UdpPort(asyncore.dispatcher):
    def __init__(self, asm, port=6881):
        asyncore.dispatcher.__init__(self, map=asm.async_map)
        self.asm = asm
        self.create_socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bind(('', port))
        self.protocols = {}

    def writable(self):
        return False

    def add_protocol(self, magic, on_read):
        self.protocols[magic] = on_read

    def handle_read(self):
        try:
            (raw, addr) = self.recvfrom(65000)
        except socket.error as serr:
            if serr.errno != errno.EAGAIN:
                raise serr
            # Ignore EAGAIN
            return
        if len(raw) == 0:
            # Ignore empty packets (is this even possible?)
            logger.debug("%s: Received empty UDP packet", addr)
            return
        if raw[0] not in self.protocols:
            logger.debug("%s: Received invalid UDP packet", addr)
            return
        self.protocols[raw[0]](raw, addr)

    def sendto(self, raw, addr):
        try:
            self.socket.sendto(raw, addr)
        except socket.error as serr:
            if serr.errno != errno.EAGAIN:
                raise serr
            # Ignore EAGAIN



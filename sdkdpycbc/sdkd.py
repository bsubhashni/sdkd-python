#!/usr/bin/env python
import socket
from select import select

from threading import Thread, Lock

from sdkdpycbc.control import Control
from sdkdpycbc.handle import Handle

class SDKD(Thread):
    def _setup_lsn(self, port):
        lsn = socket.socket()
        lsn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsn.bind(('', port))
        lsn.listen(5)
        self.addr = lsn.getsockname()[1]

        self.lsn = lsn

    def __init__(self, port):
        super(SDKD, self).__init__()
        self._setup_lsn(port)
        self.ctl = None

    def accept_new_handles(self):
        while True:
            # Poll around..
            self.ctl.join(0)
            if not self.ctl.isAlive():
                return

            r_out, _, _ = select([self.lsn], [], [], 0.1)
            if not r_out:
                continue

            newsock, _ = self.lsn.accept()
            h = Handle(self.ctl, newsock)
            h.start()

    def run(self, *args, **kwargs):
        print "SDKD Running..."
        ctl_socket, ctl_addr = self.lsn.accept()
        self.ctl = Control(ctl_socket)
        self.ctl.start()
        self.ctl.wait_for_init()
        print "Initialization done"
        self.accept_new_handles()

def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("-l", "--listen",
                    help="listening port", type=int, default=0)
    opts = ap.parse_args()

    sdkd = SDKD(opts.listen)
    print "Listening on", sdkd.addr
    sdkd.start()
    sdkd.join()
    print "SDKD Joined.."

if __name__ == '__main__':
    main()

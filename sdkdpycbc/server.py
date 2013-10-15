import json

from threading import Thread, Event
from sdkdpycbc.protocol.message import Request, Response

class CommandNotImplemented(Exception):
    pass

class Server(Thread):
    def __init__(self, sock):
        super(Server, self).__init__()
        self.fp = sock.makefile()
        self._initev = Event()
        self._stop = False

    def stop(self):
        self._stop = True


    def recv_request(self):
        txt = self.fp.readline()
        js = json.loads(txt)
        req = Request(js)
        return req

    def send_response(self, resp):
        txt = resp.encode() + "\n"
        assert txt
        self.fp.write(txt)
        self.fp.flush()

    def wait_for_init(self):
        self._initev.wait()

    def loop_actions(self):
        pass

    def run(self, *args, **kwargs):
        self._initev.set()
        while not self._stop:
            self.loop_actions()
            msg = self.recv_request()
            self.handle_request(msg)

import json
from sdkdpycbc.protocol.results import Status

class Message(object):
    @property
    def jsondict(self):
        return self.payload

class Request(Message):
    def __init__(self, d):
        self.reqid = d['ReqID']
        self.handle_id = d['Handle']
        self.cmdname = d['Command']
        self.payload = d['CommandData']

    @classmethod
    def parse(cls, data):
        return cls(data)

class Response(Message):
    def __init__(self, request, response=None, status=None):
        if not response:
            response = {}

        self.payload = response
        self.header = {
            'Command': request.cmdname,
            'ResponseData': self.payload,
            'ReqID': request.reqid,
            'Handle': request.handle_id
        }

        if not status:
            status = Status()

        status.add_to_dict(self.header)

    def encode(self):
        return json.dumps(self.header)

    @classmethod
    def create_err(cls, req, status):
        return cls(req, status=status)

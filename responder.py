from kafka import KafkaConsumer, KafkaProducer

import threading
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import httplib
import urllib
import json
from collections import namedtuple
import io
import yaml
import os
import sys
import time
import argparse
from http.server import BaseHTTPRequestHandler, HTTPServer

host = os.getenv("BOOTSTRAP_SERVERS", default = 'kafka:9092')

def read(topic,record):
    try:
        print('reading record key: ' + record.key + " on topic: " + topic)
        #with open('service_response1.yaml', 'r') as stream:
        #    j = yaml.load(stream)
        bytes_reader = io.BytesIO(record.value)
        print('read record',record.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        print('decoding avro',record.key)
        req_schema = avro.schema.parse(open(schemas[topic], "rb").read())
        print('loaded req schema ' + schemas[topic])
        reader = avro.io.DatumReader(req_schema)
        request = reader.read(decoder)
        print('got ' + topic,record.key)
        if topic == 'scpServiceResponse' : 
            return
        response = call(request, record.key)
        reply(record.key, response, topics[topic])
    except Exception as e:
        print ('process fail', e)

def pid():
    pid = str(os.getpid())
    pidfile = "responder.pid"

    if os.path.isfile(pidfile):
        print "%s already exists, exiting" % pidfile
        sys.exit()

    file(pidfile, 'w').write(pid)

def run():
    print("connecting to " + host)
    consumer = KafkaConsumer(bootstrap_servers=host)
    consumer.subscribe(['scpServiceResponse','ocsNotificationServiceRequest','ocsServiceRequest'])
    print("consumer connected", consumer)
    count = 0

    # for each messages batch
    while True:
        try:
            print("reading")
            raw_messages = consumer.poll(timeout_ms=100, max_records=200)
            for topic_partition, messages in raw_messages.items(): 
                count = 0
                read(topic_partition.topic, messages[0])
            time.sleep(2)
            count = count +1 
            if count > 5: 
                raise ValueError, "nothing read for too long"
        except Exception as e:
            print ('-',e)
            raise e

def reply(key,res,topic):
    try:
        response=json.loads(res)#, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))

        #if response['serviceResult']:
        #    response['serviceResult']['chargingSessionId']=key

        print('json data',response)

        producer = KafkaProducer(bootstrap_servers=host) #(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        res_schema = avro.schema.parse(open(schemas[topic], "rb").read())
        print('loaded res schema ', schemas[topic], ' for topic ', topic)
        writer = avro.io.DatumWriter(res_schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(response,encoder)
        producer.send(topic, bytes_writer.getvalue(),key)
        print('sent ' , topic, key, bytes_writer.getvalue())
        producer.flush()
    except Exception as e:
        print ('response fail',e)

def call(req,key):
    try:
        conn = httplib.HTTPConnection("responder:8099")

        #args=
        #headers={'Authorization' : 'Basic %s' % base64.b64encode("username:password")}

        headers={
            'content-type':'application/json',
            'accept':'application/json',
            'X-ChargingContextId': req['header']['chargingContextId'],
            'X-RequestId': req['header']['requestId'],
            'X-SubscriberId': req['header']['subscriberId'],
            'X-SessionId': req['header']['subscriberId'],
            'X-Key': key,
        }
        #request = urllib.urlencode(json.dumps(req))
        request = json.dumps(req)
        r1 = conn.request("post", "/ocs", request, headers)
        r2 = conn.getresponse()
        res = r2.read()
        conn.close()
        print ('-->',r1,r2.status,r2.reason,'===',headers,'-->',req,'<--',res,'!!!')
        return res
    except Exception as e:
        print ('call fail',e)

parser = argparse.ArgumentParser(description='test a avsc/json conversion')
parser.add_argument('--json')
parser.add_argument('--schema', metavar='path', required=False, help='relative path to schema')
args = parser.parse_args()

if args.json:
    try:
        f = open(args.json)
        data = json.load(f)
        schema = avro.schema.parse(open(args.schema, "rb").read())
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data,encoder)
        print ('all good')
    except Exception as e:
        print ('convert fail',e)

    sys.exit()

print ('starting')

pid()

global consumer
global producer
global schemas

schemas = {}
schemas['ocsServiceRequest']='ocs-service-request.avsc'
schemas['ocsServiceResponse']='ocs-service-response.avsc'
schemas['ocsNotificationServiceRequest']='ocs-service-notification-request.avsc'
schemas['ocsNotificationServiceResponse']='ocs-service-notification-response.avsc'
schemas['scpServiceRequest']='scp-service-request.avsc'
schemas['scpServiceResponse']='scp-service-response.avsc'

topics = {}
topics['ocsServiceRequest']='ocsServiceResponse'
topics['ocsNotificationServiceRequest']='ocsNotificationServiceResponse'
topics['scpServiceRequest']='scpServiceResponse'

time.sleep(2)
print("connecting to web server responder:8099")
conn = httplib.HTTPConnection("responder:8099")
r1 = conn.request("GET", "/hello")
r2 = conn.getresponse()
res = r2.read()
conn.close()
print("succesfully connected to wiremock web server")

hostName = "0.0.0.0"
serverPort = 8081

class MyServer(BaseHTTPRequestHandler):
    def do_POST(self):
        print("doPost", )
        key = self.headers.get('X-Key')
        topic = self.headers.get('X-Topic')
        print("GOT A POST", key, topic)
        len = self.headers.get('content-length')
        res = self.rfile.read(int(len))
        print("PAYLOAD", res)
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        reply(key, res, topic)

def start():
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
 
 
if __name__ =="__main__":
    # creating thread
    t1 = threading.Thread(target=start, args=())
    t1.start()

while True:
    try:
        print ('run')
        run()
        time.sleep(3)
    except Exception as e:
        print ('---',e)
        time.sleep(5)

t1.join()


    #try:
    #finally:
    #    os.unlink(pidfile)

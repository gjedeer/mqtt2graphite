#!/usr/bin/env python3

__author__ = "Jan-Piet Mens"
__copyright__ = "Copyright (C) 2013 by Jan-Piet Mens"

import paho.mqtt.client as paho
import ssl
import os, sys
import logging
import logging.handlers
import time
import socket
import json
import signal

MQTT_HOST = os.environ.get('MQTT_HOST', '127.0.0.1')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
CARBON_SERVER = os.environ.get('CARBON_SERVER', '192.168.2.43')
CARBON_PORT = int(os.environ.get('CARBON_PORT', 2003))
SYSLOG_HOST = os.environ.get('SYSLOG_HOST', None)
SYSLOG_PORT = int(os.environ.get('SYSLOG_PORT', 1514))

LOGFORMAT = 'mqtt2graphite %(asctime)-15s %(message)s'

PREFIX = 'tasmota'

DEVICES = ('pompa', ) # "topic" from tasmota

DEBUG = os.environ.get('DEBUG', False)
if DEBUG:
    logging.basicConfig(level=logging.DEBUG, format=LOGFORMAT)
else:
    logging.basicConfig(level=logging.INFO, format=LOGFORMAT)

if SYSLOG_HOST:
    logger = logging.getLogger()
    handler = logging.handlers.SysLogHandler(address=(SYSLOG_HOST, SYSLOG_PORT))
    logger.addHandler(handler)

client_id = "MQTT2Graphite_%d-%s" % (os.getpid(), socket.getfqdn())

def cleanup(signum, frame):
    '''Disconnect cleanly on SIGTERM or SIGINT'''

    mqttc.publish("/clients/" + client_id, "Offline")
    mqttc.disconnect()
    logging.info("Disconnected from broker; exiting on signal %d", signum)
    sys.exit(signum)


def is_number(s):
    '''Test whether string contains a number (leading/traling white-space is ok)'''

    try:
        float(s)
        return True
    except ValueError:
        return False


def on_connect(mosq, userdata, flags, rc):
    logging.info("Connected to broker at %s as %s" % (MQTT_HOST, client_id))

    mqttc.publish("/clients/" + client_id, "Online")

    for device in DEVICES:
        for topic in (
                'tele/%s/SENSOR' % device,
                'stat/%s/POWER' % device,
                ):
            logging.debug("Subscribing to topic %s" % topic)
            mqttc.subscribe(topic, 0)

def on_message(mosq, userdata, msg):
    try:
        lines = []
        now = int(time.time())
        message = ''

        logging.debug(msg.topic)
        logging.debug(msg.payload)

        parts = msg.topic.split('/')
        device = parts[1]

        if msg.topic.endswith('/POWER'):
            message = '%s.%s.on %d %d\n' % (PREFIX, device, 1 if msg.payload == b'ON' else 0, now)
        elif msg.topic.endswith('/SENSOR'):
            payload = json.loads(msg.payload.decode('utf8'))
            energy = payload['ENERGY']
            for k, v in energy.items():
                if not is_number(v):
                    continue
                message += '%s.%s.%s %s %d\n' % (PREFIX, device, k, v, now)


        logging.debug("%s", message)

        sock = socket.socket()
        sock.connect((CARBON_SERVER, CARBON_PORT))
        sock.sendall(message.encode('utf8'))
        sock.close()
    except Exception as e:
        import traceback
        logging.warning(traceback.print_exc())
  
def on_subscribe(mosq, userdata, mid, granted_qos):
    pass

def on_disconnect(mosq, userdata, rc):
    if rc == 0:
        logging.info("Clean disconnection")
    else:
        logging.info("Unexpected disconnect (rc %s); reconnecting in 5 seconds" % rc)
        time.sleep(5)

    
def main():
    logging.info("Starting %s" % client_id)
    logging.info("INFO MODE")
    logging.debug("DEBUG MODE")

    userdata = {
    }
    global mqttc
    mqttc = paho.Client(client_id, clean_session=True, userdata=userdata)
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_subscribe = on_subscribe

    mqttc.will_set("clients/" + client_id, payload="Adios!", qos=0, retain=False)

    mqttc.connect(MQTT_HOST, MQTT_PORT, 60)

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    mqttc.loop_forever()

if __name__ == '__main__':
	main()

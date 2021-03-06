#!/usr/bin/env python3
 
import os
import sys
import time
from datetime import datetime
import pytz
import tzlocal
import requests
import urllib.request, urllib.error, urllib.parse
import json
import sqlite3
import configparser
from peewee import *
from temperatureDB import *

import rfmon_commonsensor as rfbase
import paho.mqtt.client as mqtt


DEFAULT_API_KEY = "IjPjyGRBNX4215uvu7sAB86NBjCtklQByFAIb1VoJT2TUeXF"
DEFAULT_FEED_ID = "1785749146"

GRILL_API_KEY   = "JEksVghaisFnIpO6NyQM51ITpVeKZ5K1r8xZEBc934zZtDsl"
GRILL_FEED_ID   = "1130159067"

# ---- Globals ----
IsConnected=False
cnxnRC=-1


SLEEP_SECONDS = 1 

config = None
newSensors = {}  #Global list of our sensors

# ----------------------------------------------------------------
def getConfigExt( configSysIn, sectionIn, optionIn, defaultIn=None ):
    optionOut=defaultIn
    if( configSysIn.has_option( sectionIn, optionIn)):
        optionOut = configSysIn.get(sectionIn, optionIn)
    return optionOut

def getConfigExtBool( configSysIn, sectionIn, optionIn, defaultIn=False ):
    optionOut=defaultIn
    if( configSysIn.has_option( sectionIn, optionIn)):
        optionOut = configSysIn.getboolean(sectionIn, optionIn)
    return optionOut


def initSensors( sensorArrayIn ):
#  sensorList = (  
#      ["A1", "WaterHeater", 120, DEFAULT_API_KEY, DEFAULT_FEED_ID, "waterhtr",
#        (["1", "supply", "Supply"], ["2", "return", "Return"], ["3", "volts", "Voltage"]) ]
#    , ["A2", "Aquarium", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "aquarium",
#        (["1", "house", "House"], ["2", "aquarium", "Aquarium"], ["unused", "", ""]) ]
#    , ["C1", "Nathan", 600, DEFAULT_API_KEY, DEFAULT_FEED_ID, "nathan",
#        (["unused", "", ""], ["1", "humidity", "Humidity"], ["2", "C1_temp", "C1_Temp"]) ]
#    , ["C2", "housetemp", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "house",
#        (["1", "house", "House"], ["2", "hvac", "HVAC"], ["3", "volts", "Voltage"]) ] 
#    , ["C3", "TempUnitC3", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunitC3",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ] 
#    , ["C4", "TestUnit", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "testunit",
#        (["1", "temp", "TestTemp"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
#    , ["C5", "Plant",  1800, DEFAULT_API_KEY, DEFAULT_FEED_ID, "plant",
#        (["1", "water", "WaterLevel"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
#    , ["D1", "Freezer", 285, DEFAULT_API_KEY, DEFAULT_FEED_ID, "freezer",
#        (["1", "garagetemp", "GarageTemp"], ["2", "temp", "FreezerTemp"], ["3", "volts", "Voltage"]) ]
#    , ["E1", "TempUnit1", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit1",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
#    , ["E2", "TempUnit2", 60, DEFAULT_API_KEY, DEFAULT_FEED_ID, "testunit2",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
#    , ["E3", "TempUnit3", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit3",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
#    , ["E4", "TempUnit4", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit4",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ]
#    , ["E5", "TempUnit5", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit5",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ]
#    , ["E6", "TempUnit6", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit6",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ]
#    , ["E7", "TempUnit7", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit7",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ]
#    , ["F1", "Grill", 60, GRILL_API_KEY, GRILL_FEED_ID, "grill",
#        (["1", "pittemp", "PitTemp"], ["2", "food1temp", "Food1Temp"], ["3", "food2temp", "Food2Temp"]) ]
#    , ["O1", "RasPi", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "raspi",
#        (["1", "outsidetemp", "OutsideTemp"], ["unused", "", ""], ["unused", "", ""]) ]
#    , ["D2", "Xenon6D", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "xenon6D",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["unused", "", ""]) ]
#    , ["51", "Xenon51", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "xenon51",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["unused", "", ""]) ]
#    , ["AE", "XenonAE", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "xenonAE",
#        (["1", "temp", "Temp"], ["unused", "", ""], ["unused", "", ""]) ]
#               ) 

  sensorList=[]

  for sensor in Xmitter.select():
    metricList=[]
    for metric in XmitterMetric.select().where(XmitterMetric.node == sensor).order_by(XmitterMetric.metricnum):
      metricList.append( [metric.metricnum, metric.metricguid, metric.metricid] )

    sensorRec=[sensor.nodeid, sensor.nodedescription, sensor.nodepubinterval, sensor.nodename]
    sensorRec.append( metricList )
    sensorList.append( sensorRec )

  for y in sensorList:
    x = rfbase.rfmon_BASE( y[0], y[1], y[2], DEFAULT_API_KEY, DEFAULT_FEED_ID, y[3], y[4] )
    sensorArrayIn[x.getTransmitterID()] = x

#  for y in sensorArrayIn:
#    print(y)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    global IsConnected,cnxnRC
    print(("CB: Connected;rtn code [%d]"% (rc) ))
    cnxnRC=rc
    if( rc == 0 ):
        IsConnected=True
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe( config.get("DEFAULT", 'topic') )

def on_disconnect(client, userdata, rc):
    global IsConnected
    print(("CB: Disconnected with rtn code [%d]"% (rc) ))
    IsConnected=False

def on_log(client, userdata, level, buf):
    print(("log: ",buf))

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    myDateTime = datetime.utcnow().replace(tzinfo=pytz.utc);

    #  Get message from queue
    recv_string=msg.payload.decode()  # Convert from ByteArray into String with decode()
    if( getConfigExtBool(config, "DEFAULT", 'debug') ): 
        print("Received msg [%s:%s]" % (msg.topic, recv_string))

    nodeID = rfbase.getNodeIDFromMsgString(recv_string)
    if( nodeID not in newSensors ):
        print("No match found for Node [{0}]".format(nodeID))
        sys.stdout.flush()
        return

    n = newSensors[nodeID]
    nodeID, seq, tempList = n.parseMsgString( recv_string )  # Get info from packet
    print(("[ %s - %-12.12s ] %3.3s - %s : ") % (nodeID, n.getTransmitterName(), seq, myDateTime.astimezone(tzlocal.get_localzone()).strftime("%Y-%m-%d %H:%M:%S")), end=' ')
    print(tempList, end=' ')

    parms = n.getSensorParms()  # Get info from Sensor class

    doPublish = False
    metricsString= ""
    numTemps = len(tempList)
    for x in range(numTemps):
        _metricguid = parms[x][1]
        _metricname = parms[x][2]
        _metric     = tempList[x]

        if( _metric > 990 or parms[x][0] == "unused" ):
            continue

        try:
            # Write record using PeeWeeORM to SQLite
            myNode = Xmitter.get( Xmitter.nodeid == nodeID )
            newReading = Reading()
            newReading.node=myNode
            newReading.guid=_metricguid
            newReading.value=_metric
            newReading.seqnum=seq
            newReading.dt=myDateTime
            newReading.save()
        except:
            print("Error Inserting to DB!")

        if( n.needsPublishing(myDateTime) ):
	    # GroveStream metric string build-up
            metricsString += "&"+_metricguid+"="+str(tempList[x])
            doPublish = True
			
    if( doPublish ):
        print("- Updating")

        # GroveStream push for all streams for a node (component)
        try:
            url = config.get("DEFAULT",'grovestreams_url')+"&seq="+str(seq)+"&compId="+n.getComponentID()+metricsString
            #print url
            urlhandle = urllib.request.urlopen(url) 
            urlhandle.close() 
        except( requests.exceptions.ConnectionError, requests.HTTPError, urllib.error.URLError) as e:
            print("Error updating GroveStreams with RF Data!!({0}): {1}".format(e.errno, e.strerror))

        n.markPublished(myDateTime)
        doPublish = False
    else:
        print("- TOO SOON") 
    sys.stdout.flush()

# main program entry point - runs continuously updating our datastream with the
def run(client):
    initSensors( newSensors ) 

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    if( getConfigExtBool(config, "DEFAULT", 'qlog_enable') ):
        client.on_log = on_log
    if( getConfigExt(config, "DEFAULT", 'user', None) and getConfigExt(config, "DEFAULT", 'pswd', None) ):
        print( "Setting User and pswd")
        client.username_pw_set( config.get("DEFAULT", 'user'), config.get("DEFAULT", 'pswd') )

    client.connect(config.get("DEFAULT", 'broker'))
    retry=0
    while( (not IsConnected) and cnxnRC == -1 and retry <= 10):
        print("Waiting for Connect")
        time.sleep(.05)
        client.loop()
        retry += 1
    if( not IsConnected ):
        print(("No connection could be established: rc[%d]") % cnxnRC)
        return
    sys.stdout.flush()
 
    while( True ):
        try:
            client.loop_forever()
        finally:
            print("Dropped out of MQ loop_forever with exception.  Continuing.")
            pass
    

# -------------------------------------

client = mqtt.Client()

try:
    configFile=os.path.splitext(__file__)[0]+".conf"
    if( not os.path.isfile( configFile )): 
        print(( "Config file [%s] was not found.  Exiting" ) % configFile)
        exit()

    config = configparser.SafeConfigParser()
    config.read(configFile)
    if( getConfigExtBool(config, "DEFAULT", 'debug') ): 
        print(("Using config file [%s]") % configFile)
    sys.stdout.flush()

    db.init( config.get("DEFAULT",'dblocation') )
    db.connect()

    run(client)
except KeyboardInterrupt:
    print("Keyboard Interrupt...")
finally:
    print("Exiting.")
    db.close()
    time.sleep(.25)
    client.disconnect()
    while( IsConnected ):
        print("Waiting for Disconnect")
        time.sleep(.05)
        client.loop()



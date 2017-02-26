#!/usr/bin/env python
 
import os
import sys
import time
from datetime import datetime
import pytz
import tzlocal
import requests
import urllib2
import json
import sqlite3
import ConfigParser

import rfmon_commonsensor as rfbase
import paho.mqtt.client as mqtt


DEFAULT_API_KEY = "IjPjyGRBNX4215uvu7sAB86NBjCtklQByFAIb1VoJT2TUeXF"
DEFAULT_FEED_ID = "1785749146"

GRILL_API_KEY   = "JEksVghaisFnIpO6NyQM51ITpVeKZ5K1r8xZEBc934zZtDsl"
GRILL_FEED_ID   = "1130159067"

# ---- Globals ----
IsConnected=False
IsCnxnErr=False

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
  sensorList = (  
      ["C2", "housetemp", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "house",
        (["1", "house", "House"], ["2", "hvac", "HVAC"], ["3", "volts", "Voltage"]) ] 
    , ["F1", "Grill", 60, GRILL_API_KEY, GRILL_FEED_ID, "grill",
        (["1", "pittemp", "PitTemp"], ["2", "food1temp", "Food1Temp"], ["3", "food2temp", "Food2Temp"]) ]
    , ["C1", "Nathan", 600, DEFAULT_API_KEY, DEFAULT_FEED_ID, "nathan",
        (["unused", "", ""], ["1", "humidity", "Humidity"], ["2", "C1_temp", "C1_Temp"]) ]
    , ["C3", "Freezer", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "freezer",
        (["1", "garagetemp", "GarageTemp"], ["2", "temp", "FreezerTemp"], ["3", "volts", "Voltage"]) ]
    , ["C5", "Plant",  1800, DEFAULT_API_KEY, DEFAULT_FEED_ID, "plant",
        (["1", "water", "WaterLevel"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
    , ["C4", "TestUnit", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "testunit",
        (["1", "temp", "TestTemp"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
    , ["E1", "TempUnit1", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit1",
        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
    , ["E2", "TempUnit2", 60, DEFAULT_API_KEY, DEFAULT_FEED_ID, "testunit2",
        (["1", "temp", "TestTemp2"], ["2", "temp2", "TestTemp2b"], ["3", "volts", "Voltage"] ) ]
    , ["E3", "TempUnit3", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit3",
        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"] ) ]
    , ["E4", "TempUnit4", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit4",
        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ]
    , ["E5", "TempUnit5", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit5",
        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ]
    , ["E6", "TempUnit6", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit6",
        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ]
    , ["E7", "TempUnit7", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "tempunit7",
        (["1", "temp", "Temp"], ["unused", "", ""], ["3", "volts", "Voltage"]) ]
    , ["O1", "RasPi", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "raspi",
        (["1", "outsidetemp", "OutsideTemp"], ["unused", "", ""], ["unused", "", ""]) ]
    , ["A1", "WaterHeater", 120, DEFAULT_API_KEY, DEFAULT_FEED_ID, "waterhtr",
        (["1", "supply", "Supply"], ["2", "return", "Return"], ["3", "volts", "Voltage"]) ]
    , ["A2", "Aquarium", 300, DEFAULT_API_KEY, DEFAULT_FEED_ID, "aquarium",
        (["1", "house", "House"], ["2", "aquarium", "Aquarium"], ["unused", "", ""]) ]
               ) 

  for y in sensorList:
    x = rfbase.rfmon_BASE( y[0], y[1], y[2], y[3], y[4], y[5], y[6] )
    sensorArrayIn[x.getTransmitterID()] = x


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    global IsConnected,IsCnxnErr
    print("CB: Connected;rtn code [%d]"% (rc) )
    if( rc == 0 ):
        IsConnected=True
    else:
        IsCnxnErr=True
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe( config.get("DEFAULT", 'topic') )

def on_disconnect(client, userdata, rc):
    global IsConnected
    print("CB: Disconnected with rtn code [%d]"% (rc) )
    IsConnected=False

def on_log(client, userdata, level, buf):
    print("log: ",buf)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    myDateTime = datetime.utcnow().replace(tzinfo=pytz.utc);

    #  Get message from queue
    recv_string=str(msg.payload)
    print "Received msg [%s:%s]" % (msg.topic, recv_string)

    nodeID = rfbase.getNodeIDFromMsgString(recv_string)
    #print "Node: [%s]" % nodeID
    if( nodeID not in newSensors ):
        print "No match found for Node {0}".format(nodeID)
        return

    n = newSensors[nodeID]
    nodeID, seq, tempList = n.parseMsgString( recv_string )  # Get info from packet
    print "[", nodeID, "-", n.getTransmitterName(), "]", seq, "-", myDateTime.astimezone(tzlocal.get_localzone()).strftime("%Y-%m-%d %H:%M:%S %Z"), ": ", tempList,  # trailing comma says no NEWLINE

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

        # SQLITE DB write
        try:
	    conn = sqlite3.connect(config.get("DEFAULT",'dblocation'))
	    curs = conn.cursor()
	    curs.execute("INSERT INTO rawdata (nodeid, seqnum, metricid, metricguid, metricname, metric, metricdt) VALUES (?,?,?,?,?,?,?) ", \
			( nodeID, seq, parms[x][0], _metricguid, _metricname, _metric, myDateTime))
	    conn.commit()  #done by the 'with' statement
	    curs.close() ;
        except( sqlite3.OperationalError ) as e:
            print "Error Inserting to DB!({0}:".format(e)
        finally:
            conn.close()

        if( n.needsPublishing(myDateTime) ):
	    # GroveStream metric string build-up
	    metricsString += "&"+_metricguid+"="+str(tempList[x])
            doPublish = True
			
    if( doPublish ):
        print "- Updating"

        # GroveStream push for all streams for a node (component)
        try:
            url = config.get("DEFAULT",'grovestreams_url')+"&seq="+str(seq)+"&compId="+n.getComponentID()+metricsString
            #print url
	    urlhandle = urllib2.urlopen(url) 
	    urlhandle.close() 
        except( requests.exceptions.ConnectionError, requests.HTTPError, urllib2.URLError) as e:
            print "Error updating GroveStreams with RF Data!!({0}): {1}".format(e.errno, e.strerror)

        n.markPublished(myDateTime)
        doPublish = False
    else:
        print "- TOO SOON" 

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

    client.connect(config.get("DEFAULT", 'broker'), config.get("DEFAULT", 'port'), 60)
    retry=0
    while( (not IsConnected) and (not IsCnxnErr) and retry <= 10):
        print("Waiting for Connect")
        time.sleep(.05)
        client.loop()
        retry += 1
    if( not IsConnected or IsCnxnErr ):
        print("No connection could be established")
        return


    sys.stdout.flush()
 
    client.loop_forever()

# -------------------------------------

client = mqtt.Client()

try:
    configFile=os.path.splitext(__file__)[0]+".conf"
    if( not os.path.isfile( configFile )): 
        print( "Config file [%s] was not found.  Exiting" ) % configFile
        exit()

    config = ConfigParser.SafeConfigParser()
    config.read(configFile)
    if( getConfigExtBool(config, "DEFAULT", 'debug') ): 
        print("Using config file [%s]") % configFile

    run(client)
except KeyboardInterrupt:
    print "Keyboard Interrupt..."
finally:
    print "Exiting."

    time.sleep(.25)
    client.disconnect()
    while( IsConnected ):
        print("Waiting for Disconnect")
        time.sleep(.05)
        client.loop()



/*	Collector.js - collects data from TTN / Mosquitto 
			and stores it into mongodb
*/

global.debug=true;

function debugMsg(msg, msg2, msg3) {
  if(global.debug) {
    console.log("Debug: ",msg,msg2,msg3);
  } 
};

const appEUI = "70B3D57ED00004CD";
const mqtt = require('mqtt');
const client = mqtt.connect({"host":'staging.thethingsnetwork.org', "port":1883, "username": appEUI, "password": 'rZym0z0YUPU+BwqXS6e+GNIvAXQUaPbGhg2DNkGmsoA='});

var connected = false;
client.on('connect', () => {  
  client.subscribe('70B3D57ED00004CD/devices/+/up');
  client.subscribe('70B3D57ED00004CD/connected');
});

client.on('message', (topic, message) => {  
  if(topic === 'garage/connected') {
    connected = (message.toString() === 'true');
  }
  var readableMsg= new Buffer(message, 'base64').toString("ascii");
  debugMsg("Message received: ",topic, readableMsg);
  
  //Now lets get our bit out - payload
  var parse = require('csv-parse');
  var jsonMsg = JSON.parse(readableMsg);
  var sensorId = jsonMsg.dev_eui;
  var payload = new Buffer(jsonMsg.payload, 'base64').toString("ascii");
  debugMsg("Payload is ", payload);
  parse(payload, {comment: '#'}, function(err, output) {

    var newStats= { "sensorId": sensorId, "people": Number(output[0][0]),
      "meanSnd": Number(output[0][1]), "peakSnd": Number(output[0][2]), 
      "hum": Number(output[0][3]), "temp": Number(output[0][4]), 
      "dur": Number(output[0][5]), "totPeople": Number(output[0][6]),
      "timestamp": new Date()
      };

    debugMsg("Will attempt to store ", newStats);
    // Set up mongodb

    var mongodb = require('mongodb');       // use the mongodb native drivers
    var MongoClient = mongodb.MongoClient;  // MongoClient talks to db server
    var assert = require('assert');
    var ObjectId = require('mongodb').ObjectID;
    
    var url = 'mongodb://localhost:27017/hotspotdb';
    
    MongoClient.connect(url, function (err,db) {
      if(err) {
        debugMsg('Failed to connect to mongodb server. Error:', err);
      } else {
        debugMsg('Connected ok to ', url);
      }
      // Attempt to insert the document
      
      db.collection('sensorData').insertOne(newStats);
      db.close();

    });
  });
});


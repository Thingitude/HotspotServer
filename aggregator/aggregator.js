/*	aggregator.js - Performs the sensor aggregation tasks 
			for the Reading Hotspot project.
			Written in Node.js

	Copyright:	(c) 2016 Mark Stanley, Coraledge Ltd.

	License:	You may use or modify this code as
			you like, but you must include these
			comments at the top of the file.
*/

// Helpful functions to get us day of the year (1-365 / 366)

// First up - is it a leap year?

function daysInFebruary(year) {
    if(year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)) {
        // Leap year
        return 29;
    } else {
        // Not a leap year
        return 28;
    }
}

// Now the day of year calculation

function dateToDay(date) {
    var feb = daysInFebruary(date.getFullYear());
    var daysMonth = [0, // January
                     31, // February
                     31 + feb, // March
                     31 + feb + 31, // April
                     31 + feb + 31 + 30, // May
                     31 + feb + 31 + 30 + 31, // June
                     31 + feb + 31 + 30 + 31 + 30, // July
                     31 + feb + 31 + 30 + 31 + 30 + 31, // August
                     31 + feb + 31 + 30 + 31 + 30 + 31 + 31, // September
                     31 + feb + 31 + 30 + 31 + 30 + 31 + 31 + 30, // October
                     31 + feb + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31, // Nov
                     31 + feb + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30, // December
                   ];
    return daysMonth[date.getMonth()] + date.getDate();
}


var aggregateSensorStats = function(db, callback) {

  /* We want a set of documents formatted as follows:

     { sensorId, venueId, year, monthOfYear, dayOfYear, dayOfMonth, dayOfWeek,
       avgPeople, maxPeople, totalPeople, avgDuration, 
       avgTemp, maxTemp, avgHum, maxHum, avgVol, maxVol }
  */

  var sensorData=db.collection('sensorData');
    
  // Lets get the date variables defined
  var today=new Date();
  var thisYear=today.getFullYear();
  var thisMonth=today.getMonth() + 1;		// month no 1 to 12
  var thisDayOfMonth=today.getDate();
  var thisDayOfWeek=today.getDay();		// day no 0(Sun) to 6
  var thisDayOfYear=dateToDay(today);
  //var thisDayOfYear=224;			// just for debugging

  // And lets log what we've got
  console.log("Aggregating for ", thisDayOfMonth, "-", thisMonth, "-", thisYear, ", day of week: ", thisDayOfWeek, " and day of year: ", thisDayOfYear);

  // Time to get aggregating

  sensorData.aggregate([ { 
    $project: { 
      sensorId: "$sensorId", people: "$people", dur: "$dur", totPeople: "$totPeople",
      year: { $year: "$timestamp" }, month: thisMonth, 
      dayOfMonth: thisDayOfMonth, dayOfWeek: thisDayOfWeek, 
      dayOfYear: { $dayOfYear: "$timestamp" } 
    } 
  }, {
    $match: { dayOfYear: thisDayOfYear, year: thisYear } 
  }, {
    $group: { 
      _id: "$sensorId", 
      year: { $first: thisYear }, 
      month: {$first: thisMonth}, dayOfMonth: {$first: thisDayOfMonth },
      dayofWeek: {$first: thisDayOfWeek }, 
      dayOfYear: { $first: thisDayOfYear },
      avgPeople: { $avg: "$people" }, maxPeople: { $max: "$people" }, 
      totalPeople: { $max: "$totPeople" }, avgDuration: { $avg: "$dur"} 
    }
  } ]).toArray( function (err,aggregates) {
    if(err) {
      console.log('Failed to get aggregates. Error: ',err);
    } else {
      console.log('Aggregation completed successfully.');
    }
    callback(aggregates);
   
  });
};

// Set up mongodb

var mongodb = require('mongodb');	// use the mongodb native drivers
var MongoClient = mongodb.MongoClient;	// MongoClient talks to db server
var assert = require('assert');
var ObjectId = require('mongodb').ObjectID;

var url = 'mongodb://localhost:27017/hotspotdb';

MongoClient.connect(url, function (err,db) {
  if(err) {
    console.log('Failed to connect to mongodb server. Error:', err);
  } else {
    console.log('Connected ok to ', url);
  }
  // Call the function to run the aggregation

  aggregateSensorStats(db, function(aggregates) {
    var dailyAggregates=db.collection('dailyAggregates');
    // Lets get the date variables defined
    var today=new Date();
    var thisYear=today.getFullYear();
    var thisMonth=today.getMonth() + 1;		// month no 1 to 12
    var thisDayOfMonth=today.getDate();
    var thisDayOfWeek=today.getDay();		// day no 0(Sun) to 6
    var thisDayOfYear=dateToDay(today);
    //var thisDayOfYear=221;			// just for debugging

    console.log("Aggs is ", aggregates);
    for(doc in aggregates) {
      dailyAggregates.insertOne({
        "sensorId": aggregates[doc]._id,
        "year": thisYear, "month": thisMonth, "dayOfYear": thisDayOfYear,
        "dayOfMonth": thisDayOfMonth, "dayOfWeek": thisDayOfWeek, 
        "avgPeople": aggregates[doc].avgPeople, 
        "maxPeople": aggregates[doc].maxPeople, 
        "totalPeople": aggregates[doc].totalPeople, 
        "avgDuration": aggregates[doc].avgDuration 
      });
    }
    db.close();
    console.log('Disconnected from ', url);
  });
});


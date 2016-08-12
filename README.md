# HotspotServer
Node.js code for the Hotspot Server
Key files are:
collector.js - listens on Mosquitto for messages from Hotspot sensors (via The Things Network), and stores data in mongodb.
               Is run as a service on the server - so constantly running.
               
aggregator.js - scheduled job (crontab) run once a day.  Aggregates all the days stats on mongodb into a document per sensor
                for the day.
                

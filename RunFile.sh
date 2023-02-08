#!/bin/bash
echo "Starting Program"
mongod --port 27017 --syslog --fork --dbpath /tmp/ --bind_ip localhost
python3 IntroScript.py --type cyber --folder /cyber/ --filebase 20221220120014.record. && python3 datainsert.py --config cybersettings.json
echo "Program Finished"

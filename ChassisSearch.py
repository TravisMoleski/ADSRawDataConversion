class ChassisSearch:
    
    def __init__(self):
        
        ### VAR INIT ###
        self.auto_times = []
        self.query = {'topic': '/apollo/canbus/chassis'}
        self.chassis_data = []
        
    def mongodbChassisSearch(self, db_data):
        
        print(f"Downloading chassis data {self.query}...")
        
        if db_data.find_one(self.query) is not None:
            
            cursor = db_data.find(self.query)
            
            for data in cursor:
                
                timestamp = float(data['header']['timestampSec'])
                drivestate = data['drivingMode']
                speed = float(data['speedMps'])
                steer_rate = float(data['steeringRate'])
                steeringPercentage = float(data['steeringPercentage'])
                throttlePercentage = float(data['throttlePercentage'])
                brakePercentage = float(data['brakePercentage'])
                
                self.chassis_data.append((timestamp, drivestate, speed, steer_rate, steeringPercentage, throttlePercentage, brakePercentage))

        self.chassis_data = sorted(self.chassis_data, key= lambda x: x[0])
            
        print('Chassis data donwloaded from MongoDB.')
        
        return self.chassis_data
        
    
    def disengagmentSearch(self):
        
        ### how this works ###
        # GOAL: GET START AND END TIMES FOR AUTONOMOUS DRIVING!
        # For each row in the sorted chassis data, it checks the drive state. 
        # If the drivestate is COMPLETE_AUTO_DRIVE, it checks to see if the is_auto is False.
            # If false, it turns the auto state to true.
            # Then, it sets the auto_time_start equal to the timestamp. This gives the starting point for the autonomous driving and endpoint for manual driving.
        # IF is_auto IS TRUE, IT SIMPLY SKIPS THE ROW! 
        # If the drivestate is COMPLETE_MANUAL, it checks to see if the is_auto is True
            # If is_auto is True, it turns the auto state to false.
            # Then, it sets the auto_time_end equal to the timestamp. This gives the ending point for the autonomous driving and the start point for manual driving.
        # The goal is to get the start and end times for when the van is in autonomous drive mode.
        
        print('Searching for disengagments...')
        
        # Initializes the drive state
        is_auto = False
        
        for row in self.chassis_data:
            
            timestamp, drivestate, speed, steer_rate, steeringPercentage, throttlePercentage, brakePercentage = row  
                   
            if drivestate == 'COMPLETE_AUTO_DRIVE' and is_auto is False:
                
                is_auto = True
                auto_time_start = timestamp
                
            elif drivestate == 'COMPLETE_MANUAL' and is_auto is True:
                
                is_auto = False
                auto_time_end = timestamp
                self.auto_times.append((auto_time_start, auto_time_end))

        return self.auto_times
            
        
    def csvAutoTimesExport(self):
        
        import time, csv
        
        filename = str(round(time.time())) + '_autotimes_check.csv'
        
        # Open the CSV file in write mode
        with open(filename, 'w', newline='') as csvfile:
            
            # Create a CSV writer object
            csv_writer = csv.writer(csvfile)

            # Write the header
            header = ["START", "END"]
            csv_writer.writerow(header)

            # Write the data
            csv_writer.writerows(self.auto_times)

        print(f"Auto to Manual timestamp data exported to {filename}")


    def csvChassisExport(self):
        
        import time, csv
        
        filename = str(round(time.time())) + '_chassis_check.csv'
        
        # Open the CSV file in write mode
        with open(filename, 'w', newline='') as csvfile:
            
            # Create a CSV writer object
            csv_writer = csv.writer(csvfile)

            # Write the header
            header = ["Timestamp", "DriveState", "Speed", "SteerRate", "SteeringPercentage", "ThrottlePercentage", "BrakePercentage"]
            csv_writer.writerow(header)

            # Write the data
            csv_writer.writerows(self.chassis_data)

        print(f"Chassis topic data exported to {filename}")
        
    
    
    def jsonAutoTimesExport(self):
        
        import json
        
        json_export_filename = str(self.id) + "_.json"
        
        self.auto_start = self.auto_times[0][:]
        self.auto_end = self.auto_times[1][:]
        
        json_export = {
            '_id': self.id,
            'filename': self.filename,
            'foldername': self.foldername,
            'startTime': self.startTime,
            'endTime': self.endTime,
            'msgnum': self.msgnum,
            'size': self.size,
            'topics': self.topics,
            'type': self.type,
            'vehicleID': self.vehicleID,
            'experimentID': self.experimentID,
            'other': self.other,
            'auto_times': self.auto_times,
            'start_auto': self.auto_start,
            'end_auto': self.auto_end
        }
        
        with open(json_export_filename, 'w') as json_file:
            json.dump(json_export, json_file, default=str, indent=4)
            
        print(f"AutoTimes exported to json: {json_export_filename}")
        
    
    def getMetaData(self, db_metadata):
        
        print('Getting meta data...')

        cursor = db_metadata.find()
        idx = 0
        
        # In the event that there are multiple metadata tables uploaded, this just grabs the first. 
        # (There should be only one anyways...?)
        data = cursor[0]

        self.id = data['_id']
        self.filename = data['filename']
        self.foldername = data['foldername']
        self.startTime = data['startTime']
        self.endTime = data['endTime']
        self.msgnum = data['msgnum']
        self.size = data['size']
        self.topics = data['topics']
        self.type = data['type']
        self.vehicleID = data['vehicleID']
        self.experimentID = data['experimentID']
        self.other = data['other']
        
        print('Metadata obtained!')
        
        metadata_base_json = {
            '_id': self.id,
            'filename': self.filename,
            'foldername': self.foldername,
            'startTime': self.startTime,
            'endTime': self.endTime,
            'msgnum': self.msgnum,
            'size': self.size,
            'topics': self.topics,
            'type': self.type,
            'vehicleID': self.vehicleID,
            'experimentID': self.experimentID,
            'other': self.other,
            'auto_times': self.auto_times
        }
        
        return metadata_base_json
import pymongo
import numpy as np
import matplotlib.pyplot as plt
import csv
import time

### OPTIONS ###

# Set how much time in seconds before and after the autonomous driving disengament
dt = 5
    
### GET MONGO DATA ###
### REPLACE WITH DESIRED MONGODB INFO ###
myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017")
mydb = myclient["coll_test_6"]
mycol = mydb["coll_test_6"]
metadID = mydb['metadata'].find_one({'experimentID': 13})




class GetObstacleData:
    
    def __init__(self):
            
        ### VAR INIT ###
        
        self.num_obstacles = []
        self.num_unkn_obs = []
        self.num_pedestrian = []
        self.num_vehicle = []
        self.ts_num_obstacles = []

        ### GET OBSTACLE DATA ###
        
        query = {'topic': '/apollo/perception/obstacles'}
        
        if mycol.find_one(query) is not None:
            
            cursor = mycol.find(query)
            
            for data in cursor:
                
                self.ts_num_obstacles.append(data['header']['timestampSec'])

                temp_unkn = 0
                temp_pedestrian = 0
                temp_vehicle = 0

                if "perceptionObstacle" in data:
                    
                    for obj in range(1,len(data['perceptionObstacle'])):
                        
                        if data['perceptionObstacle'][obj]['type'] == "VEHICLE":
                            
                            temp_vehicle = temp_vehicle + 1
                            
                        elif data['perceptionObstacle'][obj]['type'] == "PEDESTRIAN":
                            
                            temp_pedestrian = temp_pedestrian + 1
                            
                        elif data['perceptionObstacle'][obj]['type'] == "ST_UNKNOWN":
                            
                            temp_unkn = temp_unkn + 1
                                            
                    self.num_obstacles.append(temp_unkn + temp_pedestrian + temp_vehicle)
                    self.num_unkn_obs.append(temp_unkn)
                    self.num_pedestrian.append(temp_pedestrian)
                    self.num_vehicle.append(temp_vehicle)

                else:
                    self.num_obstacles.append(0)
                    self.num_unkn_obs.append(0)
                    self.num_pedestrian.append(0)
                    self.num_vehicle.append(0)
        
        # Export if necessary ???    
        # self.csv_export()

    def csv_export(self):

        self.obstacles_export_csv = {
            'num_obstacles':self.num_obstacles,
            'num_unkn_obs':self.num_unkn_obs,
            'num_pedestrian':self.num_pedestrian,
            'num_vehicle':self.num_vehicle,
            'ts_num_obstacles':self.ts_num_obstacles
        }

        # Export data to csv
        self.obstacles_csv_file = 'obstacles.csv'

        with open(self.obstacles_csv_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.obstacles_export_csv.keys())  # Use data.keys() to specify column names
            writer.writeheader()
            for idx in range(len(self.num_obstacles)):
                row_data = {key: value[idx] for key, value in self.obstacles_export_csv.items()}
                writer.writerow(row_data)

        print(f'Data has been exported to {self.obstacles_csv_file}')
        
        
class ChassisSearch:
    
    def __init__(self):
        
        ### VAR INIT ###
        
        self.auto_times = []
        
        self.query = {'topic': '/apollo/canbus/chassis'}
                
        self.chassis_data = []
        
        
        ### START SEARCH ###
        
        self.mongodbSearch()
        self.autoManualSearch()
        
        
    def mongodbSearch(self):
        
        if mycol.find_one(self.query) is not None:
            
            cursor = mycol.find(self.query)
            
            for data in cursor:
                
                timestamp = data['header']['timestampSec']
                drivestate = data['drivingMode']
                speed = data['speedMps']
                steer_rate = data['steeringRate']
                steeringPercentage = data['steeringPercentage']
                throttlePercentage = data['throttlePercentage']
                brakePercentage = data['brakePercentage']
                
                self.chassis_data.append((timestamp, drivestate, speed, steer_rate, steeringPercentage, throttlePercentage, brakePercentage))

                
        self.chassis_data = sorted(self.chassis_data, key= lambda x: x[0])
        # self.csvChassisExport()
        
    def autoManualSearch(self):
        
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
                
        # self.csvAutoTimesExport()
        # print(self.auto_times)
                
    def csvAutoTimesExport(self):
        
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
    
        
class GetDisengagmentLocation():
    
    def __init__(self, auto_times, dt):
        
        self.auto_times = auto_times
        self.dt = dt
        
        self.latitude = []
        self.longitude = []
        self.latitudeStdDev = []
        self.longitudeStdDev = []
        self.heightStdDev = []
        self.galileoBeidouUsedMask = []
        self.solutionAge = []
        self.extendedSolutionStatus = []
        self.solStatus = []
        self.heightMsl = []
        self.baseStationId = []
        self.numSatsTracked = []
        self.numSatsInSolution = []
        self.solType = []
        self.datumId = []
        self.numSatsL1 = []
        self.differentialAge = []
        self.timestamp = []
        
        self.best_pos_query = {'topic': '/apollo/sensor/gnss/best_pose'}
        # self.odometry_query = {'topic': '/apollo/sensor/gnss/odometry'}
        
        self.getLocation()
        
    def getBestPoseData(self):
        
        if mycol.find_one(self.query) is not None:
            
            cursor = mycol.find(self.query)
            
            for data in cursor:
                 
                self.latitude.append(data['latitude'])
                self.longitude.append(data['longitude'])
                self.latitudeStdDev.append(data['latitudeStdDev'])
                self.longitudeStdDev.append(data['longitudeStdDev'])
                self.heightStdDev.append(data['heightStdDev'])
                self.galileoBeidouUsedMask.append(data['galileoBeidouUsedMask'])
                self.solutionAge.append(data['solutionAge'])
                self.extendedSolutionStatus.append(data['extendedSolutionStatus'])
                self.solStatus.append(data['solStatus'])
                self.heightMsl.append(data['heightMsl'])
                self.baseStationId.append(data['baseStationId'])
                self.numSatsTracked.append(data['numSatsTracked'])
                self.numSatsInSolution.append(data['numSatsInSolution'])
                self.solType.append(data['solType'])
                self.datumId.append(data['datumId'])
                self.numSatsL1.append(data['numSatsL1'])
                self.differentialAge.append(data['differentialAge'])
                self.timestamp.append(data['header']['timestampSec'])
                
                
        
    def getLocation(self):
        
        for row in self.auto_times:
            
            start_time, end_time = row
            
    
    def csvBestPoseExport(self):
        
        to_bestpos_csv = {
            'latitude': latitude,
            'longitude': longitude,
            'latitudeStdDev': latitudeStdDev,
            'longitudeStdDev': longitudeStdDev,
            'heightStdDev': heightStdDev,
            'galileoBeidouUsedMask': galileoBeidouUsedMask,
            'solutionAge': solutionAge,
            'extendedSolutionStatus': extendedSolutionStatus,
            'solStatus': solStatus,
            'heightMsl': heightMsl,
            'baseStationId': baseStationId,
            'numSatsTracked': numSatsTracked,
            'numSatsInSolution': numSatsInSolution,
            'solType': solType,
            'datumId': datumId,
            'numSatsL1': numSatsL1,
            'differentialAge': differentialAge,
            'timestamp': timestamp
        }

        # Export data to csv
        bestpos_csv_file = db_name + '_best_pose_' + str(epoch_time) + '.csv'

        with open(bestpos_csv_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=to_bestpos_csv.keys())  # Use data.keys() to specify column names
            writer.writeheader()
            for idx in range(len(latitude)):
                row_data = {key: value[idx] for key, value in to_bestpos_csv.items()}
                writer.writerow(row_data)

        print(f'Data has been exported to {bestpos_csv_file}')

            
            
        
        


if __name__ == '__main__':
    
    print('Starting search for auto -> manual! :D')
    
    auto_times = ChassisSearch()
    
    GetDisengagmentLocation(auto_times, dt)
    
    # GetObstacleData(auto_times)
    
    
    
    
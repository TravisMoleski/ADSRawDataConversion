import time


class GetDisengagmentLocalizationData():
    
    def __init__(self, auto_times, dt):
                
        self.auto_times = auto_times
        self.dt = dt
        
        self.fps = 20
        self.dims = (1360,768)
        self.video_base_name = str(round(time.time())) + "_cyber19_front_6mm_compressed_"
        
        self.best_pos_data = []
        self.localization_data = []
        self.image_data = []
        
        self.grabbed_best_pos_data = []
        self.grabbed_localization_data = []
        self.grabbed_image_data = []
        
        self.autonomous_localization_data = []
        
        self.best_pos_query = {'topic': '/apollo/sensor/gnss/best_pose'}
        self.localization_query = {'topic': '/apollo/localization/pose'}
                
        # Best Pose Data
        # self.getBestPoseData()
        # self.getBestPoseDisengagment()
        
        # Localization Data
        # self.getLocalizationData()
        # self.getLocalizationDisengagment()

        
    def getBestPoseData(self, db_data):
        
        print(f"Getting {self.best_pos_query} data")
        
        if db_data.find_one(self.best_pos_query) is not None:
            
            cursor = db_data.find(self.best_pos_query)
            
            for data in cursor:
                
                timestamp = float(data['header']['timestampSec'])
                latitude = float(data['latitude'])
                longitude = float(data['longitude'])
                latitudeStdDev = float(data['latitudeStdDev'])
                longitudeStdDev = float(data['longitudeStdDev'])
                heightStdDev = float(data['heightStdDev'])
                galileoBeidouUsedMask = data['galileoBeidouUsedMask']
                solutionAge = data['solutionAge']
                extendedSolutionStatus = data['extendedSolutionStatus']
                solStatus = data['solStatus']
                heightMsl = data['heightMsl']
                baseStationId = data['baseStationId']
                numSatsTracked = data['numSatsTracked']
                numSatsInSolution = data['numSatsInSolution']
                solType = data['solType']
                datumId = data['datumId']
                numSatsL1 = data['numSatsL1']
                differentialAge = data['differentialAge']
                
                self.best_pos_data.append((
                    timestamp,
                    latitude,
                    longitude,
                    latitudeStdDev,
                    longitudeStdDev,
                    heightStdDev,
                    galileoBeidouUsedMask,
                    solutionAge,
                    extendedSolutionStatus,
                    solStatus,
                    heightMsl,
                    baseStationId,
                    numSatsTracked,
                    numSatsInSolution,
                    solType,
                    datumId,
                    numSatsL1,
                    differentialAge
                ))
                
        self.best_pos_data = sorted(self.best_pos_data, key= lambda x: x[0])
        
        # DEBUG csv export 
        # self.csvBestPoseExport()
        # print(self.best_pos_data)
        
        print(f"{self.best_pos_query} data pull complete")
        
    def getLocalizationData(self, db_data):
        
        print(f"Getting {self.localization_query} data")
        
        if db_data.find_one(self.localization_query) is not None:
            
            cursor = db_data.find(self.localization_query)
            
            for data in cursor: 
                
                timestamp = float(data['header']['timestampSec'])
                position_x = float(data['pose']['position']['x'])
                position_y = float(data['pose']['position']['y'])
                position_z = float(data['pose']['position']['z'])
                self.localization_data.append((
                    timestamp,
                    position_x,
                    position_y,
                    position_z
                )) 
                
            self.localization_data = sorted(self.localization_data, key = lambda x: x[0])
            
        print(f"{self.localization_query} data pull complete")
        
        return self.localization_data
        
        
    def getBestPoseDisengagment(self):
        
        instance_value = 0

        for row in self.auto_times:

            start_time, end_time = row

            end_time_start = float(end_time) - self.dt
            end_time_end = float(end_time) + self.dt
            
            start_idx_to_grab = min(range(len(self.best_pos_data)),
                            key=lambda i: abs(float(self.best_pos_data[i][0]) - float(end_time_start)))
            
            end_idx_to_grab = min(range(len(self.best_pos_data)),
                            key=lambda i: abs(float(self.best_pos_data[i][0]) - float(end_time_end)))

            
            for idx in range(start_idx_to_grab, end_idx_to_grab):
                
                self.grabbed_best_pos_data.append((
                    instance_value,
                    self.best_pos_data[idx]
                ))
                
            instance_value += 1
            
        return self.grabbed_best_pos_data
    
                    
    def getLocalizationDisengagment(self):
        
        instance_value = 0
        
        for row in self.auto_times:
            
            start_time, end_time = row

            end_time_start = float(end_time) - self.dt
            end_time_end   = float(end_time) + self.dt
            
            start_idx_to_grab = min(range(len(self.localization_data)),
                            key=lambda i: abs(float(self.localization_data[i][0]) - float(end_time_start)))
            
            end_idx_to_grab = min(range(len(self.localization_data)),
                            key=lambda i: abs(float(self.localization_data[i][0]) - float(end_time_end)))
            
            start_idx_to_grab_auto = min(range(len(self.localization_data)),
                            key=lambda i: abs(float(self.localization_data[i][0]) - float(start_time)))
            
            end_idx_to_grab_auto = min(range(len(self.localization_data)),
                            key=lambda i: abs(float(self.localization_data[i][0]) - float(end_time)))

            for idx in range(start_idx_to_grab, end_idx_to_grab):
                self.grabbed_localization_data.append((
                    instance_value,
                    self.localization_data[idx]
                ))
                
            for jdx in range(start_idx_to_grab_auto, end_idx_to_grab_auto):
                self.autonomous_localization_data.append((
                    instance_value,
                    self.localization_data[jdx]
                ))
                
            instance_value += 1
            
        return self.grabbed_localization_data, self.autonomous_localization_data

    def csvBestPoseExport(self):
        
        import csv

        bestpos_csv_file = str(round(time.time())) +  '_bestpos.csv'

        with open(bestpos_csv_file, 'w', newline='') as csvfile:
            
            # Create a CSV writer object
            csv_writer = csv.writer(csvfile)

            # Write the header
            header = [
                "Timestamp", "Latitude", "Longitude", "LatitudeStdDev", "LongitudeStdDev", "HeightStdDev",
                "GalileoBeidouUsedMask", "SolutionAge", "ExtendedSolutionStatus", "SolStatus",
                "HeightMsl", "BaseStationId", "NumSatsTracked", "NumSatsInSolution", "SolType",
                "DatumId", "NumSatsL1", "DifferentialAge" 
            ]
            csv_writer.writerow(header)

            # Write the data
            csv_writer.writerows(self.best_pos_data)

        print(f'Data has been exported to {bestpos_csv_file}')
        
    def csvLocalizationExport(self):
        
        import csv

        localization_csv_file = str(round(time.time())) +  '_localization.csv'

        with open(localization_csv_file, 'w', newline='') as csvfile:
            # Create a CSV writer object
            csv_writer = csv.writer(csvfile)

            # Write the header
            header = [
                "Timestamp", "x", "y", "z"
            ]
            csv_writer.writerow(header)

            # Write the data
            csv_writer.writerows(self.best_pos_data)

        print(f'Data has been exported to {localization_csv_file}')
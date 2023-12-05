from databaseinterface import DatabaseDynamo, DatabaseMongo
import pyprog
import logging
import time
import uuid
import math

class breakup_lidar():
    
    def __init__(self) -> None:
        
        # Lidar data is broken up into equal sized chunks
        self.max_num_points_in_chunk = 5000
        
        # print("Entered breakup lidar init")
        
    
    def breakup(self, newitem, dbobject):
        
        # print("Entered breakup lidar breakup func")
                
        # Temporary bin for holding the LiDAR points
        self.lidar_to_db = newitem['point']
        # print(len(self.lidar_to_db))
        
        # Eliminate NaNs, 0s
        self.lidar_to_db = [point for point in self.lidar_to_db if all(math.isfinite(point[key]) and point[key] != 0 for key in ['x', 'y', 'z', 'intensity'])]
        # print(len(self.lidar_to_db))
         
        for start in range(0, len(self.lidar_to_db), self.max_num_points_in_chunk):
            
            end = start + self.max_num_points_in_chunk
            
            chunk = {'point': self.lidar_to_db[start:end]}
                        
            newitem['point'] = chunk
            
            dbobject.db_insert_main(newitem)
        
        
        
        time.sleep(10)
        
        pass
    
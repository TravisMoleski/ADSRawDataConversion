# from databaseinterface import DatabaseDynamo, DatabaseMongo

class breakup_lidar():
    
    def __init__(self) -> None:
        
        # Lidar data is broken up into equal sized chunks
        self.max_num_points_in_chunk = 5000
        
        # print("Entered breakup lidar init")
        
    
    def breakup(self, newitem, dbobject):
        
        import math
        
        # print("Entered breakup lidar breakup func")
                
        # Temporary bin for holding the LiDAR points
        self.lidar_to_db = newitem['point']
        # print(len(self.lidar_to_db))
        
        # Eliminate NaNs, 0s
        self.lidar_to_db = [point for point in self.lidar_to_db if all(math.isfinite(point[key]) and not math.isinf(point[key]) and point[key] != 0 for key in ['x', 'y', 'z', 'intensity'])]
        # print(len(self.lidar_to_db))
         
        for start in range(0, len(self.lidar_to_db), self.max_num_points_in_chunk):
            
            end = start + self.max_num_points_in_chunk
            
            chunk = {'point': self.lidar_to_db[start:end]}
                        
            newitem['point'] = chunk
            
            dbobject.db_insert_main(newitem)
        
        pass
    
class breakup_uncompressed_image():

        
    def __init__(self) -> None:
        
        # Number of sub-images
        self.str_subdivisions = 25
        
        
    def breakup(self, newitem, dbobject):

        self.data_str_to_db = newitem['data']
        
        self.chunk_size = len(self.data_str_to_db) // self.str_subdivisions
        
        image_string_idx = 0
        
        for idx in range(0, len(self.data_str_to_db), self.chunk_size):
            
            chunk = self.data_str_to_db[idx:idx + self.chunk_size]
                
            newitem['data'] = chunk
            
            newitem['image_string_idx'] = image_string_idx
            image_string_idx += 1
            
            dbobject.db_insert_main(newitem)


class breakup_compressed_image():

        
    def __init__(self) -> None:
        
        # Number of sub-images
        self.str_subdivisions = 3
        
        
    def breakup(self, newitem, dbobject):

        self.data_str_to_db = newitem['data']
        
        self.chunk_size = len(self.data_str_to_db) // self.str_subdivisions
        
        image_string_idx = 0
        
        for idx in range(0, len(self.data_str_to_db), self.chunk_size):
            
            chunk = self.data_str_to_db[idx:idx + self.chunk_size]
                
            newitem['data'] = chunk
            
            newitem['image_string_idx'] = image_string_idx
            image_string_idx += 1
            
            dbobject.db_insert_main(newitem)
            
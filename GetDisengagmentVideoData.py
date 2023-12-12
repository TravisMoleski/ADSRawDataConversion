import pymongo
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import io
import cv2
import base64 
import time
import json
import copy
import os
from bson import ObjectId
from datetime import datetime

class DesengagmentVideoExporter():
    
    def __init__(self):
        
        self.show_video = False
        # self.json_export_folder = ????? - currently just the current folder lol
        
    
    # Take in base64 string and return PIL i
    # mage
    def stringToImage(self, base64_string):
        imgdata = base64.b64decode(base64_string)
        return Image.open(io.BytesIO(imgdata))
    

    def toRGB(self, image):
        
        return cv2.cvtColor(np.array(image), cv2.COLOR_BGR2RGB)


    def getImageData(self, db, auto_times, dt):
        
        self.db = db
        self.auto_times = auto_times
        self.dt = dt
        
        print(auto_times)
        
        for row in auto_times:
            
            start_time, end_time = row
            
            start_query = float(end_time) - self.dt
            end_query = float(end_time) + self.dt
                
            query = {
                'topic': '/apollo/sensor/camera/front_6mm/image/compressed',
                'header.timestampSec':{"$gte": start_query, "$lte":end_query}
            }
            
            # Extract data from MongoDB
            print("LOOKING FOR DATA")
            result = self.db.find(query)

            print("FOUND QUERY!")
            
            for document in result:
                
                self.createVideo(end_query)
                
                if document['topic'] == "/apollo/sensor/camera/front_6mm/image/compressed":
                    pil_im = self.stringToImage(document['data'])
                    rgb_im = self.toRGB(pil_im)
                    cv2.imshow(str(document['topic']), rgb_im)
                    cv2.waitKey(50)

            cv2.destroyAllWindows()

    def exportImageDataWithMetadata(self, db, base_metadata, localization_data, auto_times, dt):
        
        self.db = db
        self.auto_times = auto_times
        self.dt = dt
        
        to_json_file = {}
        localization_metadata = {}
        
        for row in auto_times:
            
            start_time, end_time = row
            
            start_query = float(end_time) - self.dt
            end_query = float(end_time) + self.dt
                
            query = {
                'topic': '/apollo/sensor/camera/front_6mm/image/compressed',
                'header.timestampSec':{"$gte": start_query, "$lte":end_query}
            }
            
            # Extract data from MongoDB
            print("LOOKING FOR DATA")
            result = self.db.find(query)

            print("FOUND QUERY!")

            self.createVideo(end_query)
            
            self.json_file_name = str(round(time.time())) + "_queryts_" + str(end_query) + "_06mm.json"
            

            frame_count = 1
            
            for document in result:
                
                if document['topic'] == "/apollo/sensor/camera/front_6mm/image/compressed":
                    
                    # Get the localization metadata
                    ts = document['header']['timestampSec']
                    
                    # Gets the localization metadata
                    localization_metadata[frame_count] = self.getMatchedMetaData(ts, localization_data, frame_count)

                    # Get the image
                    pil_im = self.stringToImage(document['data'])
                    rgb_im = self.toRGB(pil_im)
                    self.add_frame_06(rgb_im, base_metadata)
                    
                    frame_count += 1

            to_json_file = {"header": base_metadata, "frames": localization_data}
            
            with open(self.json_file_name, 'a') as json_file:
                json.dump(to_json_file, json_file, indent=4)
                json_file.write('\n')
                
        
            if self.show_video:
                
                cv2.destroyAllWindows()
            
            
    def createVideo(self, end_query):
        
        self.fourcc = cv2.VideoWriter_fourcc(*'XVID')
        self.output_name_06 = str(round(time.time())) + "_queryts_" + str(end_query) + "_06mm.avi"
        self.video_06 = cv2.VideoWriter(self.output_name_06, self.fourcc, 20, (1360,768))
    
    def add_frame_06(self, img, to_frame_metadata):
        
        img = cv2.resize(img, (1360,768))
        self.video_06.write(img)
        
        if self.show_video:
            
            print('frame added')
            cv2.imshow('Image', img)
            cv2.waitKey(50)
            
            
    def getMatchedMetaData(self, ts, to_match_data, frame_count):
        
        # Extract timestamps from the first column of localization_data
        timestamps = np.array(to_match_data)[:, 0]
        
        # Find the index of the closest timestamp
        closest_index = np.argmin(np.abs(timestamps - ts))
        
        # Get the closest timestamp
        # closest_timestamp = timestamps[closest_index]
        
        localization_metadata = {
            'timestamp': to_match_data[closest_index][0],
            'x': to_match_data[closest_index][1],
            'y': to_match_data[closest_index][2],
            'z': to_match_data[closest_index][3]
        }
        
        return localization_metadata
    
    
class DecimalEncoder(json.JSONEncoder):
    
    def default(self, o):
        
        from decimal import Decimal
        
        if isinstance(o, Decimal):
            
            return str(o)  # Convert Decimal to string
        
        return super(DecimalEncoder, self).default(o)  



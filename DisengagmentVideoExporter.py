import pymongo
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import io
import cv2
import base64 

class DesengagmentVideoExporter():
    
    def __init__(self):
        
        pass

        
    # Take in base64 string and return PIL image
    def stringToImage(self, base64_string):
        imgdata = base64.b64decode(base64_string)
        return Image.open(io.BytesIO(imgdata))

    def toRGB(self, image):
        return cv2.cvtColor(np.array(image), cv2.COLOR_BGR2RGB)


    def getImageData(self, db, auto_times, dt):
        
        self.db = db
        self.auto_times = auto_times
        self.dt = dt


        # # Set up MongoDB connection
        # client = pymongo.MongoClient("mongodb://127.0.0.1:27017")

        # # Access the database
        # db = client.cyber19  # Replace 'your_database' with the actual name of your database

        # # Access the collection (similar to a table in relational databases)
        # collection = db.cyber19  # Replace 'your_collection' with the actual name of your collection

        # # Query to extract data (you can customize this based on your needs)
        # # query = {'topic': '/apollo/localization/pose'}
        
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
            # print(result)

            print("FOUND QUERY!")
            # ax1 =  plt.figure().add_subplot(projection='3d')
            # ax1.axis('equal')
            # sorted_result = sorted(result,  key=lambda x: (x['time']))
            for document in result:
                # print(document)
                if document['topic'] == "/apollo/sensor/camera/front_6mm/image/compressed":
                    pil_im = self.stringToImage(document['data'])
                    rgb_im = self.toRGB(pil_im)
                    cv2.imshow(str(document['topic']), rgb_im)
                    cv2.waitKey(50)
                    
            
            
            cv2.destroyAllWindows()
                    
            

            # elif document['topic'] == '/apollo/sensor/velodyne32/PointCloud2':
            #     # ax1.clear()
            #     ax1.cla()
            #     x_master = []
            #     y_master = []
            #     z_master = []
            #     # print(document['point'])

            #     for point in range(len(document['point'])):
            #         x_master.append(document['point'][point]['x'])
            #         y_master.append(document['point'][point]['y'])
            #         z_master.append(document['point'][point]['z'])

            #     ax1.scatter(x_master, y_master, z_master)
            #     plt.pause(0.000000000001)



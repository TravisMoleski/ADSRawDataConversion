import pymongo
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import io
import cv2
import base64 


# Take in base64 string and return PIL image
def stringToImage(base64_string):
    imgdata = base64.b64decode(base64_string)
    return Image.open(io.BytesIO(imgdata))

def toRGB(image):
    return cv2.cvtColor(np.array(image), cv2.COLOR_BGR2RGB)


# Set up MongoDB connection
client = pymongo.MongoClient("mongodb://127.0.0.1:27017")

# Access the database
db = client.mongo_aws  # Replace 'your_database' with the actual name of your database

# Access the collection (similar to a table in relational databases)
collection = db.mongo_aws  # Replace 'your_collection' with the actual name of your collection

# Query to extract data (you can customize this based on your needs)
# query = {'topic': '/apollo/localization/pose'}
query = {'topic': '/apollo/sensor/camera/front_6mm/image/compressed'}
# Extract data from MongoDB
result = collection.find(query)


ax1 =  plt.figure().add_subplot(projection='3d')
ax1.axis('equal')
# sorted_result = sorted(result,  key=lambda x: (x['time']))
for document in result:
    if document['topic'] == "/apollo/sensor/camera/front_6mm/image/compressed":
        pil_im = stringToImage(document['data'])
        rgb_im = toRGB(pil_im)
        cv2.imshow(str(document['topic']), rgb_im)
        cv2.waitKey(50)

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

# Close the MongoDB connection
client.close()


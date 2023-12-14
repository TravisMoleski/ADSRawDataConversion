import pymongo
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import io
import cv2
import base64 
import json
import sys

import utm

from scipy.spatial.transform import Rotation as R
import time

import pyvista as pv
import pyvistaqt as pvqt
from PyQt5.QtCore import QCoreApplication

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

dis_info = open("./disengagement_times/657212157f17985fbdabfd01_.json")
dis_info = json.load(dis_info)
print(dis_info)

dis_time = dis_info['disengagement_times'][0]
dis_dt   = dis_info['disengagement_tolerance']
dis_dt = 5

# Query to extract data (you can customize this based on your needs)
# query = {'topic': '/apollo/localization/pose'}
query = {
    # 'topic': '/apollo/prediction/perception_obstacles',
    'header.timestampSec':{"$gte": dis_time-dis_dt, "$lte":dis_time+dis_dt}
}
# Extract data from MongoDB
print("LOOKING FOR DATA")
result = collection.find(query)
# 
p =  pvqt.BackgroundPlotter()
# p = pv.Plotter()
p.show()  # Start visualisation, non-blocking call
# p.show(auto_close=False, interactive_update=True)  # Start visualisation, non-blocking call
print("CREATED PLOT")


get_im = True
get_lidar = True
get_obstacles = True
get_pose = True
get_gnss = True

# tStart = time.time()

sol_type = "NOT_QUERIED"
lat = None
lon = None
std_2d = None
driving_mode = None
for document in result:
    # loopStart = time.time()
    # print(document['topic'],"\n")
    if document['topic'] == "/apollo/sensor/camera/front_6mm/image/compressed" and get_im:
        pil_im = stringToImage(document['data'])
        rgb_im = toRGB(pil_im)

        cv2.putText(rgb_im, sol_type, (10,50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255,255,255), 2, cv2.LINE_AA)
        cv2.putText(rgb_im, ("(") + str(lat) + ', ' +str(lon) +  (")"), (10,100), cv2.FONT_HERSHEY_SIMPLEX, 1, (255,255,255), 2, cv2.LINE_AA)
        cv2.putText(rgb_im, 'STD: '+ str(std_2d), (10,150), cv2.FONT_HERSHEY_SIMPLEX, 1, (255,255,255), 2, cv2.LINE_AA)
        cv2.putText(rgb_im, str(driving_mode), (10,200), cv2.FONT_HERSHEY_SIMPLEX, 1, (255,255,255), 2, cv2.LINE_AA)

        cv2.imshow(str(document['topic']), rgb_im)
        cv2.waitKey(1)

    if document['topic'] == "/apollo/localization/pose" and get_pose:
        vehicle_position = document['pose']['position']
        lat, lon = utm.to_latlon(vehicle_position['x'], vehicle_position['y'], 17,'S')
        roll  = document['pose']['eulerAngles']['x']
        pitch = document['pose']['eulerAngles']['y']
        yaw   = document['pose']['eulerAngles']['z'] + np.pi/2

        vehicle_rot = R.from_euler('xyz',[roll,pitch,yaw],degrees=False)

        box_v = pv.Cube(center=(vehicle_position['x'], vehicle_position['y'], vehicle_position['z']),x_length=5.18922 , y_length=2.29616, z_length=1.77546)
        rotated = box_v.rotate_z(angle=np.rad2deg(yaw), point=[vehicle_position['x'],vehicle_position['y'],vehicle_position['z']])
        p.add_mesh(rotated, color='blue', show_edges=True)


    if document['topic'] == '/apollo/sensor/velodyne32/PointCloud2':
        # p.clear()
        if get_lidar:
            x_master = np.zeros(len(document['point']))
            y_master = np.zeros(len(document['point']))
            z_master = np.zeros(len(document['point']))
            int_master = np.zeros(len(document['point']))

            for point in range(len(document['point'])):
                lPoint = [document['point'][point]['x'],document['point'][point]['y'],document['point'][point]['z']]
                lPoint = vehicle_rot.apply(lPoint)

                x_master[point] =  (lPoint[0] + vehicle_position['x'])
                y_master[point] =  (lPoint[1] + vehicle_position['y'])
                z_master[point] =  (lPoint[2] + vehicle_position['z'])
                int_master[point] =  (document['point'][point]['intensity'])

            point_cloud = pv.PolyData(np.column_stack((x_master, y_master, z_master)))
            point_cloud['colors'] = int_master
            point_cloud.plot(point_cloud, cmap='jet', render_points_as_spheres=True)
            p.add_points(point_cloud)

        # p.update()

    if document['topic'] == "/apollo/perception/obstacles" and get_obstacles:
        # p.clear()
        # p.update()
        for obs in document['perceptionObstacle']:
            obs_point = np.array([obs['position']['x'],obs['position']['y'],obs['position']['z']])
            if obs['type'] == 'VEHICLE':
                c = 'red'
            else:
                c = 'green'

            boxO = pv.Cube(center=(obs_point),x_length=obs['length'] , y_length=obs['width'], z_length= obs['height'])
            boxRot = boxO.rotate_z(angle=np.rad2deg(obs['theta']),point=obs_point)
            p.add_mesh(boxRot, color=c, show_edges=True)

    if document['topic'] == "/apollo/sensor/gnss/best_pose" and get_gnss:
        # p.clear()
        sol_type =document['solType']
        lat_std_gnss = document['latitudeStdDev']
        lon_std_gnss = document['longitudeStdDev']
        hgt_std_gnss = document['heightStdDev']
        num_sat = document['numSatsInSolution']
        std_2d = (lat_std_gnss + lon_std_gnss)/2
        # print("SOLUTION TYPE", sol_type, "WITH 2D std (m)", std_2d)

    if document['topic'] == "/apollo/canbus/chassis":
        driving_mode = document['drivingMode']
        # print(document)

    # loopEnd = time.time()
    # rate_loop = 1 / (loopEnd - loopStart)
    # print("LOOP RATE: ", rate_loop)
    # time.sleep(.001)

    # p.update()
    # p.show(auto_close=False, interactive_update=True)  # Start visualisation, non-blocking call
    QCoreApplication.processEvents()

# Close the MongoDB connection
p.close()
cv2.destroyAllWindows()
client.close()


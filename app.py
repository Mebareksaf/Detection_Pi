from __future__ import division
import numpy as np
import threading
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import concurrent.futures
import cv2
import os
import json
from time import time
import logging
from cameras_config import cameras_config

#Creates a kafka topic dynamically
def Create_Cam_Topic(self, camera_name):
    broker_topics = self.adminClient.list_topics().topics

    topic = self.hostname+"-Detection_"+camera_name
    #create the topic if it doesnt exist among the brokers topics
    if not(topic in broker_topics.keys()):
        #NewTopic(topic_name, num_partitions, replication_factor)
        topic_list = [NewTopic(topic, 1, 1)]
        admin_client.create_topics(topic_list)
    return topic

def find_camera(list_id):
    return cameras[int(list_id)]


#Serializes img to bytes to publish them into kafka(messages in kafka are bytes)
def serializeImg(img):
    _, img_buffer_arr = cv2.imencode(".jpg", img)
    img_bytes = img_buffer_arr.tobytes()
    return img_bytes


#The produce method is asynchronous. It doesnt wait for confirmation that the message has been 
# delivered or not. We pass in a callback function that logs the information about the message 
# produced or error if any. Callbacks are executed and sent as a side-effect of calls to the poll 
# or flush methods.
def delivery_report(err, msg):
    if err:
        logging.error("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        logging.info("msg produced. "+
                    "Topic: {0}".format(msg.topic()) +
                    "Partition: {0}".format(msg.partition()) +
                    "Offset: {0}".format(msg.offset()) +
                    "Timestamp: {0}".format(msg.timestamp()))

    
#finds the objects in an image after yolov3 detection and returns coordinates,confidence & class of the objects as dict
def findObjects(outputs, img, frame_no):
        ht, wt, ct = img.shape
        bbox = []
        classIds = []
        conf = []
        frameObjects = {} #'dict()'
        frameObjects.update({"Image_Nb": frame_no})
        for output in outputs:
            for det in output:
                scores = det[5:]
                classId = np.argmax(scores)
                confidence = scores[classId]
                #only take objects above 50% confidence
                if confidence > 0.5:
                    w,h = int(det[2]*wt), int(det[3]*ht)
                    x,y = int((det[0]*wt)-w/2), int((det[1]*ht)-h/2)
                    bbox.append([x,y,w,h])
                    classIds.append(classId)
                    conf.append(float(confidence))

        #eliminate other potential boxes using a threshhold of 0.3 
        indices = cv2.dnn.NMSBoxes(bbox,conf,0.5,0.3)
        
        j = 0
        #updates the dict with the filtered boxes(coordinates,conf,class) as a nested dict
        for i in indices:
            i = i[0]
            box = bbox[i]
            x,y,w,h = box[0],box[1],box[2],box[3]
            conf[i] = round(conf[i], 3)
            classe = classNames[classIds[i]]

            #Only consider certain objects
            if classe == 'C1\r' or classe == 'C2\r' or classe == 'C3\r' or classe == 'C4\r' :
                j+=1
                #each box represents an object having a class,confidence and coordinates
                frameObjects.update({'box'+str(j):{'class':classe, 'conf':conf[i], 
                                                        'x':x, 'y':y, 'w':w, 'h':h}})

        return frameObjects   

#get the number of person
def numPer(Objects):
        j = 0
        b = True
        n = 0
        while b:
            j+=1
            if "box"+str(j) in Objects:
                if Objects["box"+str(j)]["class"] == 'person\r' : n+=1
            else:
                b = False
        return n

#count the speed of change
def countMov(fram1,fram2):
    print('Start COUNTMOVE2')
    diff = cv2.absdiff(fram1, fram2)
    gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (25,25), 0)
    _, thresh = cv2.threshold(blur, 20, 255, cv2.THRESH_BINARY)
    dilated = cv2.dilate(thresh, None, iterations=3)
    contours, _ = cv2.findContours(dilated, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    coun = 0
    for contour in contours:
        (x, y, w, h) = cv2.boundingRect(contour)
        coun+=cv2.contourArea(contour)
    if len(contours)>0:
        if coun/len(contours) <= 5000:
            return coun/len(contours)
        else:
            return 5000.0
    else: return coun



class ProducerThread:
    def __init__(self, hostname, adminClient, kafka_broker_url):
        self.adminClient = adminClient
        self.producer = Producer({"bootstrap.servers": kafka_broker_url}) #KAFKA_BROKER_URL
        self.hostname = str(hostname)
        

    def publishFrame(self, cam, netModel):    
        cam_index = cameras.index(cam)    
        camera_name = "Camera"+str(cam_index)

        cap = cv2.VideoCapture(cam)
        frame_no = 0
        oldframe = '\r' #None
        speed_change = 0
        while True: #cap.isOpened():
            frameObjectss = {}
            _, frame = cap.read()
            if _:
                frame_time = time()
            
                # applying detection every 5th frame (reduces traffic-load)
                if frame_no % 5 == 0:
                    topic = Create_Cam_Topic(self, camera_name)
                   
                    img = cv2.resize(frame, (224, 224))

                    #creates a blob from the img to pass to the network
                    blob = cv2.dnn.blobFromImage(img,1/255,(wht,wht),[0,0,0],1,crop=False)
                    netModel.setInput(blob)

                    layerNames = netModel.getLayerNames()
                    outputNames = [layerNames[i[0]-1] for i in netModel.getUnconnectedOutLayers()]
		    start = time()
                    outputs = netModel.forward(outputNames)
		    end = time()
		    print("YOLO took ", (end-start), " seconds")
                    frameObjectss.update(findObjects(outputs,img, frame_no))

                    #get number of persons in the frame
                    nb_people = numPer(frameObjectss)
                    
                    #get speed of change
                    
                    if oldframe == '\r':
                        oldframe = frame
                        print("old frame NONE!!")
                    else:
			print("Starting COUNTMOV")
                        speed_change = countMov(oldframe,frame)
                        oldframe = frame
                        print("COUNT MOVE DONE!!")

                    #serializes objectss to bytes
                    frameObjectss = json.dumps(frameObjectss).encode('utf-8')

                    frame_bytes = serializeImg(frame)
                    
                    headers = {"Host": self.hostname.encode('utf-8'),
                        "camera_name": camera_name.encode('utf-8'),
                        "frameObjects":frameObjectss,
                        "Nb_people": str(nb_people).encode('utf-8'),
                        "Speed_change":str(speed_change).encode('utf-8'),
                        "frame_no": str(frame_no).encode('utf-8'),
                        "timestamp":str(frame_time).encode('utf-8')
                            }

                    self.producer.produce(
                        topic, 
                        key="key",
                        value=frame_bytes, 
                        on_delivery=delivery_report,
                        headers=headers
                        )
                    
                    print(frame_no,"        ",nb_people,"         ",speed_change)
                    self.producer.poll(0)#0.1)
                    
                    #time.sleep(0.01)
                frame_no += 1
                
        cap.release()
    

    #maps each camera from cam_IPs list to a producer thread 
    def start(self, cams,netModel_list):
        # runs until the processes in all the threads are finished
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.publishFrame, cams, netModel_list)

        self.producer.flush() # push all the remaining messages in the queue
        print("Finished...")


if __name__ == "__main__":
    print('start producer')

    # list of camera accesses
    cameras = cameras_config

    
    wht = 320

    classFile = 'coco.names'
    classNames = []
    with open(classFile,'rt') as f:
        classNames = f.read().rstrip('\n').split('\n')
    
    modelConfiguration = 'yolov3.cfg'
    modelWeights = 'yolov3.weights' #yolov3.weights file must be present !!
    netModel_list = []
    
    for iii in range(len(cameras)):
        #init the neural network with configuration & weights
        net= cv2.dnn.readNetFromDarknet(modelConfiguration,modelWeights)
        net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
        net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
        netModel_list.append(net)

    kafka_broker_url = os.environ.get("KAFKA_BROKER_URL")

    admin_client = AdminClient({
    "bootstrap.servers":  kafka_broker_url
    })
    broker_topics = admin_client.list_topics().topics.keys()

    #get all brokers topics (a workaround to test if the broker is up)
    broker_topics = admin_client.list_topics().topics.keys()

    host = os.environ.get('HOST_NAME')
    
    producer_thread = ProducerThread(host, admin_client, kafka_broker_url)
    producer_thread.start(cameras,netModel_list)

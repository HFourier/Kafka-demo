import threading
from confluent_kafka import Consumer, KafkaError, KafkaException
from consumer_config import config as consumer_config
from utils import *
from datetime import datetime

# import tensorflow as tf
# from tensorflow.keras.applications import ResNet50
# from tensorflow.keras.applications.resnet50 import preprocess_input
# from tensorflow.keras.applications.imagenet_utils import decode_predictions

# from pymongo import MongoClient


import torch
from torchvision import models
import torch.nn.functional as F

from torchvision import transforms

import cv2
import numpy as np
import time


coco_labels_name = ["unlabeled", "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck", "boat",
    "traffic light", "fire hydrant", "street sign", "stop sign", "parking meter", "bench", "bird", "cat", "dog", "horse",
    "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "hat", "backpack", "umbrella", "shoe",
    "eye glasses", "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard", "sports_ball", "kite", "baseball bat",
    "baseball glove", "skateboard", "surfboard", "tennis racket", "bottle", "plate", "wine glass", "cup", "fork", "knife",
    "spoon", "bowl", "banana", "apple", "sandwich", "orange", "broccoli", "carrot", "hot_dog", "pizza",
    "donut", "cake", "chair", "couch", "potted plant", "bed", "mirror", "dining table", "window", "desk",
    "toilet", "door", "tv", "laptop", "mouse", "remote", "keyboard", "cell phone", "microwave", "oven",
    "toaster", "sink", "refrigerator", "blender", "book", "clock", "vase", "scissors", "teddy bear", "hair drier",
    "toothbrush", "hair brush"] # len = 92


class ConsumerThread:
    # def __init__(self, config, topic, batch_size, model, db, videos_map):
    def __init__(self, config, topic, batch_size, model, videos_map):
        self.config = config
        self.topic = topic
        self.batch_size = batch_size
        self.model = model
        # self.db = db
        self.videos_map = videos_map
        self.model = model

    def read_data(self):
        consumer = Consumer(self.config)
        consumer.subscribe(self.topic)
        self.run(consumer, 0, [], [])

    def run(self, consumer, msg_count, msg_array, metadata_array):
        print('[INFO] begin Run ........... ')
        transform = transforms.Compose([transforms.ToTensor()])
        img_array = []
        try:
            while True:
                msg = consumer.poll(0.5)
                if msg == None:
                    continue
                elif msg.error() == None:

                    # convert image bytes data to numpy array of dtype uint8
                    nparr = np.frombuffer(msg.value(), np.uint8)
                    print('[INFO] ',datetime.now())
                    # decode image
                    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                    img = cv2.resize(img, (224, 224))
                    img_array.append(img)
                    img_tensor = transform(img)
                    msg_array.append(img_tensor)

                    # get metadata
                    frame_no = msg.timestamp()[1]
                    video_name = msg.headers()[0][1].decode("utf-8")
                    print('[INFO] Get Frame No. {} '.format(frame_no), datetime.now())

                    metadata_array.append((frame_no, video_name))

                    # bulk process
                    msg_count += 1
                    if msg_count % self.batch_size == 0:
                        # print('[INFO] Have get a whole batch, begin process')
                        # 已经收集到一个batch了
                        predictions = self.model(msg_array)
                        # print(predictions)
                        for x in range(len(predictions)):
                            print('[INFO] Processing id {} '.format(int(frame_no-self.batch_size+x+1)))
                            pred = predictions[x]
                            scores = pred["scores"]
                            mask = scores > 0.5 # 只取scores值大于0.5的部分
                        
                            boxes = pred["boxes"][mask].int().detach().numpy() # [x1, y1, x2, y2]
                            labels = pred["labels"][mask]
                            scores = scores[mask]
                            # print(f"prediction: boxes:{boxes}, labels:{labels}, scores:{scores}")
                        
                            img = img_array[x]
                        
                            for idx in range(len(boxes)):
                                cv2.rectangle(img, (boxes[idx][0], boxes[idx][1]), (boxes[idx][2], boxes[idx][3]), (255, 0, 0))
                                cv2.putText(img, coco_labels_name[labels[idx]]+" "+str(scores[idx].detach().numpy()), (boxes[idx][0]+10, boxes[idx][1]+10), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0, 255, 0), 1)
                        
                            # cv2.imshow("image", img)
                            # cv2.waitKey(1000)
                            img_save_path = "./result/"+"batch_"+str(frame_no//2)+"img_"+str(x)+".jpg"
                            cv2.imwrite(img_save_path, img)
                            # print('[INFO] saved: ', img_save_path)
                            # time.sleep(1)
                            print('finished')
                       
                        consumer.commit(asynchronous=False)
                        # reset the parameters
                        msg_count = 0
                        metadata_array = []
                        msg_array = []
                        img_array = []
                        

                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached {0}/{1}'
                        .format(msg.topic(), msg.partition()))
                else:
                    print('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            print("Detected Keyboard Interrupt. Quitting...")
            pass

        finally:
            consumer.close()

    def start(self, numThreads):
        # Note that number of consumers in a group shouldn't exceed the number of partitions in the topic
        for _ in range(numThreads):
            t = threading.Thread(target=self.read_data)
            t.daemon = True
            t.start()
            while True: time.sleep(10)

if __name__ == "__main__":

    topic = ["multi-video-stream"]

    # initialize model
    # model = ResNet50 (
    #         include_top = True, 
    #         weights = 'imagenet', 
    #         input_tensor = None, 
    #         input_shape = (224, 224, 3), 
    #         pooling = None, 
    #         classes = 1000
    #         )
    
    model = models.detection.fasterrcnn_resnet50_fpn(pretrained=True)
    model.eval()
    
    # connect to mongodb
    # client = MongoClient('mongodb://localhost:27017')
    # db = client['video-stream-records']

    # video_names = ["MOT20-02-raw", "MOT20-03-raw", "MOT20-05-raw"]
    # videos_map = create_collections_unique(db, video_names)
    
    videos_map = False
    # consumer_thread = ConsumerThread(consumer_config, topic, 32, model, videos_map)
    consumer_thread = ConsumerThread(consumer_config, topic, 2, model, videos_map)
    # consumer_thread.start(3)
    consumer_thread.read_data()
"""
Music Streaming Data

TODO:


- Generate pool of users
- Generate random events
  -- play song
  -- change song
  -- stop song
"""



import random
import logging
import threading
import time
import os
import pandas as pd

from pymongo import MongoClient
from datetime import date
from datetime import datetime
from uuid import uuid4
from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker


class UserSession:

    def __init__(self, profile=None):
        self.profile = profile
        self.current_song = None
        self.application_state = 'STOPPED' # ['STOPPED', 'PLAYING']
        self.app = MusicStreamApp()
        self.listen_song_percentage = 1
        self.song_started_at = 0
        self.elepsed_session_time = 0
        kafka_host = os.environ['KAFKA_HOST']
        kafka_port = os.environ['KAFKA_PORT']
        self.kafka_topic = os.environ['KAFKA_TOPIC']
        db_user = os.environ['MONGO_USER']
        db_pass = os.environ['MONGO_PASS']
        db_host = os.environ['MONGO_HOST']
        self.db = MongoClient('mongodb://%s:%s@%s' % (db_user, db_pass, db_host)).musicapp
        #self.kafka_producer = KafkaProducer(bootstrap_servers='%s:%s' % (kafka_host, kafka_port))

        #self.logger = logging.basicConfig(filename='{username}_{timestamp}'.format(self.profile['username'], str(int(datetime.utcnow().timestamp()))), encoding='utf-8', level=logging.DEBUG)

    def initiate_session(self):
        # this will run a infinite loop simulating a user using the application. Timeout = 30min
        """  
            probabilities
            -> 0 ~ 0.60 - Entire song
            -> 0.61 ~ 0.85 - Stop after some random time between (1m ~ 2m)
            -> 0.85 ~ 1 - Wait some random time between 1m ~ 2m and get new song
            -> if stopped, set a random time between 30s ~ 2m and press play
        """

        self.push_to_kafka('open app')

        # session duration time
        session_time = random.randint(500, 3000) 
        logging.info('Profile session: {} --> SESSION TIME: {}'.format(self.profile['username'], session_time))


        start_time = time.time()
        while self.elepsed_session_time < session_time: 
            #logging.info('Elepsed session time: {} from {} '.format(str(round(self.elepsed_session_time)), session_time))
            #for _ in range(10):
            #    self.play_song()

            if self.application_state == 'STOPPED':
                self.play_song()
            elif self.application_state == 'PLAYING':
                song_elapsed_time = self.elepsed_session_time - self.song_started_at
                # logging.info('Song: {} - Elepsed song time: {} from {} - Percentage? - {} || Session duration: {}'.format(self.current_song['name'],
                #                                                                                                             round(song_elapsed_time), 
                #                                                                                                             self.current_song['duration_ms']/1000,
                #                                                                                                             self.listen_song_percentage,
                #                                                                                                             session_time))
                if song_elapsed_time >= (self.current_song['duration_ms']/1000) * self.listen_song_percentage:
                    self.change_song()
            

            
            time.sleep(0.3)
            self.elepsed_session_time = time.time() - start_time    
        
        # finished session
        if self.application_state == 'PLAYING':
            self.stop_song()

        self.close_session()

        return

    def play_song(self, song = None):
        if self.current_song == None or song == None:
            self.current_song = self.app.get_song()
        else:
            self.current_song = song

        logging.info('{} - Playing song {}'.format(self.profile['username'], self.current_song['name']))
        
        self.get_song_percetange_to_play()

        self.application_state = 'PLAYING'
        self.song_started_at = self.elepsed_session_time
        
        self.push_to_kafka('play')

    def stop_song(self):
        self.application_state = 'STOPPED'
        self.push_to_kafka('stop')
        return

    def change_song(self):
        self.push_to_kafka('change')
        song = self.app.get_song()
        
        self.play_song(song)
        

    def close_session(self):
        self.push_to_kafka('close app')
        return

    def get_song_percetange_to_play(self):
        listen_full_song = random.randint(15, 100) > 40
        if listen_full_song:
            self.listen_song_percentage = 1
        else:
            self.listen_song_percentage = listen_full_song / 100

        return

    def push_to_kafka(self, action):
        event_timestamp = int(datetime.utcnow().timestamp())
        event_id = uuid4()

        event = {
            'user_id': self.profile['user_id'],
            'event_id': str(event_id),
            'event_timestamp': event_timestamp,
            'song_id': self.current_song['id'] if self.current_song != None else '',
            'action': action
        }

        print(event)

        return


class ProfilesGenerator:

    def __init__(self):
        logging.info('INITIALIZING APP...')
        Faker.seed(0)
        self.fake = Faker()
        self.db = MongoClient('mongodb://%s:%s@127.0.0.1' % ('admin', 'admin')).musicapp

    def generate_profiles_pool(self, quantity: int = 10):
        logging.info("Generating {} profiles...".format(str(quantity)))
        profiles = list()
        for _ in range(quantity):
            new_profile = self.fake.simple_profile()
            print(new_profile)
            new_profile['birthdate'] = datetime.combine(new_profile['birthdate'], datetime.min.time())
            result = self.db.profiles.insert_one(new_profile)
            new_profile['user_id'] = str(result.inserted_id)
            profiles.append(new_profile)
            
            

        # save to database
        #print(profiles)
        return profiles
        
    def create_new_profile(self):
        print(self.fake.simple_profile())

class MusicStreamApp:

    def __init__(self):
        self.db = MongoClient('mongodb://%s:%s@127.0.0.1' % ('admin', 'admin')).musicapp
        self.tracks = pd.read_csv(filepath="tracks.csv", delimiter=',', encoding="utf-8")
        self.artists = pd.read_csv(filepath="artists.csv", delimiter=',', encoding="utf-8")

    def get_song(self):
        """
            This method must return a random song from database
        """ 

        # pipeline = [
        #     {"$match": {"duration_ms": {"$lt": 300000}}},
        #     {"$sample": { "size": 1 }}
        # ]

        # song = list(self.db.tracks.aggregate(pipeline=pipeline))[0]
  
        return song

    def add_new_song(self):
        """
            This method must get a new song from csv dataset and register in database
        """
        return "New Song Added"



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Running application')

    profile_generator = ProfilesGenerator()
    profiles = profile_generator.generate_profiles_pool(200)

    threads = list()
    for p in profiles:
        x = threading.Thread(target=UserSession(p).initiate_session)
        threads.append(x)
        x.start()
    
    for index, thread in enumerate(threads):
        thread.join()
        logging.info("Main    : thread %d done", index)

    logging.info('Closing application')


 
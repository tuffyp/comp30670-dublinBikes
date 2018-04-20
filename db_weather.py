#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 10:18:57 2018

@author: sam
"""

import json
#ximport sqlite3
import mysql.connector
#import sqlalchemy 
import datetime
import requests
import traceback
import time


print("Initializing Script")
conn = mysql.connector.connect(user = 'abisheksam', password ='abisheksam7', host = 'bikes.czjtkqzwr4ef.us-west-2.rds.amazonaws.com')
print("Connected to RDS.. Loading")
c = conn.cursor()
sql = 'CREATE DATABASE IF NOT EXISTS weather'
c.execute(sql)

#Function for creating the new database
def create_db():
    conn = mysql.connector.connect(user = 'abisheksam', password = 'abisheksam7', host = 'bikes.czjtkqzwr4ef.us-west-2.rds.amazonaws.com')
    c = conn.cursor()
    print("inside create db")
    sql = 'CREATE DATABASE IF NOT EXISTS weather_data'
    c.execute(sql)
    
#class for retreiving scrapped data from API   
class db_retrieve(object):
    
    def __init__(self):
        '''
        Constructor
        '''
        self.url = 'http://api.openweathermap.org/data/2.5/forecast?id=524901&APPID=b3aa3f8637f7a190411119e7cf3a93e2'
        
    #retreive the data
    def call_req(self):
        data = requests.get(self.url)
        return data
    
    #saves the retrieved data as csv file
    def csv_save(self, db):
        with open("weather.csv", "w") as file:
            file.write(db)
            
    def json(self, db):
        with open("weather.json", "w") as file:
            file.write(db)

    def fetchDataJson(self, JSONfile):
        with open(JSONfile) as data_file:
            db = json.load(data_file)
        return db



    def create_table_db(self):
        conn = mysql.connector.connect(user = 'abisheksam', password = 'abisheksam7', host = 'bikes.czjtkqzwr4ef.us-west-2.rds.amazonaws.com', database = 'weather')
        c = conn.cursor()
        
       
        
        #Creating Variable for 
        create_stmt = "CREATE TABLE IF NOT EXISTS Weather_info(\
              Temperature FLOAT(10,8),\
              Min_Temp FLOAT(10,8),\
              Max_Temp FLOAT(10,8),\
              Clouds INT(10),\
              Pressure FLOAT(10,8),\
              Sea_Level FLOAT(10,8),\
              Ground_Level FLOAT(10,8),\
              Humidity FLOAT(10,8),\
              Wind_Speed FLOAT(10,8),\
              Date VARCHAR(255),\
              PRIMARY KEY (Sea_Level,Ground_Level))"
        
        c.execute(create_stmt)
    
    
        conn.commit()
        c.close
        conn.close()
        
    def data_entry(self, db):
        value = db[0]['dt_txt']
        print(value)
        res = datetime.datetime.fromtimestamp(value/1000).strftime('%Y-%m-%d %H:%M:%S')
        
        for i in range(0, len(db)):
            db[i]['last_update'] = res
            
       
        #Connecting to Database
       # conn = mysql.connector.connect(user = 'abisheksam', password = 'abisheksam7', host = 'bikes.czjtkqzwr4ef.us-west-2.rds.amazonaws.com', database = 'weather')
        #c = conn.cursor()
        # var = j[
        #print(j)
        
        for j in range(len(db)):
            var = j[0]
            print(var)
            Temperature = j['list']['main']['temp']
            Min_Temp = j['list']['main']['temp_min']
            Max_Temp = j['list']['main']['temp_max']
            Clouds = j['list']['clouds']
            Pressure = j['list']['main']['pressure']
            Sea_Level = j['list']['main']['sea_level']
            Ground_Level = j['list']['main']['grnd_level']
            Humidity = j['list']['main']['humidity']
            Wind_Speed = j['list']['wind']['speed']
           
            
            print("Scrapping Data...")
            
            c.execute("""INSERT INTO Weather_info (Temperature, Min_Temp, Max_Temp, Clouds, Pressure, Sea_Level, Ground_Level, Humidity, Wind_Speed) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (Temperature, Min_Temp, Max_Temp, Clouds, Pressure, Sea_Level, Ground_Level, Humidity, Wind_Speed))
            
            
        conn.commit()
        c.close
        conn.close()
        

dbs =  db_retrieve()
db = dbs.call_req()
dbs.create_table_db()
dbs.json(db.text)
dbs.data_entry(dbs.fetchDataJson('weather.json'))

 
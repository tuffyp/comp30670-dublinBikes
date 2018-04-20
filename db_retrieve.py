#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 18:05:25 2018

@author: sam
"""

import json
#ximport sqlite3
import mysql.connector
#import sqlalchemy 
import datetime
import requests
#import create_engine
import time
import traceback

print("Initializing Script")
conn = mysql.connector.connect(user = 'abisheksam', password ='abisheksam7', host = 'bikes.czjtkqzwr4ef.us-west-2.rds.amazonaws.com')
print("Connected to RDS.. Loading")
c = conn.cursor()
sql = 'CREATE DATABASE IF NOT EXISTS dubbikes'
c.execute(sql)

#Function for creating the new database
def create_db():
    conn = mysql.connector.connect(user = 'abisheksam', password = 'abisheksam7', host = 'bikes.czjtkqzwr4ef.us-west-2.rds.amazonaws.com')
    c = conn.cursor()
    print("inside create db")
    sql = 'CREATE DATABASE IF NOT EXISTS bike_data'
    c.execute(sql)
    
#class for retreiving scrapped data from API   
class db_retrieve(object):
    
    def __init__(self):
        '''
        Constructor
        '''
        self.url = 'https://api.jcdecaux.com/vls/v1/stations?contract=Dublin&apiKey=41875ce3055e106267cf6104eb90bf8def0adfea'
        
    #retreive the data
    def call_req(self):
        data = requests.get(self.url)
        return data
    
    #saves the retrieved data as csv file
    def csv_save(self, db):
        with open("dbikes.csv", "w") as file:
            file.write(db)
            
    def json(self, db):
        with open("dbikes.json", "w") as file:
            file.write(db)

    def fetchDataJson(self, JSONfile):
        with open(JSONfile) as data_file:
            db = json.load(data_file)
        return db



    def create_table_db(self):
        conn = mysql.connector.connect(user = 'abisheksam', password = 'abisheksam7', host = 'bikes.czjtkqzwr4ef.us-west-2.rds.amazonaws.com', database = 'dubbikes')
        c = conn.cursor()
        
        #Creating Variable for 
        create_stmt = "CREATE TABLE IF NOT EXISTS Occupancy_info(\
              Number INT(10),\
              Station_Name VARCHAR(45),\
              Location FLOAT(10,8),\
              Latitude FLOAT(10,8),\
              Longitude FLOAT(10,8),\
              Banking VARCHAR(10),\
              Bonus VARCHAR(45),\
              Status VARCHAR(10),\
              Stands_info INT(5),\
              Available_stands INT(5),\
              Available_bikes INT(5),\
              Last_Update VARCHAR(255),\
              PRIMARY KEY (Number, Last_Update))"
        
        c.execute(create_stmt)
    
    
        conn.commit()
        c.close
        conn.close()
        
    def data_entry(self, db):
        value = db[0]['last_update']
        res = datetime.datetime.fromtimestamp(value/1000).strftime('%Y-%m-%d %H:%M:%S')
        
        for i in range(0, len(db)):
            db[i]['last_update'] = res
            
       
        #Connecting to Database
        conn = mysql.connector.connect(user = 'abisheksam', password = 'abisheksam7', host = 'bikes.czjtkqzwr4ef.us-west-2.rds.amazonaws.com', database = 'dubbikes')
        c = conn.cursor()
        
        for j in db:
            Number = j['number']
            Station_Name = j['name']
            Location = j['address']
            Latitude = j['position']['lat']
            Longitude = j['position']['lng']
            Banking = j['banking']
            Bonus = j['bonus']
            Status = j['status']
            Stands_info = j['bike_stands']
            Available_stands = j['available_bike_stands']
            Available_bikes = j['available_bikes']
            Last_update = j['last_update']
            
            print("Scrapping Data...")
            
            c.execute("""INSERT INTO Occupancy_info (Number,Station_Name, Location, Latitude, Longitude, Banking, Bonus, Status, Stands_info, Available_stands, Available_bikes, Last_update) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (Number, Station_Name, Location, Latitude, Longitude, Banking, Bonus, Status, Stands_info, Available_stands, Available_bikes, Last_update))
            
            
        conn.commit()
        c.close
        conn.close()

def main():
    dbs =  db_retrieve()
    db = dbs.call_req()
    dbs.create_table_db()
    dbs.json(db.text)
    dbs.data_entry(dbs.fetchDataJson('dbikes.json'))
    return

if __name__ == '__main__':
    # infinite loop
    
    while True:
        try:
            main()
        except:
            print(traceback.format_exc())            
        
        time.sleep(5*60)

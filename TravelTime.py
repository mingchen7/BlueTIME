'''
Created on Sep 16, 2014

@author: mingchen7
'''

import csv
import datetime
from collections import defaultdict

class TravelTimeCalculator:
    __TravelTime__ = None
    __MacAddress__ = None
    __Timestamp__ = None
    __Threshold__ = None #3 times signal cycle 
    __Locationlist__ = None
    
    def __init__(self,LocationList):
        
        self.__Threshold__ = 360
        self.__TravelTime__ = {}
        self.__MacAddress__ = {}
        self.__Timestamp__ = {}
        self.__Locationlist__ = LocationList
        for location in self.__Locationlist__:
            self.__TravelTime__[location] = defaultdict(list)  #for each location, it's a structure of lists in dictionary to store travel time results
            self.__MacAddress__[location] = defaultdict(list)  #same the mac address for the corresponding travel time samples
            self.__Timestamp__[location] = defaultdict(list)
            
    def getTravelTime(self):
        return self.__TravelTime__
    
    def getMacAddress(self):
        return self.__MacAddress__
    
    def getTimestamp(self):
        return self.__Timestamp__
        
    def CalculateTT(self,TripList,method='F2F'):
        AdjTrip = [] #receive the new trip data after removing parking records
        for i in range(len(TripList)):
            OneTrip = TripList[i]
            
            #-------------------------------------------------------
            #method to identify running vehicle or bikes/pedestrians
            #-------------------------------------------------------
            
            #vehicle part            
            AdjTrip = self.RemoveParking(OneTrip)
            
            if(AdjTrip != None):
                for i in range(len(AdjTrip)):
                    if(self.checkMultiLocation(AdjTrip[i]) == True):                
                        if(method == 'F2F'):
                            self.First2First(AdjTrip[i])
                        else:
                            self.Last2Last(AdjTrip[i])
                
    
    def First2First(self,Trip):
        row_mark = 0
        mac = Trip[0][0]
        FromRecord = Trip[row_mark]
        FromLocation = FromRecord[1]  #get the location of current row
        FromTime = FromRecord[2]
        ArrivalTime = FromTime
        
        
        for row in range(len(Trip)-1):           
            nxtRow = Trip[row+1]
                
            if(FromLocation == nxtRow[1]):  #records from the same location
                continue
            else:  #from two different locations                
                ToRecord = nxtRow
                ToLocation = ToRecord[1]
                ToTime = ToRecord[2]
                    
                TimeDiff = ToTime - FromTime
                TT = TimeDiff.seconds
                self.Save2Container(FromLocation,ToLocation,TT,mac,ArrivalTime)  #should FromTime, as this is arrival time
                    
                row_mark = row + 1
                row = row + 1  #sychronize row and row mark
                
                FromRecord = Trip[row_mark]
                FromLocation = FromRecord[1]
                FromTime = FromRecord[2]
                ArrivalTime = FromTime
                
                    
    def Last2Last(self,Trip):
        row_mark = -1
        mac = Trip[0][0]
        FromLocation = None
        FromTime = None
        ToLocation = None
        ToTime = None        
        ArrivalTime = None
        
        for row in range(len(Trip)):
            thisRow = Trip[row]
            
            if((row == len(Trip) - 1) and (FromLocation != None)): # last record of this trip, calculate the last travel time sample
                ToLocation = thisRow[1]
                ToTime = thisRow[2]
                TimeDiff = ToTime - FromTime
                TT = TimeDiff.seconds
                self.Save2Container(FromLocation, ToLocation, TT,mac,ArrivalTime)
            
            else:
                nxtRow = Trip[row+1]
              
                if(thisRow[1] == nxtRow[1]):  #records from the same location
                    continue
                
                else: #from two different locations
                    
                    if(row_mark == -1):
                        row_mark = row
                        FromLocation = thisRow[1]
                        FromTime = thisRow[2]
                        if(row < (len(Trip) - 1)): 
                            ArrivalTime = Trip[row + 1][2] # record arrival time of this trip 
                             
                    else:    
                        ToLocation = thisRow[1]
                        ToTime = thisRow[2]
                        TimeDiff = ToTime - FromTime
                        TT = TimeDiff.seconds
                        self.Save2Container(FromLocation, ToLocation, TT, mac,ArrivalTime)
                        
                        FromLocation = thisRow[1]
                        FromTime = thisRow[2]
                        if(row < (len(Trip) - 1)): 
                            ArrivalTime = Trip[row + 1][2] # record arrival time of this trip 
                        
                        row_mark = row
    
    def RemoveParking(self,Trip):
        
        NewTripContainer = []  #size may >=2, depending on the location of parking records
        NewTrip = []
        
        FirstRow = Trip[0] #fist row
        FirstTime = FirstRow[2]  #the time that was first detected
        idxFirst = 0
        idxLast = 0
        
        for row in range(len(Trip)-1):
            nxtRow = Trip[row+1]
            
            if(row == len(Trip) -2): #reaching the last second record
                idxLast = row + 1
                LastRow = Trip[row+1]
                LastTime = LastRow[2]  #last detected time
                TimeDiff = LastTime - FirstTime
                
                if(TimeDiff.seconds <= self.__Threshold__): # THRESHOLD: three Average Signal Cycle
                    self.CopyTrip(idxFirst, idxLast,Trip, NewTrip)
                    NewTripContainer.append(NewTrip)
                    NewTrip = None
                    
                    return NewTripContainer #return here
                
            if(FirstRow[1] == nxtRow[1]): #same location 
                continue
                          
            idxLast = row
            LastRow = Trip[row]
            LastTime = LastRow[2]  #last detected time
            TimeDiff = LastTime - FirstTime
            
            if(TimeDiff.seconds <= 360): # THRESHOLD: three Average Signal Cycle
                self.CopyTrip(idxFirst, idxLast,Trip, NewTrip)
            else: #detect parking records
                if(len(NewTrip) != 0):
                    NewTripContainer.append(NewTrip)  #locate parking records, store the previous trip records and set a new trip list
                
                NewTrip = []
            
            #set new first row
            idxFirst = row+1
            FirstRow = Trip[row+1]
            FirstTime = FirstRow[2]
            
        
        return None #vehicle parks at every intersection
                
    def CopyTrip(self,idxFirst,idxLast,preTrip, adjTrip):
        for i in range(idxFirst, idxLast+1):
            adjTrip.append(preTrip[i])
    
    #To check if the trip include multiple locations, return true if yes, return false if no
    def checkMultiLocation(self,Trip):   
        firstLocation = Trip[0][1] #first location
        
        for i in range(len(Trip)):
            Location = Trip[i][1]
            
            if(firstLocation != Location):
                return True
        
        return False      
                                                         
                    
    def Save2Container(self,FromLocation,ToLocation,TravelTime,mac,timestamp):
        if(self.__TravelTime__.has_key(FromLocation)):
            if(ToLocation in self.__Locationlist__): #if both from and to location are on the same corridor - Speedway
                self.__TravelTime__[FromLocation][ToLocation].append(TravelTime)
                self.__MacAddress__[FromLocation][ToLocation].append(mac)
                self.__Timestamp__[FromLocation][ToLocation].append(timestamp)
            
                        
                    
                    
                
                
        
        
'''
Created on Sep 8, 2014

@author: mingchen7
'''

import MySQLdb
import csv
import datetime

class DataQuality:
    
    __BeginTime__ = None
    __EndTmie__= None
    __Threshold__ = None
    __RawTripList__ = None
    __CleanTripList__ = None
    
    def getTripList(self):
        if(self.__CleanTripList__ != None):
            return self.__CleanTripList__
    
    def __init__(self,BeginTime,EndTime):
        self.__BeginTime__ = BeginTime  #initialize the time period
        self.__EndTime__ = EndTime
        self.__Threshold__ = 10  #threshold to determine two non-consecutive trips
        self.__RawTripList__ = []
        self.__CleanTripList__ = []
    
    # Basic function to process trip data
    def ProcessingTripData(self, SqlCursor):
        self.RemoveNontripRecord(SqlCursor)
        self.SeparateTrip()        
    
    def RemoveNontripRecord(self,SqlCursor):
        idxThisRow =  0
        idxIterator = -1
        
        #if have data
        for i in range(SqlCursor.rowcount-1):
                   
            if(i < idxIterator):
                continue
            
            flag = False
            tmpRow = SqlCursor.fetchone() 
            tmpMac = tmpRow[0]
            tmpLocation = tmpRow[1]
               
            idxThisRow = idxIterator = i
            
            nxtRow = SqlCursor.fetchone()

            idxIterator+=1
            
            while(nxtRow[0] == tmpMac): #if the same MAC
                               
                if(nxtRow[1] != tmpLocation):    #two records with the same location
                    flag = True #indicate that these records are trip-based data
                    
                nxtRow = SqlCursor.fetchone()
                idxIterator+=1
                
                if(idxIterator > SqlCursor.rowcount-1):
                    break
            
            SqlCursor.scroll(-1) #roll back for one row
                
            if(flag == True):
                #self.Write2CSV(SqlCursor,idxThisRow,idxIterator)  #write these into CSV files
                self.Save2RawTripList(SqlCursor,idxThisRow,idxIterator)
                
    def SeparateTrip(self):

        for i in range(len(self.__RawTripList__)):
            idxBegin = idxEnd = 0
            
            for j in range(len(self.__RawTripList__[i]) - 1):
                idxNext = j+1
                thisTime = self.__RawTripList__[i][j][2]  #get the time of this record
                nxtTime = self.__RawTripList__[i][idxNext][2] #get the time of the next record
                
                TimeDiff = nxtTime - thisTime
                if(TimeDiff.seconds >= 600):  # Time interval larger than 10 minutes
                    idxEnd = j
                    if(self.CheckSameLocation(i,idxBegin,idxEnd) == False):
                        #save these records as individual trip
                        self.Save2CleanTripList(i, idxBegin, idxEnd) #save into the CleanTripList
                                        
                    if(idxEnd < len(self.__RawTripList__[i])):
                        idxBegin = idxEnd+1  #direct to the next line
                        
                #reach the end of data
                if(idxNext == len(self.__RawTripList__[i]) -1):
                    idxEnd = idxNext
                    if(idxBegin != idxEnd):  #more than two records
                        if(self.CheckSameLocation(i,idxBegin,idxEnd) == False):
                            self.Save2CleanTripList(i, idxBegin, idxEnd)
                            
        self.__RawTripList__ = None
                                                
                    
    def CheckSameLocation(self,idxMac,idxBegin,idxEnd):
        Location = self.__RawTripList__[idxMac][idxBegin][1] #get the location for the first line
        for i in range(idxBegin+1,idxEnd+1):
            nxtLocation = self.__RawTripList__[idxMac][i][1]
            if(Location != nxtLocation):
                return False 
        
        return True  #all the records have the same location
                                                                 
        
    def Save2RawTripList(self,Cursor,idxBegin,idxEnd):
        OneMacTrip = []
        
        for i in range(idxBegin,idxEnd):
            OneMacTrip.append(Cursor._rows[i])
            
        self.__RawTripList__.append(OneMacTrip)    
        
    def Save2CleanTripList(self,idxMac,idxBegin,idxEnd):
        OneMacTrip = []
        
        for i in range(idxBegin,idxEnd+1):
            OneMacTrip.append(self.__RawTripList__[idxMac][i])
            
        self.__CleanTripList__.append(OneMacTrip)
        
    def Write2CSV(self,Cursor,idxBegin,idxEnd):
        Year = self.__BeginTime__[0:4]  #get the year
        Month = self.__BeginTime__[5:7] #get the month
        Date = self.__BeginTime__[8:10]  #get the date
        HourFrom = self.__BeginTime__[11:13] #get the hour
        HourTo = self.__EndTime__[11:13] 
        
        FileName = 'Data\\%s_%s_%s_%s to %s.csv' % (Year,Month,Date,HourFrom,HourTo)
        
        csvfile = open(FileName,'ab')
        writer = csv.writer(csvfile)
        
        for i in range(idxBegin,idxEnd):
            #print Cursor._rows[i]
            writer.writerow(Cursor._rows[i])
            
        writer.writerow("\n")        
        
            
   
            
        
            
                
                
                
                
            
      
    
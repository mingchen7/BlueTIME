'''
Created on Sep 7, 2014

@author: mingchen7
'''

import datetime
import SqlConnect
import os
import DataQuality
import TravelTime
import csv
import LinkTravelTime
import subprocess
import Output
import ImportRawFile
import time

  
def ImputeTravelTime(Date):
    ppath = r'C:\Program Files\R\R-3.1.1\bin\x64\Rscript.exe'
    r_file_name = 'EMScript_UnitCall.R'
    
    #### send parameter into R script
    ### By default, the R script will impute the data for yesterday
    proc = subprocess.Popen("%s %s" % (ppath, r_file_name), stdout=subprocess.PIPE)
    output = proc.stdout.read()

    print output
                  
Speedway = ('Speedway-Euclid','Speedway-Park','Speedway-Mountain','Speedway-Cherry','Speedway-Campbell','Speedway-Tucson','Speedway-Country','Speedway-ElRancho','Speedway-Alvernon','Speedway-Columbus','Speedway-Swan','Speedway-Rosemont','Speedway-Carycroft','Speedway-Wilmot','Speedway-Kolb','Speedway-Prudence','Speedway-Pantano','Speedway-Camino','Speedway-Harrison')

flag_History = True

start_time = time.time()

# REMEMBER SQL TABLE ONLY CREATED WHEN IT'S THE FIRST DAY OF MONTH
if(flag_History == True):
    Start_Year = 2015
    Start_Month = 3
    Start_Day = 20
    End_Year = 2015
    End_Month = 3
    End_Day = 25

    StartDate = datetime.datetime(Start_Year, Start_Month, Start_Day)
    EndDate = datetime.datetime(End_Year, End_Month, End_Day)
    TempTime = StartDate
else:
    # TempDate = datetime.date.today()  # get today's date
    # TempDate = datetime.timedelta(days = -1) # get yesterday
    # year = TempDate.year
    # month = TempDate.month
    # day = TempDate.day
    
    # TempTime = datetime.datetime(year,month,day)
    # EndDate = datetime.datetime(year,month,day)
        
    #Test
    TempTime = datetime.datetime(2014,12,15) 
    EndDate = datetime.datetime(2014,12,15)

interval = 14 #hours
isNewday = True
flag_Output = True # if need to output

while TempTime <= EndDate:
    # import raw data into database (Shu's codes)
    ImportRawData = ImportRawFile.ImportRawData(TempTime)
    ImportRawData.ImportData()     
    
    if(isNewday == True):
        TempTime = TempTime + datetime.timedelta(hours = 6)
        isNewday = False

    BeginTime = TempTime.strftime("%Y-%m-%d %H:%M:%S")
    TempTime = TempTime + datetime.timedelta(hours = interval)
    EndTime = TempTime.strftime("%Y-%m-%d %H:%M:%S")  
    
    print "%s to %s" % (BeginTime,EndTime)
      
    Query = SqlConnect.MySQLQuery()
    Cursor = Query.SelectMacData(BeginTime,EndTime,TempTime)
    
    DQ = DataQuality.DataQuality(BeginTime,EndTime)
    DQ.ProcessingTripData(Cursor)
    TripList = DQ.getTripList()        
    
    if(len(TripList) > 0):
        #F2F
        TT = TravelTime.TravelTimeCalculator(Speedway)
        TT.CalculateTT(TripList,'F2F')
        F2FTT = TT.getTravelTime()
        F2FMac = TT.getMacAddress()
        F2FTimestamp = TT.getTimestamp()
        
        #L2L
        TT2 = TravelTime.TravelTimeCalculator(Speedway)
        TT2.CalculateTT(TripList, 'L2L')
        L2LTT = TT2.getTravelTime()
        L2LMac = TT2.getMacAddress()
        L2LTimestamp = TT2.getTimestamp()
                    
        #Aggregating Link Travel Time
        
        #=======================================================================
        #  
        # if(len(F2FTT) > 0):                        
        #     LinkTT = LinkTravelTime.LinkTravelTimeProcess(15, Speedway)
        #     LinkTT.EstimateLinkTT(F2FTT, L2LTT, F2FMac, F2FTimestamp, BeginTime, EndTime)
        # else:
        #     print "Fail to extract trip-level data!"                                
        #  
        #=======================================================================
                
        #Write TripData and Paired TT Samples                
        if(flag_Output == True):
            output = Output.DataOutput()
            #Trajectory-level Data
            #output.WriteTrajectory2CSV(TripList, BeginTime)
            
            #Trip-level Travel Time
            output.WriteTripTT2CSV(F2FTT, L2LTT, F2FMac, L2LMac, F2FTimestamp, L2LTimestamp, BeginTime)       
            #output.WriteTripTT2DB(F2FTT, L2LTT, F2FMac, L2LMac, F2FTimestamp, L2LTimestamp)
            
            #Link-level Travel Time 
            #LinkTT.InsertEstLinkTT2DB()
            
            #Link-level TT Imputation
            #LinkTT.GenerateImpDataset(BeginTime)
            #ImputeTravelTime(TempTime)  # call R imputation script 
    
    else:
        print "Fail to extract trajectory data"
            
            
    #clear all the variables
    DQ = None
    TripList = None
    TT = None
    TT2 = None
    F2FTT = None
    L2LTT = None
    F2FMac = None
    L2LMac = None
        
    if(TempTime.hour >= 20): #later than 20:00 PM
        TempTime = TempTime + datetime.timedelta(hours = 4)
        isNewday = True

print "End of Execution for %s " % (EndDate)
print "Running time: %s seconds" % (time.time() - start_time)

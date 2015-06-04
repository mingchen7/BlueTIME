'''
Created on Nov 7, 2013

@author: admshuyang
'''

import Connect2DB;
import re;
import datetime;
import os;


class ImportRawData:
    
    __Date__= None
    
    def __init__(self, crtDate):
        self.__Date__ = crtDate

    def NewMacComing_RawData(self,data, connection2DB):
        StringSplit = data.split(';');
        
        if(len(StringSplit) != 3):
            return;
            
        DetectedTimeString = StringSplit[0];
        
        Year_Month = DetectedTimeString.split('-');
        Year = Year_Month[0];
        Month = Year_Month[1];
        
        Location = StringSplit[1];
        MacString = StringSplit[2].split('\'');
        
        for i in range((len(MacString)-1)/2):
            #print MacString[2 * i + 1];
            connection2DB.InsertIndividualMAC(Year, Month, MacString[2 * i + 1], Location, DetectedTimeString, None);
            
    def ImportData(self):
                
        # StartTime = datetime.datetime(Start_Year, Start_Month, Start_Date);
        # EndTime = datetime.datetime(End_Year, End_Month, End_Date);
                
        # Today = datetime.datetime(2015,3,3)
        
        Connection = Connect2DB.MacData2MySQL();
        
        # try to create a new table using the initial information
        if(self.__Date__.day == 1):
            Connection.CreateNewTable('{0:04d}'.format(self.__Date__.year), '{0:02d}'.format(self.__Date__.month));
        
        # import the data each day (from [Start_Year,Start_Month] to [End_Year, End_Date] )
        Year = self.__Date__.year;
        Month = self.__Date__.month;
        Date = self.__Date__.day;
        
        for hour in range(24):     
            FileName = 'Plant Files\\{0:04d}\\{0:04d}_{1:02d}\\{0:04d}_{1:02d}_{2:02d}\\{0:04d}_{1:02d}_{2:02d}_{3:02d}.txt'.format(Year, Month, Date, hour);
            
            if (os.path.exists(FileName) == False):
                continue;
            
            RawFile = open(FileName, 'r');
            
            for line in RawFile:
                self.NewMacComing_RawData(line, Connection);
                
            Connect2DB.Connect2MySQL.GetInisitance().commit();
            print(FileName);
        
        """
        Month_Previous = TempTime.month;
        TempTime = TempTime + datetime.timedelta(days = 1);
        Month_Next = TempTime.month;
        
        if(Month_Previous != Month_Next):
            Connection.CreateNewTable('{0:04d}'.format(TempTime.year), '{0:02d}'.format(TempTime.month));
        """
        
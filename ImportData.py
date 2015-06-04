'''
Created on Sep 6, 2014

@author: mingchen7
Import data from txt files and insert into database 
'''

import datetime
import SqlConnect
import os

def InsertNewRow(OneLine,SqlQuery):
    strSplit = OneLine.split(';')
    TimeString = strSplit[0]
    Location = strSplit[1]
    MacSequence = strSplit[2].split('\'')
        
    for i in range((len(MacSequence)-1)/2):
        #print MacSequence[2*i+1]
        #Query=SqlConnect.MySQLQuery()
        SqlQuery.InsertMacDataRow(TimeString,Location,MacSequence[2 * i + 1])


#Import data between xxxx-xx-xx to xxxx-xx-xx
Start_Year = 2014;
Start_Month = 6;
Start_Date = 30;
End_Year = 2014;
End_Month = 6;
End_Date = 30;

StartTime = datetime.datetime(Start_Year, Start_Month, Start_Date);
EndTime = datetime.datetime(End_Year, End_Month, End_Date);

TempTime = StartTime;

while TempTime <= EndTime:
    Year = TempTime.year;
    Month = TempTime.month;
    Date = TempTime.day;

    for hour in range(24):
        FileName = '/home/matthew/Desktop/MacData/{0:04d}/{0:04d}_{1:02d}/{0:04d}_{1:02d}_{2:02d}/{0:04d}_{1:02d}_{2:02d}_{3:02d}.txt'.format(Year, Month, Date, hour)
        
        if (os.path.exists(FileName) == False):
            continue;
        
        f=open(FileName,"r")
        SqlQuery=SqlConnect.MySQLQuery()
        
        count_row=0
        for line in f:
            InsertNewRow(line,SqlQuery)
            count_row=count_row+1
            print count_row
        
        print FileName + "is done!" 
        print "The total number of rows is %d" % count_row
            
    TempTime = TempTime + datetime.timedelta(days = 1);

        












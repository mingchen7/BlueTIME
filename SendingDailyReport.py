'''
Created on Oct 12, 2014

@author: mingchen7
'''

import datetime
import time
import os
import CheckStationStatus
import sys
import re
from smtplib import SMTP_SSL as SMTP       # this invokes the secure SMTP protocol (port 465, uses SSL)
# from smtplib import SMTP                  # use this for standard SMTP protocol   (port 25, no encryption)
from email.MIMEText import MIMEText
import SqlConnect


# Generate report content
# Version 1 - for daily report
def GenerateReport(Reportday,StationList):
    strReport = ""
    dicTotal = {}
    
    for location in StationList:
        dicTotal[location] = 0;
    
    Year = Reportday.year
    Month = Reportday.month
    Day = Reportday.day
    
    for Hour in range(24):

        BeginTime = Reportday + datetime.timedelta(hours = Hour)
        EndTime = BeginTime + datetime.timedelta(hours = 1)
        BeginTime = BeginTime.strftime("%Y-%m-%d %H:%M:%S")
        EndTime = EndTime.strftime("%Y-%m-%d %H:%M:%S")            
        
        FilePath = 'Raw Data/{0:04d}/{0:04d}_{1:02d}/{0:04d}_{1:02d}_{2:02d}'.format(Year, Month, Day)
        FileName = 'Raw Data/{0:04d}/{0:04d}_{1:02d}/{0:04d}_{1:02d}_{2:02d}/{0:04d}_{1:02d}_{2:02d}_{3:02d}.txt'.format(Year, Month, Day, Hour)
        if(os.path.exists(path = FilePath)):
            if(os.path.isfile(FileName)):
                f = open(FileName,'r')
            else:
                strReport = strReport +  "File %s does not exist!\n" % (FileName)
                continue
        else:
            return "File path %s does not exist!\n" % (FilePath)
                        
        SS = CheckStationStatus.StationStatus(StationList)
        dic = SS.checkStatus(f)     
        
        if(Hour == 5):  #record the 5am to 6am sample counts
            strReport = strReport + "5:00 AM to 6:00 AM:\n"   
            for location in dic: 
                line = "  %s, %d\n" % (location, dic[location])
                strReport = strReport + line
            strReport = strReport + "\n"
                                
        for location in dic: #adding the total day sample counts
            dicTotal[location] = dicTotal[location] + dic[location] 
         
    strReport = strReport + "\nTotal Sample Counts:\n"
    for location in dicTotal:        
            line = "  %s, %d\n" % (location, dicTotal[location])
            strReport = strReport + line
                
    strReport = strReport + "\n"    
    return strReport


# Getting total sample size for each location each day
# Version 2 - for history report
def GetTotalSamples(Reportday,StationList):    
    dicTotal = {}
    
    for location in StationList:
        dicTotal[location] = 0;
    
    Year = Reportday.year
    Month = Reportday.month
    Day = Reportday.day
    
    for Hour in range(24):

        BeginTime = Reportday + datetime.timedelta(hours = Hour)
        EndTime = BeginTime + datetime.timedelta(hours = 1)
        BeginTime = BeginTime.strftime("%Y-%m-%d %H:%M:%S")
        EndTime = EndTime.strftime("%Y-%m-%d %H:%M:%S")
               
        FilePath = 'Raw Data/{0:04d}/{0:04d}_{1:02d}/{0:04d}_{1:02d}_{2:02d}'.format(Year, Month, Day)
        FileName = 'Raw Data/{0:04d}/{0:04d}_{1:02d}/{0:04d}_{1:02d}_{2:02d}/{0:04d}_{1:02d}_{2:02d}_{3:02d}.txt'.format(Year, Month, Day, Hour)
        if(os.path.exists(path = FilePath)):
            if(os.path.isfile(FileName)):
                f = open(FileName,'r')
            else:
                print "File %s does not exist!\n" % (FileName)                
                continue
        else:
            print "File path %s does not exist!\n" % (FilePath)
            return -1
                        
        SS = CheckStationStatus.StationStatus(StationList)
        dic = SS.checkStatus(f)     
                                        
        for location in dic: #adding the total day sample counts
            dicTotal[location] = dicTotal[location] + dic[location] 
          
    return dicTotal


def SendingEmail(email,password,message,date):
    SMTPserver = 'smtp.gmail.com'
    sender = email
    receivers = ['mingchen7@email.arizona.edu','shuyang@email.arizona.edu']    
    
    # typical values for text_subtype are plain, html, xml
    text_subtype = 'plain'
        
    content = message
    
    subject="Bluetooth Data Daily Report: %s" % (date)
    
    USERNAME = email
    PASSWORD = password
    
    try:
        msg = MIMEText(content, text_subtype)
        msg['Subject']= subject
        msg['From'] = sender # some SMTP servers will do this automatically, not all
    
        conn = SMTP(SMTPserver)
        conn.set_debuglevel(False)
        conn.login(USERNAME, PASSWORD)
        try:
            conn.sendmail(sender, receivers, msg.as_string())
        finally:
            conn.close()
    
    except Exception, exc:
        sys.exit( "mail failed; %s" % str(exc) ) # give a error message


def DailyReport(Corridors):    
    Today = datetime.datetime.today()
    Yesterday = Today + datetime.timedelta(days = -1)
    Year = Yesterday.year
    Month = Yesterday.month
    Day = Yesterday.day
    
    # Reportday = datetime.datetime(Year,Month,Day)
    Reportday = datetime.datetime(2015,3,24)
    
    msg = ""
    
    for Corridor in Corridors.keys():
        msg = msg + "%s:\n" % (Corridor);
        msg = msg + GenerateReport(Reportday, Corridors[Corridor])
        
    SendingEmail('smarttranslab@gmail.com', 'googleaccountpwd', msg, Reportday)
        
def HistoryReport(Corridors):
    Start_Year = 2014
    Start_Month = 9
    Start_Day = 1
    End_Year = 2014
    End_Month = 12
    End_Day = 31

    StartDate = datetime.datetime(Start_Year, Start_Month, Start_Day)
    EndDate = datetime.datetime(End_Year, End_Month, End_Day)
    TempDate = StartDate
    
    Query = SqlConnect.MySQLQuery()
    dicIntID = Query.GetIntersectionTable()
    
    while TempDate <= EndDate:
        for corridor in Corridors:            
            dicTotal = GetTotalSamples(TempDate, corridor)
            if dicTotal != -1:        
                for location in dicTotal: 
                    INT_id = dicIntID[location]
                    ReportDate = TempDate
                    
                    if dicTotal[location] > 0:
                        BTStatus = 1
                    else:
                        BTStatus = 0
                    
                    TotalSamples = dicTotal[location]
        
                    Query.InsertBTStatusRecord(INT_id, ReportDate, BTStatus, TotalSamples)
                    print "%s, %s" % (TempDate,location)
            
        TempDate = TempDate + datetime.timedelta(days = 1)

def main():    
    Corridors = {}
    Speedway = ('Speedway-Euclid','Speedway-Park','Speedway-Mountain','Speedway-Cherry','Speedway-Campbell','Speedway-Tucson','Speedway-Country', \
                'Speedway-ElRancho','Speedway-Alvernon','Speedway-Columbus','Speedway-Swan','Speedway-Rosemont','Speedway-Carycroft','Speedway-Wilmot',\
                'Speedway-Kolb','Speedway-Prudence','Speedway-Pantano','Speedway-Camino','Speedway-Harrison')
    
    Broadway = ('Broadway-Aviation','Broadway-Euclid','Broadway-Highland','Broadway-Campbell','Broadway-Tucson','Broadway-CountryClub','Broadway-Randolph','Broadway-Dodge','Broadway-Alvernon')    
    Oracle = ('Grant-Oracle','Glenn-Oracle','Miraclemile-Oracle','Prince-Oracle','Roger-Oracle','Limberlost-Oracle','Wetmore-Oracle','TucsonMall-Oracle','River-Oracle');
              
    Corridors["Speedway"] = Speedway;
    Corridors["Broadway"] = Broadway;
    Corridors["Oracle"] = Oracle; 
    
    # DailyReport(Corridors)
    # HistoryReport(Corridors)
    
    
    while True:
        time.sleep(1 * 3600)        
        if (datetime.datetime.now().hour == 8):            
            DailyReport(Corridors)        
        
if __name__ == "__main__":
    main(); 
        
library("Amelia")
# library("VIM")
library("sm")
# library("ggplot2")
library("RMySQL")

#initialization
rm(list=ls())
setwd("C:\\Users\\mingchen7\\workspace\\Bluetooth\\src")
Interval<-15 #minutes
TotalIntvl<-(14/(Interval/60))

getHardThreshold<-function(FROM_INT,TO_INT)
{
  #initial value
  speedway.thresholds<-c(10,15,15,14,30,30,17,42,30,30,30,30,62,59,30,30,59,60)
  
  if(FROM_INT < TO_INT)
  {
    idxUp<-FROM_INT
    idxDown<-TO_INT - 1
  }
  else
  {
    idxUp<-TO_INT
    idxDown<-FROM_INT - 1
  }
  tmpThresholds<-speedway.thresholds[idxUp:idxDown]  
  HardThreshold<-sum(tmpThresholds)
  return(HardThreshold)
}

getTimestamp<-function(Date,TOD)
{
  Date<-as.POSIXlt(Date)
  Timestamp<-Date + (TOD + 24 - 1) * 15 * 60
  return(Timestamp)
}

# today<-Sys.Date()
# test
today<-as.Date("2014-12-15")

for(direction in 1:2)
{
  day<-as.numeric(format(today, "%d"))
  month<-as.numeric(format(today, "%m"))
  year<-as.numeric(format(today, "%Y"))
  
  if(direction == 1)
  {
    filename<-sprintf("LinkTT/LinkTT_%4d_%02d_%02d_EB.csv",year,month,day)  
  }
  else
  {
    filename<-sprintf("LinkTT/LinkTT_%4d_%02d_%02d_WB.csv",year,month,day)  
  }
  
  
  if(file.exists(filename) == FALSE)
  {
    # today<-today+1
    print("Terminiated: imputing File already exists!")
  }
  
  #loading data
  TT.missing<-read.csv(filename, header=T)
  TT.missing$Day<-as.Date(TT.missing$Day)
  TT.missing$idxDay<-as.numeric(format(TT.missing$Day,"%d"))  
  TT.missing$idxTime<-TT.missing$idxDay + TT.missing$TOD / TotalIntvl
  
  n<-ncol(TT.missing)
  nend<-n-2
  
  #pre-configuration
  LinkNames<-names(TT.missing)[3:nend]
  Nimpcols<-nend-2
  columns<-c(2:(Nimpcols+1))
  lowbnd<-NULL
  
  for(i in 1:Nimpcols)
  {
    tmpStrings<-unlist(strsplit(LinkNames[i],"_"))
    int.from<-as.numeric(substr(tmpStrings[1],2,10000))
    int.to<-as.numeric(tmpStrings[2])
    lowbnd[i]<-getHardThreshold(int.from, int.to) 
  }
  
  upbnd<-replicate(Nimpcols,1000)
  
  #Imputation
  bds<-matrix(c(columns,lowbnd,upbnd),nrow=Nimpcols,ncol=3)
  EM.output<-amelia(TT.missing[,2:nend],m=5,bounds=bds, ts="TOD",polytime = 2, max.resample=1000)
  
  #insert data into MySQL
  maxrow<-nrow(TT.missing)
  
  row<-maxrow
  sumTT<-0
  
  #open database connection
  conn<-dbConnect(MySQL(),user="ming",password="password",dbname="tucsonbttt",host="128.196.93.142")
  
  while((row >= 0) && (TT.missing[row,1] == today))
  {
    history.date<-as.character(TT.missing[row,1])
    TOD<-TT.missing[row,2]  
    for(i in 1:length(LinkNames))
    {
      Colname<-LinkNames[i]
      tmpStrings<-unlist(strsplit(LinkNames[i],"_"))
      int.from<-as.numeric(substr(tmpStrings[1],2,10000))
      int.to<-as.numeric(tmpStrings[2])
      
      for(j in 1:5)
      { 
        sumTT<-sumTT + EM.output$imputations[[j]][row,i+1]  #i refers to linkname
      }
      avgTT<-sumTT / 5 #average TT
      sumTT<-0
      
      #Insert Query    
      # query<-paste("INSERT INTO LINKTT_15M_IMP VALUES(",int.from,",",int.to,",","\'",history.date,"\',",TOD,",",avgTT,")")
      time<-getTimestamp(history.date,TOD)
      query<-paste("INSERT INTO LINKTT_15M_IMP VALUES(",int.from,",",int.to,",","\'",history.date,"\',",TOD,",",avgTT,",","\'",time,"\')")
      dbSendQuery(conn,query)            
    }
    
    row<-row - 1
  }
  
  dbDisconnect(conn)
}

print("Link-level Travel Time Imputed and Stored in the database.")











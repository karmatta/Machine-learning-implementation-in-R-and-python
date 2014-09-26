#******************************************************************************
# Name of script       : consumerAgentGameboardFixedTime.R
# Script Details       : Charges the dataframe for the Gameboard agency for a fixed time period. 
# Package version      : 0.1
# Script version       : 0.1
# Date                 : 29/05/2013
# Author(s)             : Avinash Joshi
# Task id(s)  	       : 211 - http://182.71.223.28/agilefant/qr.action?q=story:211
# Git link             : https://github.com/Mu-Sigma/labs/blob/mint-dev/Market_Gameboard/consumerAgentGameboardFixedTime.R
#******************************************************************************

#==============================
# Get the environment variables
#==============================
mripPath = Sys.getenv("MRIP_HOME")

#=================================
# Source the agency utilities file
#=================================
source(paste(common__,"/gameBoardUtilities.R",sep=""))

#=================================
# Setting futile logger threshold
#=================================
logNS <- "consumerAgentGameboardFixedTime"
flog.threshold(INFO, name=logNS)
flog.appender(appender.file(paste(mripPath,"/MINTlogs/consumerAgentGameboardFixedTime.log",sep="")), name=logNS)

flog.info("Sourcing the file - start.", name=logNS)
#====================================
# Check R version and package version
#====================================
CheckRversion(logNS=logNS)

checPkgVec[1] <- CheckPackVer("RPostgreSQL", '0.3-3', logNS=logNS)
checPkgVec[2] <- CheckPackVer("Rjms", '0.0.4', logNS=logNS)
checPkgVec[3] <- CheckPackVer("rjson", '0.2.12', logNS=logNS)

if(any(checPkgVec != 0)){
  errorCode <- 11
}

#===========================
# Loading required libraries
#===========================
require("RPostgreSQL")
require('Rjms')
require("rjson")

#======================
# Initialize parameters
#======================

consumer <- initialize.consumer(activeMQIp,'T','jadeStockStream')
frameSize <- 2
nSymbols <- length(symbolVec)
dfMatrix <- matrix(-1,nrow=frameSize,ncol=nSymbols+1)
fillCount <- 1
retDf <- as.data.frame(matrix(-1, nrow = 1, ncol=nSymbols+1))
colnames(retDf) <- symbolVec
tradeTime <- "\"NULL\""
marketIndicator <- "NULL"
retFrameCount <- 0
updateCount <- 10
returnsDfFrameSize <- 2000
# set the period in which to compute the returns (in secs)
timeDiff <- 1
closedFlag<-TRUE
refreshFlag<-FALSE

#______________________________________________________________________________________________________________________________________________________________
# Querying the database for the initial values for the first row of the data frame
#_______________________________________________________________________________________________________________________________________________________________

drv <- dbDriver("PostgreSQL")

readFromDb = function(){
  tryCatch({con <- dbConnect(drv, host=dbProperties[1], port = as.numeric(dbProperties[2]), dbname = dbProperties[3], user = dbProperties[4], pass=dbProperties[5])}, error=function(err){errorCode<-200}, finally={})
  
  # check if the connection was established
  if(exists("con")){
    rs <- dbSendQuery(con, statement = "select lasttimestamp from mstream_lasttime")
    dateTime <- fetch(rs, n = -1)
    dbNameS <- paste("stockdto_objects_", dateTime, sep="")
    dbNameI <- paste("indexdata_", dateTime, sep="")
    syms <- paste("'",symbolVec, "'", sep="")
    lastPriceVecS = lapply(1:length(syms), function(i){
      rs <- dbSendQuery(con, statement = paste("select buy_price from", dbNameS, "where symbol =", syms[i] ,"order by stockcreate_datetime DESC LIMIT 1"))
      lastP <- fetch(rs, n = -1)
      
      if(nrow(lastP)==0){
        lastP=-1
      }
      return(lastP)
    })
    ii = which(lastPriceVecS==-1)
    lastPriceVecI = lapply(1:length(ii), function(i){
      rs <- dbSendQuery(con, statement = paste("select last_price from", dbNameI, "where symbol =", syms[ii[i]] ,"order by indexcreate_datetime DESC LIMIT 1"))
      lastP <- fetch(rs, n = -1)
      if(nrow(lastP)==0){
        lastP=-1
      }
      return(lastP)
    })
    lastPriceVecS[ii] <- lastPriceVecI
    rs <- dbSendQuery(con, statement = paste("select stockcreate_datetime from", dbNameS, "order by stockcreate_datetime DESC LIMIT 1"))
    lastTime <- as.POSIXct(strptime(fetch(rs, n = -1), "%Y-%m-%d %H:%M:%OS"))
    dfMatrix[1,1:length(symbolVec)] <<- unname(unlist(lastPriceVecS))
    dfMatrix[1,length(symbolVec)+1] <<- unname(lastTime)
    dbDisconnect(con)
    dbClearResult(rs)
    flog.info("Successfully read the last updated prices from the data base", name=logNS)
  }else{
    errorCode <<- 200
    flog.fatal("Failed to read the last updated prices from the data base!", name=logNS)
  }
}

# call the above function
readFromDb()


#-----------------------------------------------------------------------------
# Consume from the Queue and drop the extra information
# Args: None
# Returns: A string. The last consumed message from  the queue
#-----------------------------------------------------------------------------

consumeFn<-function(){
  z <- consume(consumer,asString=TRUE)  
  a1 <- strsplit(strsplit(z,split="args\":")[[1]][2],split=",")[[1]]
  a3 <- gsub('[^ a-zA-Z0-9:.-]', '',a1)
  return(paste(a3[-c(8,9)],collapse=","))
}

#______________________________________________________________________________________
# data frame charging function
#______________________________________________________________________________________

dfChargeUsePrev <- function(tickerId, askPrice, tradeTime){
  temp <- gsub('.*:','', tradeTime)
  tradeTime <- sub(paste(':',temp,sep=""),paste(':',temp,sep=""), tradeTime)
  if(!is.na(askPrice)){
    symIndex <- which(symbolVec == tickerId)
    #First fill row no 1 in the matrix
    if(is.element(-1,as.vector(dfMatrix[1,]))){
      dfMatrix[1,symIndex] <<- askPrice
      dfMatrix[1,(nSymbols+1)] <<- as.POSIXct(strptime(tradeTime,"%Y-%m-%d %H:%M:%OS"))
      dFrame = data.frame(dfMatrix)
      colnames(dFrame) <- c(symbolVec,'Time')
      return(list("dFrame"=dFrame,"full"=FALSE))
    }
    #We have a complete row now
    else{
      if(fillCount == frameSize){
        #Pop the first element and insert at bottom
        tVec <- dfMatrix[fillCount[1],]
        dfMatrix[1:frameSize-1,] <<- dfMatrix[2:frameSize,]
        dfMatrix[frameSize,] <<- tVec
        dfMatrix[frameSize,symIndex] <<- askPrice
        dfMatrix[frameSize,(nSymbols+1)] <<- as.POSIXct(strptime(tradeTime,"%Y-%m-%d
                                                                 %H:%M:%OS"))
        dFrame <- data.frame(dfMatrix)
        colnames(dFrame) <- c(symbolVec,'Time')
        return(list("dFrame"=dFrame,"full"=TRUE))
      }
      else{
        #Add the element
        fillCount <<- fillCount + 1
        #Replicate previous value
        dfMatrix[fillCount,] <<- dfMatrix[fillCount-1,]
        dfMatrix[fillCount,symIndex] <<- askPrice
        dfMatrix[fillCount,(nSymbols+1)] <<- as.POSIXct(strptime(tradeTime,"%Y-%m-%d
                                                                 %H:%M:%OS"))
        if(fillCount == frameSize){
          dFrame <- data.frame(dfMatrix)
          colnames(dFrame) <- c(symbolVec,'Time')
          return(list("dFrame"=dFrame,"full"=TRUE))
        }
        else{
          dFrame = data.frame(dfMatrix)
          colnames(dFrame) <- c(symbolVec,'Time')
          return(list("dFrame"=dFrame,"full"=FALSE))
        }
      }
    }
  }
}

#_____________________________________________________________________________________
# Function to create a data frame consisting of all the tickers
#______________________________________________________________________________________

dfCharge_for_multi_ticks = function(streamData){
  
  askPrice <- as.numeric(gsub(".*?(askPrice|lastPrice):(.*?),.*","\\2",streamData))
  tickerId <- gsub(".*?symbol:(.*?),.*","\\1",streamData)
  tradeTime <<- gsub(".*?CreateDateTime:(.*?),.*","\\1",streamData)
  marketIndicator <<- gsub(".*?marketStatus:(.*?).*?","\\1",streamData)
  res <- dfChargeUsePrev(tickerId,askPrice,tradeTime)
  res <- res$dFrame
  
  if(marketIndicator == "refreshed"){
    refreshFlag <<- TRUE
  }
  
  # If the connection to the database was failed
  if (errorCode != 200){
    res <- res[-1,]
    # Now we have a data frame of row 1 which is called after a certain period
    return(res)
  } else {
    return(res[1,])
  }  
}

#_____________________________________________________________________________________
# Function to create a data frame consisting of all the tickers for a specified period
#______________________________________________________________________________________

chargeDfForReturns = function(res){
  
  if (retFrameCount == 0){
    retDf <<- res
    retFrameCount <<- retFrameCount + 1
  } else {
    retDf[nrow(retDf)+1,] <<- res
    retFrameCount <<- retFrameCount + 1
  }
  # if the frame size limit is reached, pop the first row of the DF
  if(returnsDfFrameSize < retFrameCount){
    retDf <<- retDf[-1,]
    retFrameCount <<- retFrameCount - 1
  }
  
  return(retDf)
}

MainChargeDfFixedTime <- function(){
  
  if (errorCode != 0){
    return(paste("{\"ErrorCode:",errorCode,"\"}",sep=""))
  }
  
  if(!is.na(marketIndicator)){
    if(marketIndicator == "closed" || marketIndicator == "open"){
      closedFlag<<-TRUE
    }
    if(refreshFlag && closedFlag){
      retDf <<- as.data.frame(matrix(-1, nrow = 1, ncol=nSymbols))
      colnames(retDf) <<- symbolVec
      retFrameCount <<- 0
      dfMatrix <<- matrix(-1,nrow=frameSize,ncol=nSymbols+1)
      fillCount <<-1
      flog.info("Market opened/poller refreshed", name=logNS)
      readFromDb()
      closedFlag <<- FALSE
      refreshFlag <<- FALSE
    }
  }
  
  for(i in 1:updateCount){
    # charge the data frame for exactly timeDiff secs
    timeLim = Sys.time() + timeDiff
    while(Sys.time() < timeLim){
      # check for ActiveMQ connection
      tryCatch({streamData <- consumeFn()}, 
               error=function(err){errorCode<<-400
                                   flog.error("Did not receive data from the queue on %s",activeMQIp,name=logNS)
                                   return(paste("[\"ErrorCode:",errorCode,"\"]",sep=""))
               },
               finally={})
      tickerId <- gsub(".*?symbol:(.*?),.*","\\1",streamData)
      # res1 is the data frame that contains 1 latest row of data
      
      if ((errorCode != 400) && exists("streamData")){
        res1 <<- dfCharge_for_multi_ticks(streamData)
      } else {
        errorCode <<- 0
        flog.fatal("The queue on %s is disconnected!",activeMQIp, name=logNS)
        flog.info("Error:The consumer queue is down!", name=logNS)
        Sys.sleep(timeDiff * updateCount)
        # send the last updated json with the error code of 400
        return(paste(prevJson,"\"ErrorCode\":400}", sep=","))
      }
      
    }
    
    res2 <- NULL
    
    if(!(-1 %in% res1) && !is.null(res1)){
      # Charge the dataframe where each row is a timeLim period of data
      res2 <- chargeDfForReturns(res1)
    }
  }
  
  if(is.null(res2)){
    errorCode <- 701
    flog.error("Data frame was not charged. -1 exists in the first row.", name=logNS)
    return(return(paste("[\"ErrorCode:",errorCode,"\"]",sep="")))
  }
  
  res2 <- res2[,-ncol(res2)]
  dfSize <- paste(retFrameCount, "/", returnsDfFrameSize,sep="")
  resList <- list("DF" = res2, "TimeStamp" = tradeTime, "marketStatus" = marketIndicator,"DataFrameSize"=dfSize, "ErrorCode" = errorCode)
  resList <- toJSON(resList)
  
  flog.info("DataFrame sent succesfully", name=logNS)

  return(resList)
#   if(marketIndicator == "open"){
#     flog.info("DataFrame sent succesfully", name=logNS)
#     return(resList)
#   } else {
#     flog.info("marketIndicator sent succesfully", name=logNS)
#     return(marketIndicator)
#   } 
}

#-------------------------------------------
# Clean up function when the agent is killed
#-------------------------------------------

cleanUp <- function(){
  flog.info("The agent %s in agency %s has been killed. Clean up function evoked",
            agent__,agency__, name="info_consume")
  
  tryCatch(destroy.consumer(consumer),
           error=function(err){
             flog.error("Error in disconnecting form ActiveMQ", name="error_consume")
           }
  )
  
  if(exists("con")) dbDisconnect(con)
  if(exists("con1")) dbDisconnect(con1)
  
  rm(list=ls())
  flog.info("Clean up done.")         
}

flog.info("Sourcing the file - end.", name=logNS)
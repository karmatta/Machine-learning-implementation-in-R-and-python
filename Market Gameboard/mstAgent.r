#******************************************************************************
# Name of script       : corMst.R
# Script Details       : Computes correlation matrix and mst. 
#                        writes it to a database.
#                        Sends the latest cor/mst as a json.
# Package version      : 0.1
# Script version       : 0.1
# Date                 : 22/05/2013
# Author(s)            : Karthik D
# Task id(s)  	       : 211 - http://182.71.223.28/agilefant/qr.action?q=story:211
# Git link             : https://github.com/Mu-Sigma/labs/blob/mint-dev/Market_Gameboard/corMStAgent.R
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
logNS <- "corMstAgent"
flog.threshold(INFO, name=logNS)
flog.appender(appender.file(paste(mripPath,"/MINTlogs/mstAgentGB.log",sep="")), name=logNS)

flog.info("Sourcing the file - start.", name=logNS)

#====================================
# Check R version and package version
#====================================
CheckRversion(logNS="corMstAgent")
checPkgVec[1]<-CheckPackVer("ape",'3.0-6',logNS=logNS)
checPkgVec[2]<-CheckPackVer("rjson",'0.2.11',logNS=logNS)
checPkgVec[3] <- CheckPackVer("ff",'2.2-11',logNS=logNS)

if(any(checPkgVec != 0)){
  errorCode <- 11
}

#===========================
# Loading required libraries
#===========================
require("ape")
require("rjson")
require("ff")

#=====================================
# Initialize variables for this script
#=====================================
fieldsJson <- NULL
whichMat <- "mst"
prevMstLength <- 0
chngPercent <- 0.15

#========================
# Function to get the MST 
#========================

getMst <- function(distMat,corMat){
  mstMat <- mst(distMat) 
  mstLength <- sum(mstMat*distMat)
  mstMat <- mstMat * corMat
  return(list("mst" = mstMat, "mstLength" = mstLength))
}

#===========================
# Create Json for this agent
#===========================

createJson <- function(corMat){
  
  #Calling community and centrality function
  
  
  values<- corMat[lower.tri(corMat, diag = F)]
  dims <- which(lower.tri(corMat, diag = F), arr.ind =T)-1
  
  #-------------------------------------------------------------------------------------------------------
  # Correlation Json (jsonCor)
  #-------------------------------------------------------------------------------------------------------
  jsonCor = "\"graph\":"
  jsonCor[1]<-paste(jsonCor, "[{\"source\":", dims[1,2],",", "\"target\":", dims[1,1],",", "\"value\":", values[1],"}")
  jsc<-lapply(2:length(values), function(i)
    jsonCor[i]<- paste("{\"source\":", dims[i,2],",", "\"target\":", dims[i,1],",", "\"value\":", values[i],"}")) 
  jscfinal<-paste(jsc,sep=",",collapse=",")
  jsonCor<-paste(jsonCor,",",jscfinal, "]")
  #_______________________________________________________________________________________________________
  
  
  #-------------------------------------------------------------------------------------------------------
  # Nodes Json
  #-------------------------------------------------------------------------------------------------------
  nodes = paste("\"nodes\":[{\"id\":","\"", symbolVec[1],"\"}", sep ="")
  for(i in 2:length(symbolVec)){
    nodes <- paste(nodes, paste("{\"id\":","\"", symbolVec[i],"\"}", sep =""), sep=",")
  }
  nodes<- paste(nodes, "]")
  #-------------------------------------------------------------------------------------------------------
  
  return (paste( "{", paste(jsonCor, nodes,sep=",")))
}

#==============
# Main function
#==============
MainComputeMst <- function(inputDfJson){
  if (!any(errorCode == c(0,100)) && prevJson == ""){
    flog.error("Error in sourcing the file. ErrorCode is %d",errorCode,name=logNS)
    return(paste("{\"ErrorCode:",errorCode,"\"}",sep=""))
  }
  
  tryCatch({inputList <-fromJSON(inputDfJson)
            fieldsJson <<- paste("\"TimeStamp\"",gsub(".*?\"TimeStamp\"(.*?)}","\\1",inputDfJson),sep="")
            errorCode <<- 0},
           error = function(err){ 
             errorCode <<- 702
             flog.error("Received an invalid Json from the above agent.\n %s",inputDfJson, name=logNS)
           },
           finally = {})
  
  if (!any(errorCode == c(0,100)) && prevJson == ""){
    flog.error("Error in sourcing the file. ErrorCode is %d",errorCode,name=logNS)
    return(paste("{\"ErrorCode:",errorCode,"\"}",sep=""))
  }
  
  errorCode <- inputList$ErrorCode
  dataFrame <- as.data.frame(inputList$DF)
  timeStamp <- inputList$TimeStamp
  marketIndicator <- inputList$marketStatus
  numTimeStamp <- as.numeric(as.POSIXct(timeStamp))
  
  if (!any(errorCode == c(0,100)) && prevJson == ""){
    flog.error("Received an error from the above agent in the first call so sending just the error code. ErrorCode is %d",errorCode,name=logNS)
    return(paste("[\"ErrorCode:",errorCode,"\"]",sep=""))
  }
  
  if (!any(errorCode == c(0,100))){
    flog.error("Received an error from the above agent sending the previous json with the error code. ErrorCode is %d",errorCode,name=logNS)
    
    return(paste(prevJson,",",fieldsJson,"}",sep=""))
  }
  
  if (nrow(dataFrame) < minDfSize){
    flog.info("minimum df size is %d", minDfSize, name=logNS)
    errorCode <- 100
    retJson <- "{\"graph\":\"NULL\""
    prevJson <<- retJson
    fieldsJson <- gsub("\"ErrorCode\":[0-9]{1,10}",paste("\"ErrorCode\":",errorCode,sep=""),fieldsJson)
    retJson <- paste(retJson,",",fieldsJson,"}",sep="")
    flog.info("Json sent successfully", name=logNS)
    return(retJson)
    
  }
  
  dataFrame <- as.matrix(dataFrame)
  flog.info("Received the dataFrame.", name=logNS)
  nSymbols <- ncol(dataFrame)
  frameSize <- nrow(dataFrame)
  tryCatch(corMat <- cor(dataFrame), 
           error = function(err){
             flog.error("Error in calculating correlation matrix. The error message is %s",err)
             errorCode <<- 200
           },
           finally = {})
  
  corMat[which(is.na(corMat))] <- 0
  corMat <- round(corMat, digits=3)
  writeCorMat <- c(numTimeStamp,as.vector(corMat[lower.tri(corMat,diag=F)]))
  
  
  distMat <- sqrt(2*(1-abs(corMat)))
  
  tryCatch(mstList <- getMst(distMat,corMat), 
           error = function(err){
             flog.error("Error in calculating mst. The error message is %s",err)
           },
           finally = {})

  mstLength <- mstList$mstLength
  mstMat <- as.matrix(mstList$mst)
  mstMat <- round(mstMat, digits=3)
  writeMstMat <- c(numTimeStamp,as.vector(mstMat)) 
  writeMstMat <- c(numTimeStamp,as.vector(mstMat[lower.tri(mstMat,diag=F)]))
  writeMstLength <- c(numTimeStamp,mstLength)
  save("mstLength",file=paste(common__,"/latestMstLength.rdata",sep=""))
  
  
  if(marketIndicator == "open"){
    if(firstCall){
      mrketOpenTime <- numTimeStamp
      firstCall <<- FALSE
    }
    write.csv.ffdf(as.ffdf(as.ff(t(writeCorMat))),file=paste(common__,"/correlationsTable.csv",sep=""),append=TRUE)
    
    write.csv.ffdf(as.ffdf(as.ff(t(writeMstMat))),file=paste(common__,"/mstTable.csv",sep=""),append=TRUE)
    
    write.csv.ffdf(as.ffdf(as.ff(t(writeMstLength))),file=paste(common__,"/mstLengthTable.csv",sep=""),append=TRUE)
    
     flog.info("Wrote the matrices to a file", name=logNS)
    
  } else if(marketIndicator == "closed"){
    firstCall <<- TRUE
  }
  
  if(whichMat == "mst"){
    retJson <- createJson(mstMat)
  } else {
    retJson <- createJson(corMat)
  }
  
  flog.info("%s Json created succesfully.",whichMat,name=logNS)
  prevJson <<- retJson
  
  if(abs(mstLength - prevMstLength) >= chngPercent*mstLength){
    prevMstLength <<- mstLength
    showFlag <- 1
    flog.info("Mst has changed enough to be shown. Difference is %d",abs(mstLength - prevMstLength), name=logNS)
  } else {
    prevMstLength <<- mstLength
    showFlag <- 0
  }
  fieldsJson <- paste(fieldsJson,",","\"showGraph\":",showFlag,sep="")
  retJson <- paste(retJson,",",fieldsJson,"}",sep="")
  return(retJson)
}

#-------------------------------------------
# Clean up function when the agent is killed
#-------------------------------------------

cleanUp <- function(){
  flog.info("The agent %s in agency %s has been killed. Clean up function evoked",
            agent__,agency__, name=logNS)
  
  if(exists("con")) dbDisconnect(con)
  if(exists("con1")) dbDisconnect(con1)
  
  rm(list=ls())
  flog.info("Clean up done.")         
}

flog.info("Sourcing the file - end.", name=logNS)
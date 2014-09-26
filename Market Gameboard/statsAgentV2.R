#******************************************************************************
# Name of script       : statsAgentV2.R
# Script Details       : Computes a set of statistics for a time series data and
#                        writes it to Hbase.
# Package version      : 0.1-2
# Script version       : 0.1-2
# Date                 : 06/11/2013
# Author(s)  	       : Karthik Matta
# Task id(s)		   : 211 - http://182.71.223.28/agilefant/qr.action?q=story:211
# Comment(s)           : Computes returns on the last 't' rows of data
#******************************************************************************

#==============================
# Get the environment variables
#==============================
mripPath = Sys.getenv("MRIP_HOME")

#=================================
# Source the agency utilities file
#=================================
source(paste(common__,"/gameBoardUtilities.R",sep=""))
source(paste(common__,"/HbaseUtils.R",sep=""))

#=================================
# Setting futile logger threshold
#=================================
logNS <- "statsAgent"
flog.threshold(INFO, name=logNS)
flog.appender(appender.file(paste(mripPath,"/MINTlogs/statsAgent.log",sep="")), name=logNS)

#====================================
# Check R version and package version
#====================================
CheckRversion(logNS=logNS)

checPkgVec[1] <- CheckPackVer("fBasics",'3010.86',logNS=logNS)
checPkgVec[2] <- CheckPackVer("ff",'2.2-11',logNS=logNS)
checPkgVec[3] <- CheckPackVer("pracma",'1.4.5',logNS=logNS)
checPkgVec[4] <- CheckPackVer("fractaldim",'0.8.1',logNS=logNS)
checPkgVec[5]<-CheckPackVer("ape",'3.0.11',logNS=logNS)
checPkgVec[6] <- CheckPackVer("Rjms", '0.0.4', logNS=logNS)

if(any(checPkgVec != 0)){
  errorCode <- 11
}

#===========================
# Loading required libraries
#===========================
require("fBasics")
require("ape")
require("ff")
require("pracma")
require("fractaldim")
require('Rjms')

#=====================================
# Initialize variables for this script
#=====================================
mrketOpenTime <- "NA"
GBStatsStream <- initialize.logger(activeMQIp,'T','GBStatsStream')
# Time duration on which the returns are to be computed
tn <- 10
if(tn > returnsDfFrameSize){
   flog.warn("The tn value is > than the data frame size; setting tn equal to data frame size", name=logNS)
   tn <- returnsDfFrameSize
}

#========================
# Function to get the MST 
#========================

getMst <- function(distMat,corMat){
  mstMat <- mst(distMat) 
  mstLength <- sum(mstMat*distMat)
  mstMat <- mstMat * corMat
  return(list("mst" = mstMat, "mstLength" = mstLength))
}

#--------------------------------------------------------------------------------------------------
# Function for computing the cor and the mst of the data frame for the mst length
#--------------------------------------------------------------------------------------------------
computeCorMst <- function(dataFrame, numTimeStamp){
  tryCatch(corMat <- cor(dataFrame), 
           error = function(err){
             flog.error("Error in calculating correlation matrix. The error message is %s",err)
             errorCode <<- 200
           },
           finally = {})
  
  # Make the NA correlations 0 (for the sake of the UI)
  corMat[which(is.na(corMat))] <- 0
  corMat <- round(corMat, digits=3)
  writeCorMat <- as.vector(corMat[lower.tri(corMat,diag=F)])
  
  distMat <- sqrt(2*(1-abs(corMat)))
  
  tryCatch(mstList <- getMst(distMat,corMat), 
           error = function(err){
             flog.error("Error in calculating mst. The error message is %s",err)
           },
           finally = {})

  mstLength <- mstList$mstLength
  mstMat <- as.matrix(mstList$mst)
  mstMat <- round(mstMat, digits=3)
  writeMstMat <- c(numTimeStamp,as.vector(mstMat[lower.tri(mstMat,diag=F)]))
  
  writeMstLength <- c(numTimeStamp,mstLength)
  return(list(writeMstLength=writeMstLength, writeMstMat=writeMstMat, writeCorMat=writeCorMat))
}

#========================================================================================================================
# Function to compute the returns for last 'tn' rows of data
#========================================================================================================================

computeReturns <- function(dataFrame, numTimeStamp){
   tryCatch({returnsVec <- dataFrame[(frameSize-tn),] - dataFrame[frameSize,]
              # Insert the returns vector to Hbase
	          tryCatch({hb.insert(returnsTable,list(list(numTimeStamp, columnNamesRet, returnsVec)))}, error= function(err){flog.error("Could not write to Returns table : %s", err, name=logNS); errorCode <<- 904}, finally={})
              },
             error = function(err){
               flog.error("Error in calculating Returns: %s",err, name=logNS)
             },
             finally = {})
    return(returnsVec)
}

#========================================================================================================================
# Function to compute the returns on 'tn' rows of data
#========================================================================================================================
splitAt <- function(x, pos) unname(split(x, cumsum(seq_along(x) %in% pos)))
computeReturnsOnTnChunk <- function(dataFrame){
   if(returnsDfFrameSize %% tn == 0){
      ind <-which(1:nrow(dataFrame)%%10==0)+1
	  splits <- splitAt(1:nrow(dataFrame), ind)
      retDataFrame <- t(sapply(splits, FUN=function(X) {return(dataFrame[X[length(X)],]-dataFrame[X[1],])}))
	  colnames(retDataFrame) <- colnames(dataFrame)
	  return(retDataFrame)
    } else {
	   flog.error("set the right tn param!", name=logNS)
	   return(-1)
	}
}

#========================================================================================================================
# Main function
#========================================================================================================================
MainComputeStats <- function(inputDfJson){
  
  
  tryCatch({inputList <-fromJSON(inputDfJson)
            fieldsJson <<- paste("\"TimeStamp\"",gsub(".*?\"TimeStamp\"(.*?)}","\\1",inputDfJson),sep="")
            errorCode <<- 0},
           error = function(err){
             errorCode <<- 702
             flog.error("Received an invalid Json from the above agent.\n %s",inputDfJson, name=logNS)},
           finally = {})
  
  # Return the previous json in the case of error
  if (!any(errorCode == c(0,100)) && prevJson == ""){
    flog.error("Error in sourcing the file. ErrorCode is %d",errorCode,name=logNS)
    return(paste("[\"ErrorCode:",errorCode,"\"]",sep=""))
  }
  
  # Extract the json fields
  tryCatch({errorCode <- inputList$ErrorCode
  dataFrame <- as.data.frame(inputList$DF)
  timeStamp <- inputList$TimeStamp
  marketIndicator <- inputList$marketStatus  
  numTimeStamp <- as.numeric(as.POSIXct(timeStamp))
  
  dataFrame <- as.matrix(dataFrame)
  flog.info("Received the dataFrame.", name=logNS)
  nSymbols <- ncol(dataFrame)
  frameSize <<- nrow(dataFrame)}, error = function(err){flog.error("Error occoured in extraction json fields: %s", err, name=logNS)},finally={})
  
  #-----------------------------------------------------------------------------------------------------------------------------------------
  # Compute the correlation and the MST
  tryCatch({corMstObj <- computeCorMst(dataFrame, numTimeStamp)}, error=function(err){flog.error("Error occoured in computing Cor/Mst: %s", err, name=logNS)}, finally={})
  writeCorMat <- corMstObj$writeCorMat
  writeMstLength <- corMstObj$writeMstLength
  writeMstMat <- corMstObj$writeMstMat
  #-----------------------------------------------------------------------------------------------------------------------------------------
  
  # Calculating stats 
  
  if (nrow(dataFrame) >= minDfSize  ){
    if(marketIndicator == "open"){
	
	  tryCatch({retDataFrame <- computeReturnsOnTnChunk(dataFrame)}, error=function(err){flog.error("Error occoured in computeReturnsOnTnChunk: %s", err, name=logNS)},finally={})
	  if(!is.matrix(retDataFrame)){
        if(retDataFrame == -1){
	      return("ignore")
		}
	  }
  
      tryCatch({hb.insert(correlationsTable,list(list(numTimeStamp, columnNamesCor, writeCorMat)))}, error= function(err){flog.error("Could not write to correlations table : %s", err, name=logNS); errorCode <<- 904}, finally={})
   
      tryCatch({hb.insert(mstTable,list(list(writeMstMat[1], columnNamesMst, writeMstMat[-1])))}, error= function(err){flog.error("Could not write to mst table : %s", err, name=logNS);errorCode <<- 904}, finally={})
    
	  tryCatch({hb.insert(mstLengthTable,list(list(writeMstLength[1], columnNamesMstL, writeMstLength[-1])))}, error= function(err){flog.error("Could not write to mst Length table : %s", err, name=logNS); errorCode <<- 904}, finally={})
	  
      returnsVec <- computeReturns(dataFrame, numTimeStamp)	  
	  
	  tryCatch({volatilityVec <- colSds(retDataFrame)
	          tryCatch({hb.insert(volatilityTable,list(list(numTimeStamp, columnNamesVol, volatilityVec)))}, error= function(err){flog.error("Could not write to Volatility table : %s", err, name=logNS); errorCode <<- 904}, finally={})
              }, 
             error = function(err){
               flog.error("Error in calculating volatility. The error message is %s",err, name=logNS)
             },
             finally = {})
    
      tryCatch({skewnessVec <- colSkewness(retDataFrame)
	          tryCatch({hb.insert(skewnessTable,list(list(numTimeStamp, columnNamesSke, skewnessVec)))}, error= function(err){flog.error("Could not write to Skewness table : %s", err, name=logNS); errorCode <<- 904}, finally={})
              }, 
             error = function(err){
               flog.error("Error in calculating Skewness. The error message is %s",err, name=logNS)
             },
             finally = {})
    
      tryCatch({kurtosisVec <- colKurtosis(retDataFrame)
	           tryCatch({hb.insert(kurtosisTable,list(list(numTimeStamp, columnNamesKur, kurtosisVec)))}, error= function(err){flog.error("Could not write to Kurtosis table : %s", err, name=logNS); errorCode <<- 904}, finally={})
              },
             error = function(err){
               flog.error("Error in calculating Kurtosis. The error message is %s",err, name=logNS)
             },
             finally = {})
    
      
    
    
      tryCatch({hurstVec <- apply(dataFrame, MARGIN=2, FUN= function(x){fd = fd.estimate(x); return(2-fd$fd)})
	           tryCatch({hb.insert(hurstTable,list(list(numTimeStamp, columnNamesHur, hurstVec)))}, error= function(err){flog.error("Could not write to Hurst table : %s", err, name=logNS); errorCode <<- 904}, finally={})
              },
             error = function(err){
               flog.error("Error in calculating Hurst Exponent. The error message is %s",err, name=logNS)
             },
             finally = {})
      save(list = c("volatilityVec","skewnessVec","kurtosisVec","returnsVec","hurstVec","mrketOpenTime"),file=paste(common__,"/latestStatsVectors.rdata",sep=""))
	
	  if(errorCode != 904){
	    flog.info("Successfully wrote all the stats to Hbase", name=logNS)
	  } else {
	    errorCode <<- 0
	  }
	
	  statsList <- list(vVec = volatilityVec, sVec = skewnessVec, kVec = kurtosisVec, rVec = returnsVec, hVec=hurstVec, mlVec = writeMstLength[-1])
	  statsJson <- toJSON(statsList)
	  # Push the stats to the queue
      tryCatch({to.logger(GBStatsStream, statsJson, FALSE, 'clientID', 'xxx-yy-zzz')}, error=function(err){flog.error("error occoured in pushing stats to the queue! : %s", err, name=logNS); errorCode<<-298}, finally={})
    
	  if(errorCode !=298){
	    errorCode <<- 0
	    flog.info("Stats Done; pushed to queue", name=logNS)
	  }
      return("ignore")
    } else {
      firstCall <<- TRUE
      flog.info("Markets are closed", name=logNS)
      return("ignore")
    }
  } else {
    firstCall <<- TRUE
    flog.info("Dataframe Charging", name=logNS)
    return("ignore")
  }
  
  if(firstCall && marketIndicator == "open"){
    mrketOpenTime <- numTimeStamp
    firstCall <<- FALSE
  }
    
}

#-------------------------------------------
# Clean up function when the agent is killed
#-------------------------------------------

cleanUp <- function(){
  flog.info("The agent %s in agency %s has been killed. Clean up function evoked",
            agent__,agency__, name=logNS)
  
  if(exists("con")) dbDisconnect(con)
  if(exists("con1")) dbDisconnect(con1)
   destroy.logger(GBStatsStream)
  
  rm(list=ls())
  flog.info("Clean up done.")         
}
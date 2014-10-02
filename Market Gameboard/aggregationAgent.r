#******************************************************************************
# Name of script       : aggregationAgentV2.R
# Script Details       : Aggregates a set of statistics 
# Script version       : 0.2
# Date                 : 25/11/2013
# Author(s)            : Karthik D Matta
# Task id(s)		   : 211 - http://182.71.223.28/agilefant/qr.action?q=story:211
# Comment(s)           : This agent is made for Gameboard 2.0
#                      : writes it to Hbase.
#******************************************************************************

#==============================
# Get the environment variables
#==============================
mripPath = Sys.getenv("MRIP_HOME")

#=================================
# Source the agency utilities file
#=================================
#source(paste(mripPath,"/wd/gameBoardUtilities.R",sep=""))
source(paste(common__,"/gameBoardUtilities.R",sep=""))
source(paste(common__,"/HbaseUtils.R",sep=""))

#=================================
# Setting futile logger threshold
#=================================
logNS <- "aggregationAgent"
flog.threshold(INFO, name=logNS)
flog.appender(appender.file(paste(mripPath,"/MINTlogs/aggregationAgent.log",sep="")), name=logNS)

#====================================
# Check R version and package version
#====================================
CheckRversion(logNS=logNS)

checPkgVec[1] <- CheckPackVer("ff",'2.2-11',logNS=logNS)
checPkgVec[2] <- CheckPackVer("ffbase",'0.9',logNS=logNS)
checPkgVec[3] <- CheckPackVer("Rjms",'0.0.4',logNS=logNS)
checPkgVec[1] <- CheckPackVer("fBasics",'3010.86',logNS=logNS)


if(any(checPkgVec != 0)){
  errorCode <- 11
}

#===========================
# Loading required libraries
#===========================
require("ff")
require("ffbase")
library('Rjms')
require("fBasics")

#======================
# Initialize parameters
#======================
statsListNames <- c("volatilityVec","skewnessVec","kurtosisVec","returnsVec","hurstVec","mstLengthVec")
tryCatch({consumeStats <- initialize.consumer(activeMQIp,'T','GBStatsStream')}, error = function(err){flog.fatal("Can't connect to GBStatsStream on %s", activeMQIp, name = logNS)}, finally={})
clusterMembership <- NULL
aggList <- NULL
refreshFlag <- FALSE

#=================================================================================
# Function to read tables into memory from Hbase
#=================================================================================

readTablesFromHbase <- function(){
   tryCatch({rTable <<- scanTable(returnsTable, colFam); rTable<<- rTable[,-1]},
           error = function(err){
             flog.error("Error in reading in the %s from HBASE: %s",returnsTable,err, name=logNS)
			 errorCode <<- 900
           },
           finally={}
  )
  
  tryCatch({vTable <<- scanTable(volatilityTable, colFam);vTable<<- vTable[,-1]},
           error = function(err){
             flog.error("Error in reading in the %s from HBASE: %s",volatilityTable,err, name=logNS); 
			 errorCode <<- 900
           },
           finally={}
  )
  
  tryCatch({hTable <<- scanTable(hurstTable, colFam);hTable<<- hTable[,-1]},
           error = function(err){
             flog.error("Error in reading in the %s from HBASE: %s",hurstTable,err, name=logNS); 
			 errorCode <<- 900
           },
           finally={}
  )
  
  tryCatch({sTable <<- scanTable(skewnessTable, colFam);sTable<<- sTable[,-1]},
           error = function(err){
             flog.error("Error in reading in the %s from HBASE: %s",skewnessTable,err, name=logNS); 
			 errorCode <<- 900
           },
           finally={}
  )
  
  tryCatch({kTable <<- scanTable(kurtosisTable, colFam);kTable<<- kTable[,-1]},
           error = function(err){
             flog.error("Error in reading in the %s from HBASE: %s",kurtosisTable,err, name=logNS); 
			 errorCode <<- 900
           },
           finally={}
  )
  
  tryCatch({mlTable <<- scanTable(mstLengthTable, colFam);mlTable<<- mlTable[,-1]},
           error = function(err){
             flog.error("Error in reading in the %s from HBASE: %s",mstLengthTable,err, name=logNS); 
			 errorCode <<- 900
           },
           finally={}
  )
  
  if(errorCode != 900){
    statsTableList <<- list(vTable, sTable, kTable,  rTable, hTable, mlTable)
    flog.info('Successfully queried all the stats tables from Hbase', name=logNS)
  }

}

#=================================================================================
# Function computes stats for a particular cluster
#=================================================================================

computeStatsPerCluster <- function(currMem, rVec, clusterMembership){
    statTable <- rTable
    if(nrow(statTable) >= length(clusterMembership)){
      statTable <- statTable[1:length(clusterMembership),]
	} else {
      clusterMembership <- clusterMembership[1:nrow(statTable)]
	}
	fmChunk <- frameChunks[[currMem]] <<- rbind(frameChunks[[currMem]], rVec)
	
	
	
	tryCatch({volatilityPerFrame <- colSds(fmChunk)}, error=function(err){flog.error("Error Occoured in computing volatility per cluster:%s", err, name=logNS)}, finally={})
	
	tryCatch({kurtosisPerFrame <- colKurtosis(fmChunk)}, error=function(err){flog.error("Error Occoured in computing kurtosis per cluster:%s", err, name=logNS)}, finally={})
	
	tryCatch({skewnessPerFrame <- colSkewness(fmChunk)}, error=function(err){flog.error("Error Occoured in computing skewness per cluster:%s", err, name=logNS)}, finally={})
	
	tryCatch({returnsPerFrame <- colMeans(fmChunk)}, error=function(err){flog.info("frameChunk : %d", length(frameChunk), name=logNS);flog.info("frameChunk Dim: %d",dim(frameChunk), name=logNS);flog.error("Error Occoured in computing returns per cluster:%s", err, name=logNS)}, finally={})
	
	returnsPerCluster[[currMem]] <- returnsPerFrame
	skewnessPerCluster[[currMem]] <- skewnessPerFrame
	kurtosisPerCluster[[currMem]] <- kurtosisPerFrame
	volatilityPerCluster[[currMem]] <- volatilityPerFrame
	
	statsPerClusterList <- list(volatilityPerCluster=volatilityPerCluster, skewnessPerCluster=skewnessPerCluster, kurtosisPerCluster=kurtosisPerCluster, returnsPerCluster=returnsPerCluster)
	save(statsPerClusterList,file=paste(common__,"/statsPerClusterList.rdata",sep=""))
    flog.info("Updated all the stats per cluster", name=logNS)
	
}

#=================================================================================
# Function computes stats for a particular cluster at cluster break
#=================================================================================

computeStatsPerClusterAtClusterBreak <- function(clusterMembership){
    statTable <- rTable
    if(nrow(statTable) >= length(clusterMembership)){
      statTable <- statTable[1:length(clusterMembership),]
	} else {
      clusterMembership <- clusterMembership[1:nrow(statTable)]
	}
	
	tryCatch({uniques <- unique(clusterMembership)
	indecies <- lapply(uniques, FUN=function(X){which(clusterMembership==X)})
	frameChunks <<- lapply(indecies, FUN=function(X){statTable[X,]})}, error=function(err){flog.error("Error Occoured chunking the returns frame : %s", err, name=logNS)}, finally={})
	
	tryCatch({volatilityPerCluster <- lapply(frameChunks, FUN=colSds)}, error=function(err){flog.error("Error Occoured in computing volatility per cluster:%s", err, name=logNS)}, finally={})
	
	tryCatch({kurtosisPerCluster <- lapply(frameChunks, FUN=colKurtosis)}, error=function(err){flog.error("Error Occoured in computing kurtosis per cluster:%s", err, name=logNS)}, finally={})
	
	tryCatch({skewnessPerCluster <- lapply(frameChunks, FUN=colSkewness)}, error=function(err){flog.error("Error Occoured in computing skewness per cluster:%s", err, name=logNS)}, finally={})
	
	tryCatch({returnsPerCluster <- lapply(frameChunks, FUN=colMeans)}, error=function(err){save(fmChunk,file=paste(common__,"/fmChunkTest.rdata",sep=""));flog.error("Error Occoured in computing returns per cluster:%s", err, name=logNS)}, finally={})
	
	statsPerClusterList <- list(volatilityPerCluster=volatilityPerCluster, skewnessPerCluster=skewnessPerCluster, kurtosisPerCluster=kurtosisPerCluster, returnsPerCluster=returnsPerCluster)
	save(statsPerClusterList,file=paste(common__,"/statsPerClusterList.rdata",sep=""))
    flog.info("Updated all the stats per cluster", name=logNS)
	
}

#=================================================================================
# Function that loads the required table and aggregates it according to membership
#=================================================================================

AggOneStat <- function(statTable,clusterMembership){
if(!is.null(statTable)){
 tryCatch({
  if(!is.null(dim(statTable))){  
    if(nrow(statTable) >= length(clusterMembership)){
      statTable <- statTable[1:length(clusterMembership),]
	} else {
      clusterMembership <- clusterMembership[1:nrow(statTable)]
	}
    if(dim(statTable)[1] == length(clusterMembership)){
      #statTable <- statTable[,-1] #Drop time column
      aggregatedData <- aggregate(statTable, list(clusterMembership), FUN = function(X) {return(mean(X, na.rm=TRUE))})
      return(aggregatedData)
    } else {
      flog.error("The dimensions of statsTable is %d and the clusterMembership is %d. They do not match.", dim(statTable)[1],length(clusterMembership), name=logNS)
      return(NULL)
    }
  } else {
    if(length(statTable) >= length(clusterMembership)){
      statTable <- statTable[1:length(clusterMembership)]
	} else {
      clusterMembership <- clusterMembership[1:length(statTable)]
	}
	aggregatedData <- aggregate(statTable, list(clusterMembership), FUN = function(X) {return(mean(X, na.rm=TRUE))})
    return(aggregatedData)
  }
  }, error =function(err){flog.error("Error occoured in aggregation one stat: %s", err, name=logNS)},finally={})
  }
}

#========================================================================================================================================================
AggOneCluster <- function(statTable,clusterMembership,currMemShip){
  if(!is.null(statTable)){
  tryCatch({
  if(!is.null(dim(statTable))){  
    if(nrow(statTable) >= length(clusterMembership)){
      statTable <- statTable[1:length(clusterMembership),]
	} else {
      clusterMembership <- clusterMembership[1:nrow(statTable)]
	}
    if(dim(statTable)[1] == length(clusterMembership)){
      #statTable <- as.data.frame(statTable[,-1]) #Drop time column
      statTable <- as.data.frame(statTable[which(clusterMembership == currMemShip),])
      aggregatedData <- colMeans(statTable, na.rm = TRUE)
      return(aggregatedData)
    } else {
      flog.error("The dimensions of statsTable is %d and the clusterMembership is %d. They do not match.", dim(statTable)[1],length(clusterMembership), name=logNS)
      return(NULL)
    }
  } else {
    if(length(statTable) >= length(clusterMembership)){
      statTable <- statTable[1:length(clusterMembership)]
	} else {
      clusterMembership <- clusterMembership[1:length(statTable)]
	}
	tryCatch({statTable <- as.data.frame(statTable[which(clusterMembership == currMemShip)])}, error=function(err){flog.error("The cluster Mem is greater than that of the stst tables:%s", err, name=logNS)}, finally={})
    aggregatedData <- colMeans(statTable, na.rm = TRUE)
    return(aggregatedData)
  }
  }, error = function(err){flog.error("Error occoured in aggregation a cluster : %s", err, name=logNS)}, finally={})
  } 
}
#========================================================================================================================================================
# Function to consume the latest stats from the queue and update the tables
#========================================================================================================================================================

consumeStatsFromQueue <- function(){
  tryCatch({statsJson <- consume(consumeStats, asString=FALSE, timeOut=12000) }, error=function(err){ flog.fatal("The GBStatsStream Queue is down!", name=logNS); errorCode <<- 400}, finally={})
  
  if(errorCode!=400 && !is.null(statsJson)){
    flog.info('Consumed a msg from the stats queue', name=logNS)
    tryCatch({statsList <- fromJSON(statsJson)
    rVec <<- as.numeric(statsList$rVec)
    sVec <- as.numeric(statsList$sVec)
    kVec <- as.numeric(statsList$kVec)
    mlVec <- as.numeric(statsList$mlVec)
    hVec <- as.numeric(statsList$hVec)
    vVec <- as.numeric(statsList$vVec)}, error=function(err){flog.error("error occoured in extracting the stats json: %s", err, name=logNS)}, finally={})
	
	tryCatch({rTable <<- rbind(rTable, rVec)
	sTable <<- rbind(sTable, sVec)
	kTable <<- rbind(kTable, kVec)
	tryCatch(mlTable <<- c(mlTable, mlVec), error=function(err){flog.error("Error in appending MST Len : %s", err, name=logNS)},finally={})
	hTable <<- rbind(hTable, hVec)
	vTable <<- rbind(vTable, vVec)}, error=function(err){flog.error("error occoured in appending the stats Vecs to the tables : %s", err, name=logNS); errorCode <<- 705}, finally={})
	if(errorCode!=705){
	  errorCode <<-0
	  flog.info("Appended all the stats to their respective tables", name=logNS)
	}
	statsTableList <<- list(vTable, sTable, kTable,  rTable, hTable, mlTable)
	
	return(0)
  } else if(is.null(statsJson)){
    flog.info("Consumed all the messages from the queue", name=logNS)
	return(400)
  } else {
    errorCode <<- 0	   
	return(400)
  }
}

#=====================================================================================================================
# Main function
#=====================================================================================================================

MainAggregateStats <- function(tstMem=NA){
  if(is.na(tstMem)){
    flog.info("Input is NA", name=logNS)  
  }else {
    flog.info("Received a message from VQ agent", name=logNS)  
  }
  
  if(tstMem == "error"){
    flog.error("Received an error from VQ agent. Message received is %s. Aggregation will not happen.",tstMem, name=logNS)
    return("ignore")
  } else if(tstMem == "charging"){
    flog.info("Dataframe charging.", name=logNS)
    return("ignore")
  }
  
  # Extract the json fields from teh VQ agent
  tryCatch({tstMem <- fromJSON(tstMem)
  currMemShip <- tstMem$membership
  clusterBreakFlag <- tstMem$clusterBreakFlag}, error = function(err){flog.error("Error in converting vq json : %s", err, name=logNS)}, finally={})
  
  # At first time call
  if(firstCall){
    flog.info("First Call made", name=logNS)
    firstCall <<- FALSE
	tryCatch({clusterMembership <<- read.csv.ffdf(file=paste(common__,"/stateSequenceData",sep=""))
	         clusterMembership <- clusterMembership$ff.seque.[1:length(clusterMembership$ff.seque.)]},
             error = function(err){
               flog.error("Error in reading in the clusterMemberships error is %s",err, name=logNS)
             },
             finally={}
    )
	aggList <<- lapply(statsTableList, FUN=AggOneStat, clusterMembership)
	
	computeStatsPerClusterAtClusterBreak(clusterMembership)
    
    if(any(unlist(lapply(aggList,is.null)))){
      flog.error("The dimensions are not same.", name=logNS)
      return("ignore")
    }
	
	names(aggList) <<- gsub("Vec","",statsListNames)    
    save(aggList,file=paste(common__,"/latestAggData.rdata",sep=""))
    flog.info("Updated the latest aggregated Json after first call", name=logNS)
	return('ignore')
	
  }
 
  if(clusterBreakFlag == TRUE){
    
    tryCatch({clusterMembership <<- read.csv.ffdf(file=paste(common__,"/stateSequenceData",sep=""))
	        clusterMembership <- clusterMembership$ff.seque.[1:length(clusterMembership$ff.seque.)]},
             error = function(err){
               flog.error("Error in reading in the clusterMemberships error is %s",err, name=logNS)
             },
             finally={}
    )
    
	# Update the stats tables by listening to the queue
	if(consumeStatsFromQueue()==400){
	   return('ignore')
	}
	
    aggList <<- lapply(statsTableList, FUN=AggOneStat, clusterMembership)
	
	computeStatsPerClusterAtClusterBreak(clusterMembership)
    
    if(any(unlist(lapply(aggList,is.null)))){
      flog.error("The dimensions are not same.", name=logNS)
      return("ignore")
    }
    
    names(aggList) <<- gsub("Vec","",statsListNames)    
    save(aggList,file=paste(common__,"/latestAggData.rdata",sep=""))
    flog.info("Successfully updated the latest aggregated Json after cluster break", name=logNS)
    
  } else if(clusterBreakFlag == FALSE) {
    
     
    #clusterMembership <<- ffappend(clusterMembership,currMemShip)
    tryCatch({clusterMembership <<- read.csv.ffdf(file=paste(common__,"/stateSequenceData",sep=""))
	         clusterMembership <- clusterMembership$ff.seque.[1:length(clusterMembership$ff.seque.)]},
             error = function(err){
               flog.error("Error in reading in the clusterMemberships error is %s",err, name=logNS)
             },
             finally={}
    )
    
	# Update the stats tables by listening to the queue
	if(consumeStatsFromQueue()==400){
	   return('ignore')
	}	
    aggListOne <- lapply(statsTableList,FUN=AggOneCluster,clusterMembership,currMemShip)
	
	computeStatsPerCluster(currMemShip, rVec, clusterMembership)
    
    tmp <<- lapply(1:length(aggList), FUN=function(x){
      aggList[[x]][currMemShip,] <<- c(currMemShip,aggListOne[[x]])
      return(aggList[[x]])
    }
                   ) 
      
    if(any(unlist(lapply(aggList,is.null)))){
      flog.error("The dimensions are not same.", name=logNS)
      return("ignore")
    }
    names(aggList) <<- gsub("Vec","",statsListNames)
    save(aggList,file=paste(common__,"/latestAggData.rdata",sep=""))
    flog.info("Successfully updated the latest aggregated Json", name=logNS)
	
	
  } else if (clusterBreakFlag == "closed"){
    flog.info("Markets are closed", name=logNS)   
  }
  
  return("ignore")
}

#-------------------------------------------
# Clean up function when the agent is killed
#-------------------------------------------

cleanUp <- function(){
  flog.info("The agent %s in agency %s has been killed. Clean up function evoked",
            agent__,agency__, name=logNS)
  
  if(exists("con")) dbDisconnect(con)
  if(exists("con1")) dbDisconnect(con1)
  destroy.consumer(consumeStats)
  
  rm(list=ls())
  flog.info("Clean up done.")         
}

#----------------------------------------------------
# Read all the stats tables from Hbase whilst source
#----------------------------------------------------
readTablesFromHbase()
#----------------------------------------------------

#tmp <- "{\"x\":[0,0,4.33428125937494,6.46121749780682,1.62577661044311,8.2087778366361,8.87340880003453,11.6180511344093,5.76730672264296,3.12393230340115,1.93603121602166,6.59205109707931,4.27116938069401,0.63278638054572,3.40622486711278,6.11567697430649,4.49853398351435,3.39322145794316,5.48951209442303,-2.31252003314958,3.84170184732006,4.73133569275787,5.77214791725998,6.99704824981256,2.84201548460568,6.47185928665699,5.9796165097167,4.4029528263557,2.79827305886746,1.78935931976835,-3.69426751705692],\"y\":[0,3.98063359610833,2.53685963751361,1.64599255313035,4.77362476964973,4.94568535776862,2.09720876401905,2.33936489421178,2.04489099106648,4.00095543721378,5.85184008281549,2.33485942235574,3.55116913111673,6.32960780872309,1.55951419545949,6.67597701916048,3.22868370634202,2.87895817961532,4.17300718994881,10.2434217526765,4.8204866903885,4.59521313525162,3.59920618700927,5.19508223725622,6.74352872803139,2.6718365127876,4.82275484242518,6.76420791291474,5.12567623528991,8.60012788910094,10.1556162899692],\"clusterBreakFlag\":false,\"membership\":2}"

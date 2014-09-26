#******************************************************************************
# Name of script       : StatePredictionAgentV6_GraphMov.R
# Script Details       : Computes the score for transition of the current state to the next state
# Script version       : 0.5
# Date                 : 27/08/2013
# Author(s)  	       : Karthik D
# Comment(s)           : If your man gets personal,
#                      : Won't you have your fun?
#                      : Well, come on back to Friar's Point, mama,
#                      : Let's Barrelhouse all night long.
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
logNS <- "StatesPredAgent"
flog.threshold(INFO, name=logNS)
flog.appender(appender.file(paste(mripPath,"/MINTlogs/StatesPredictionAgentGraphMov.log",sep="")), name=logNS)

#====================================
# Check R version and package version
#====================================
CheckRversion(logNS=logNS)

checPkgVec[1]<-CheckPackVer("reshape2",'1.2.2',logNS=logNS)
checPkgVec[2] <- CheckPackVer("rjson",'0.2.12',logNS=logNS)
checPkgVec[3] <- CheckPackVer("ffbase",'0.9',logNS=logNS)
checPkgVec[4] <- CheckPackVer("ff",'2.2.11',logNS=logNS)

#===========================
# Loading required libraries
#===========================
library(reshape2)
library(rjson)
library(ffbase)
library(ff)

prevState <- NULL
predictedState <- NULL
prev <- NULL
ignoreNextCallFlag <- FALSE
predictedFlag <- FALSE
jumpTresh <- 0.07
stepsToIgnore <- 10
count <- 0
params <- c(jumpTresh, stepsToIgnore)

#==================================================================================================================
# Json maker 
#==================================================================================================================

makeSPJson <- function(vqObj, probabilities, startTimeStamp, endTimeStamp, jumpWindowInitial=NA, predictedForTime=NA){
   nClusters <- length(vqObj$x)
   probabilities[unlist(vqObj$membership)] <- -1
   centroidList <- lapply(1:nClusters,FUN=function(x) c(x, vqObj$x[x], vqObj$y[x], probabilities[x]))
   centroidList <- lapply(centroidList, FUN= unname)
   #centroidList <- append(centroidList, list(startTimeStamp=startTimeStamp, endTimeStamp=endTimeStamp))
   cL <- list(tess = centroidList, timestamps = c(as.character(startTimeStamp), as.character(endTimeStamp), as.character(jumpWindowInitial), as.character(predictedForTime)))
   json <- toJSON(cL)
   return(json)
}

#==================================================================================================================
# State Prediction function 
#==================================================================================================================

predictStates_main <- function(inputJson){
  if(inputJson != "charging"){
    if(inputJson != "error" ){
	
      flog.info("call made..", name=logNS)
	 # Extract data from the VQ object
      tryCatch({vqObj <- fromJSON(inputJson);
	  #Extract the current membership from the json
	  currMem <- unlist(vqObj$membership);
	  dataForStatePrediction <- vqObj$dataForStatePrediction; 
	  clusterBreakFlag <- vqObj$clusterBreakFlag;
	  startTimeStamp <- as.numeric(vqObj$startTimeStamp);
	  startTimeStamp <- as.POSIXlt(as.numeric(startTimeStamp), tz="EST", origin="1970-01-01");
	  endTimeStamp <- vqObj$endTimeStamp;}, error=function(err){flog.error("error occoured in json conversion:%s",err, name=logNS)}, finally={})
      
	  if(clusterBreakFlag == "closed"){
	    clusterBreakFlag <- FALSE
	  }
	  
      if(dataForStatePrediction!="NA"){
	    if(!is.null(prev)){
		
		  tryCatch({distances <- dataForStatePrediction
		  # Compute the differences of the distance between clusters
		  howFarAway <- abs(distances-distances[currMem])
		  score <- howFarAway/max(howFarAway)}, error= function(err){flog.error("Error occoured in lines 97-99: %s", err, name=logNS)}, finally={})
		  
		  # Set a score
		  tryCatch({score <- 1-score
		  # Set the score for the current cluster based on the previous values of the distances
		  score[currMem] <- 1/abs(prev[currMem]-distances[currMem]);}, error= function(err){flog.error("Error occoured in computing score: %s", err, name=logNS)}, finally={})
		  #===================================================================================================
	  
	  #tryCatch({
	  
	  #if(!is.null(prevState) && !is.null(predictedState) && predictedFlag){	    	
	   # if(prevState  == currMem){
		#   flog.info("Jump never happened", name=logNS)
		#} else if(predictedState == currMem){
		   # Accurately predicted
		 #  flog.info("Accurately predicted", name = logNS)
		  # write.csv("1", file = paste(mripPath, "/wd/AccuracyMarkov.csv", sep=""), append=TRUE)
		#} else {
		  # inaccurately predicted
		 # flog.info("Jump Happened but to wrong state", name = logNS)
		  #write.csv("0", file = paste(mripPath, "/wd/AccuracyMarkov.csv", sep=""), append=TRUE)
		#}
		#predictedFlag <<- FALSE
	  #}
	  #flog.debug("%d", max(score)-sort(score, decreasing=TRUE)[2], name=logNS)
	  #if((max(score)-sort(score, decreasing=TRUE)[2]) <= jumpTresh){
	   # flog.info("Predicted a jump", name=logNS)
       # predictedState <<- 	sort(score, decreasing=TRUE)[2]
	#	predictedFlag <<- TRUE
     #  }	
	  
	  #}, error=function(err){flog.error("Error occoured in accuracy: %s" , err, name = logNS)}, finally={})
	  
	  #===================================================================================================
		  	  
		  prev <<- distances
		  prevState <<- currMem
		  
		  tryCatch({json <- makeSPJson(vqObj, score, startTimeStamp, endTimeStamp)}, error= function(err){flog.error("Error occoured in making json: %s", err, name=logNS)}, finally={})
		  
		  flog.info("Json successfully sent", name = logNS)
		  return(json)
		  
		} else {
		  prev <<- dataForStatePrediction
		  return("ignore")
		}		 
		
	  } else {
	    flog.info("waiting for the second message from VQ agent", name=logNS)
		return("ignore")
	  }
	} else {
       flog.error("Received an error Json from VQ agent. Please check VQ logs", name = logNS)
	   return("{\"ErrorCode\":901}")
	}
  } else {
	  flog.info("Date frame charging", name = logNS)
	  return("{\"ErrorCode\":100}")
  }
}

#================================================================%END%========================================================================================

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Project              : MINT - Market Gameboard
# Script Details       : Performs Vector Quantization by the codebook splitting method
# Package version      : 0.6
# Script version       : 0.6
# Date                 : 23/07/2013
# Author(s)            : Karthik Matta
# Comment(s)           : Sends out a matrix of distances btw the current graph with all the other centroids
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
logNS <- "vq"
flog.threshold(INFO, name=logNS)
flog.appender(appender.file(paste(mripPath,"/MINTlogs/vq.log",sep="")), name=logNS)

#====================================
# Check R version and package version
#====================================
CheckRversion(logNS=logNS)

checPkgVec[1] <- CheckPackVer("plyr",'1.8',logNS=logNS)
checPkgVec[2] <- CheckPackVer("rjson",'0.2.12',logNS=logNS)
checPkgVec[3] <- CheckPackVer("ffbase",'0.9',logNS=logNS)
checPkgVec[4] <- CheckPackVer("ff",'2.2.11',logNS=logNS)



if(any(checPkgVec != 0)){
  errorCode <- 11
}

#===========================
# Loading required libraries
#===========================

library(plyr)
library(rjson)
library(ffbase)
library(ff)


#======================================================================================
# Global variables
#======================================================================================

model <- list()
percentageE <- 50
firstCall <- TRUE
nSymbols <- length(symbolVec)
corMat <- matrix(1,nSymbols,nSymbols)
clusterBreakFlag <- FALSE
closedFlag <- TRUE
refFlag <- FALSE

#=============================================
# Helper function for function convJsonToMat
# Will be used as a part of an apply statement
#=============================================

convStrngToRow <- function(strng){
  
  src <- as.numeric(gsub(".*?source.*?: (.*?) ,.*","\\1",strng)) + 1
  to <- as.numeric(gsub(".*?target.*?: (.*?) ,.*","\\1",strng)) + 1
  value <- as.numeric(gsub(".*?value.*?: (.*?) .*","\\1",strng))
  corMat[src,to] <<- value
  corMat[to,src] <<- value
  
}

#==================================
# Converting the Json into a matrix
#==================================

convJsonToMat <- function(inputJson){
  
  drpExtraInfo = strsplit(inputJson,"nodes",fixed=TRUE)[[1]][1]
  corJson = strsplit(drpExtraInfo,split="[{}]")[[1]]
  corJson = corJson[seq(from=3,to=length(corJson),by=2)]
  tmp = sapply(corJson, FUN=convStrngToRow)
  rm(tmp)
}
#______________________________________________________________________________________
# Splitting the codebooks
#______________________________________________________________________________________

split_codevec = function(c){
  e=e/100
  c <- llply(c, .fun= function(X) return(list((1+e)*X, (1-e)*X)))
  return(c)  
}

#_____________________________________________________________________________________
# Checks if the MAPE for a given cluster and its codebook is lesser/greater
# than the specified treshold
#_____________________________________________________________________________________

mapeTest = function(cluster, c){
  if(is.null(dim(cluster))){
    return(TRUE)
  }
  mape <- 1/nrow(cluster) * sum(apply(cluster, MARGIN = 1, FUN= function(X){ return(abs(sum((X-c), na.rm=TRUE)/length(X)))}))
  print(paste("Quantization error:", mape))
  if(mape < e){
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#_____________________________________________________________________________________
# allocate members to their centroids
#_____________________________________________________________________________________
findMembers <- function(dat, centroid){
  
  # compute the distance of every point in the data from a centroid
  #err <- lapply(centroid, FUN = function(X){ return(apply(dat, MARGIN=1, FUN= function(Y)sqrt(sum((Y-X)^2))))})
  err = lapply(centroid, FUN=function(x) sqrt(colSums((t(dat) - x)^2)))
  err <- do.call(rbind, err)
  
  # Allocate the membership of a datum to its closest centroid
  clust.list <- apply(err, MARGIN=2, FUN = function(X) { return(which(X==min(X), arr.ind=T))})
  
  return(clust.list)
}


findSeque <- function(dat, clusters){
  tryCatch({
  clusters <- lapply(clusters, unname)
  clusters <- lapply(clusters, as.matrix)
  oneRowIndices <- which(lapply(lapply(clusters, dim), function(X)return(X[2]))==1)
  clusters[oneRowIndices] <- lapply(clusters[oneRowIndices], t)
  pointPos <- lapply(clusters, FUN=function(X)rownames(match_df(as.data.frame(unname(dat)),as.data.frame(X))))
  pointPos <- lapply(pointPos, as.numeric)
  sequeDf <<- as.data.frame(1:nrow(dat))
  lapply(1:length(pointPos), FUN=function(X){sequeDf[pointPos[[X]],]<<- X})
  clust.list <- sequeDf[,1]}, error=function(err){flog.error("error occoured in findSeque : %s ", err, name=logNS)}, finally={})
  
  return(clust.list)
}

#_____________________________________________________________________________________
# allocate members to their centroids
#_____________________________________________________________________________________

allocateMembers <- function(dat, centroid){ 
  
  clust.list <- findMembers(dat, centroid)
  
  return(lapply(unique(clust.list), FUN=function(X)which(clust.list==X)))
}

#_____________________________________________________________________________________
# Nearest neighbour condition - LBGVQ function
#_____________________________________________________________________________________

lbgVQ = function(dat){
  
  # mean of the entire data set
  cnew <- list(apply(dat, MARGIN = 2, FUN= function(X)mean(X, na.rm=TRUE)))
  cFin <- c()
  finalClusters <- c()
  
  # Set the quantization error
  mape <- 1/nrow(dat) * sum(apply(dat, MARGIN = 1, FUN= function(X){ return(abs(sum((X-unlist(cnew)), na.rm=TRUE))/length(X))}))
  if(mape != 0){
    e <<- mape*percentageE/100
  } else {
    e <<- percentageE/100
  }
  
  # clust.list is the indices of the members of a cluster
  clust.list <- list(1:nrow(dat))
  #tryCatch({clust.list <- allocateMembers(dat, cnew)}, error=function(err){flog.error("error occoured in allocateMembers place 1:%s",err, name=logNS)}, finally={})
  while(T){
    
    # cluster is the actual data points in the cluster
    cluster <- lapply(clust.list, FUN=function(Y) { dat[Y,] })
    
    
    # codesToSplit is the codebooks which have q error greater than the treshold 
    tryCatch({codesToSplit <- cnew[which(!unlist(lapply(1:length(cluster), FUN= function(i){mapeTest(cluster[[i]], cnew[[i]])})))]}, error=function(err){flog.error("error occoured in mapeTest:%s",err, name=logNS)}, finally={})
    
    # split the codebooks
    tryCatch({cSplit <- unlist(split_codevec(codesToSplit), recursive=F)}, error=function(err){flog.error("error occoured in split_codevec:%s",err, name=logNS)}, finally={})
    
    # remove the final codebook from the list whose q error is lesser than the treshold
    doneFlags <- !(cnew %in% codesToSplit)
	cFin <- c(cFin, cnew[doneFlags])
    if(length(which(doneFlags))!=0){
      
      # Update the finalClusters
      finalClusters <- c(finalClusters, cluster[which(!cnew %in% codesToSplit)])
      cluster[which(!cnew %in% codesToSplit)] <- NULL
      
      
      # Remove the clustered data from the original dataset
      dat <- dat[-unlist(clust.list[which(doneFlags)]),]
      
      # Start afresh
      cnew <- list(apply(dat, MARGIN = 2, FUN= mean))
      #tryCatch({clust.list <- allocateMembers(dat, cnew)}, error=function(err){flog.error("error occoured in allocateMembers place 2:%s",err, name=logNS)}, finally={})
      clust.list <- list(1:nrow(dat))
	  if(length(codesToSplit) == 0){
        break
      }
      next
    }
    
    # update the clust.list with the new split codebooks and 
    # re compute the mean codebook
    tryCatch({clust.list <- allocateMembers(dat, cSplit)}, error=function(err){flog.error("error occoured in allocateMembers place 3:%s",err, name=logNS)}, finally={})
    cnew <- lapply(clust.list, FUN= function(X) if(length(X)==1){
      return(dat[X,])
    } else{ return(apply(dat[unlist(X),], MARGIN=2, FUN=mean))})  
  }
  cFin <- lapply(cFin, FUN = as.numeric)
  return(list(clusters = finalClusters, codebooks = cFin))
}

#_____________________________________________________________________________________
# Functions to merge the clusters that satisfy the quantization treshold
#_____________________________________________________________________________________

euDistance <- function(index, codebooks){
  return((codebooks[[index[1]]] - codebooks[[index[2]]])^2)
}


# Function checks if the clusters should be merged based on the quantization error
shouldMergeClusters <- function(X, clusters){ 
    
    # merge the two clusters and compute the new mean
    newCluster <- do.call(rbind, list(clusters[[X[1]]], clusters[[X[2]]]))
    meanCb <- apply(newCluster, MARGIN = 2, FUN= mean)
	
	# check for the quantization error
    tryCatch({mapeFlag <- mapeTest(newCluster, meanCb)}, error=function(err){flog.error("error occoured in mapeTest :%s",err, name=logNS)}, finally={})
    return(mapeFlag)
 }

mergeClusters <- function(codebooks, clusters){
  
  # find the index of the centroids that are closest (X)
  pairs <- t(combn(length(codebooks),2))
  tryCatch({euD <- sqrt(colSums(apply(pairs, MARGIN=1, FUN=function(X){euDistance(X, codebooks)})))}, error=function(err){flog.error("error occoured in euDistance :%s",err, name=logNS)}, finally={})
  X<- pairs[which(euD==min(euD)),]
  
   
  if(shouldMergeClusters(X, clusters)){
    # merge the two clusters and compute the new mean
    newCluster <- do.call(rbind, list(clusters[[X[1]]], clusters[[X[2]]]))
    meanCb <- list(apply(newCluster, MARGIN = 2, FUN= mean))
        
    # Update the codebooks and the clusters
    clusters[[X[1]]] <- newCluster
    clusters[[X[2]]] <- NULL
    codebooks[[X[1]]] <- meanCb[[1]]
    codebooks[[X[2]]] <- NULL
        
    # recurse to find the next merge-able codebook pair
    mergeClusters(codebooks, clusters)
  } else {
    return(list(clusters =clusters, codebooks = codebooks))
  }
  
}

#_______________________________________________________________________________________
# Main Function to quantize
#______________________________________________________________________________________

vectorQuantize <- function(dat){
  tryCatch({lbgvq <- lbgVQ(dat)}, error=function(err){flog.error("error occoured in lbgVQ:%s",err, name=logNS)}, finally={})
  if(length(lbgvq$codebooks) > 1){
    tryCatch({model <- mergeClusters(lbgvq$codebooks, lbgvq$clusters)}, error=function(err){flog.error("error occoured in mergeClusters:%s",err, name=logNS)}, finally={})
    return(model)
  } else return(lbgvq)
}

# Wrapper for the mapeTest function
#__________________________________-
checkClusterStability <- function(centroid, cluster){
  if(mapeTest(cluster, centroid)){
    return(TRUE)
  } else return(FALSE)
}

#_______________________________________________________________________________________
# Function to find the nearest neighbour
#______________________________________________________________________________________

nearestNeighbour <- function(datum, codebooks, clusters){
  # Distances of the datum drom all the centroids
  distances <- lapply(codebooks, FUN= function(X){ sqrt(sum((X - datum) ^ 2))})
  dataForStatePrediction <<- distances
  #=================================================================================
  # For state prediction
  #=================================================================================
  #cbs <- do.call(rbind, model$codebooks)
  #cbs <- rbind(cbs, datum)
  #dds <- dist(cbs)
  #if(length(dds) != 0){
   # tryCatch({twoDCodebooks <- trilaterate(dds)}, error=function(err){flog.error("error occoured in trilaterate:%s",err, name=logNS)}, finally={})
  #} else {
   # twoDCodebooks <- model$codebooks[[1]][1:2]
  #}
  #twoDDistances <- as.matrix(dist(twoDCodebooks))
  #dataForStatePrediction <<- twoDDistances[nrow(twoDCodebooks),]
  #=================================================================================
  
  # index of the closest centroid
  index <- which(distances==min(unlist(distances)))
  
  # find the nearest cluster and the centroid
  currentcB <- codebooks[index]
  currentCluster=rbind(do.call(rbind, clusters[index]), datum)
  
  # update the centroid
  newMean <- apply(currentCluster, MARGIN = 2, FUN= mean)
  cc <- list(currentCB = newMean, currentCluster=currentCluster, index =index)
   
  return(cc)
}

#_______________________________________________________________________________________
# Function to allocate the membership to the new datum
#______________________________________________________________________________________

pred_VQ = function(datum, model){
  
  # Find the nearet neighbour centroid for the given datum
  tryCatch({currentCluster <- nearestNeighbour(datum, model$codebooks, model$clusters)}, error=function(err){flog.error("error occoured in nearestNeighbour:%s",err, name=logNS)}, finally={})
  
  # check for stability of the cluster using the quantization error and update the 
  # centroid and the cluster appropriately
  if(checkClusterStability(currentCluster$currentCB, currentCluster$currentCluster)){
    # Update the clusters 
    model$clusters[currentCluster$index] <- list(currentCluster$currentCluster)
    model$codebooks[[currentCluster$index]] <-  currentCluster$currentCB
	
	membership <<- currentCluster$index
	# Find the state sequence
	seque <<- append(seque, membership)
	
	if(marketStatus == "open"){
	  # Write the sequence of states computed to file
	  write.csv.ffdf(ffdf(ff(membership)), paste(common__, "/stateSequenceData",sep=""), append =TRUE)
	  flog.info("Wrote sequence to file", name=logNS)
	}
	
    return(model)
  } else {
    flog.info("Clusters have broken.. Recomputing..", name=logNS)
	clusterBreakFlag <<- TRUE
    # recompute the clusters if the cluster is too unstable
    if(errorCode != 900){
	  tryCatch({mod <- vectorQuantize(dat)}, error=function(err){flog.error("error occoured in vectorQuantize:%s",err, name=logNS)}, finally={})
	  # Find the state sequence
	  save(mod,file=paste(common__,"/modelTest.rdata",sep=""))
	  tryCatch({seque <<- findSeque(dat, mod$clusters)}, error=function(err){flog.error("error occoured in findSeque: to compute sequence:%s",err, name=logNS)}, finally={})
      
	  membership <<- seque[length(seque)]
	  
	  if(marketStatus == "open"){
	    # Write the sequence of states computed to file
        ffseque <- ffdf(ff(seque))
	    write.csv.ffdf(ffseque, paste(common__, "/stateSequenceData",sep=""))
	    flog.info("Wrote sequence to file after recomputation", name=logNS)
	  }
	  
	  flog.info("Recomputation successfully done", name=logNS)
      return(mod)
	} else {
	  return("error")
	}
  }
}

#==================================================================
# Function to Trilaterate
# Input : a distance matrix or a 'dist' object
# Output : a set of trilaterated points
#==================================================================

trilaterate <- function(d){
  if(class(d)=='dist'){
    d <- as.matrix(d)
  }
  
  # p1, p2, p3 are the initial triangulation points
  p1 <- c(0,0)
  p2 <- c(0, d[1,2])
  
  if(dim(d)[1]==3){  # If there are only 3 points to trilaterate
  
    p3y <- (d[1,3]^2 + d[1,2]^2 - d[2,3]^2)/(2*d[1,2])
    p3x <- sqrt(d[1,3]^2 - p3y^2)
    p3 <- c(p3x,p3y)	
	pdash <- rbind(p1,p2,p3)	
	colnames(pdash) = c('x', 'y')
    return(pdash)
	
  } else if(dim(d)[1] > 3){ # If there are more than 3 points to trilaterate
    
    p3y <- (d[1,3]^2 + d[1,2]^2 - d[2,3]^2)/(2*d[1,2])
    p3x <- sqrt(d[1,3]^2 - p3y^2)
    p3 <- c(p3x,p3y)
	
    # iterator
    i <- 4:dim(d)[1]
    # compute the remaining points
    pdash <- lapply(i, FUN= function(ind) {
      y <- (d[1,ind]^2 + d[1,2]^2 - d[2,ind]^2)/(2*d[1,2])
      x <- (d[1,ind]^2 + d[1,3]^2 - d[3,ind]^2 - 2*y*p3[2])/(2*p3[1])
      return(c(x, y))
    })
  
    pdash<- do.call(rbind, pdash)
    pdash <- rbind(p1,p2, p3,pdash)
    colnames(pdash) = c('x', 'y')
    return(pdash)
  } else {  # If there are one or 2 points to trilaterate
    pdash <- rbind(p1,p2)
    colnames(pdash) = c('x', 'y')
    return(pdash)
  }
}



#===========================================================================
# Function to make VQ json 
#===========================================================================

makeVQJson <- function(codebooks, membership =NA , clusterBreakFlag=FALSE, dataForStatePrediction=NA, startTimeStamp= NA, endTimeStamp=NA){
  dataForSP <- unname(dataForStatePrediction)
  x <- unname(codebooks[,1])
  y <- unname(codebooks[,2])
  jsonList <- list(x = x, y = y , clusterBreakFlag = clusterBreakFlag, membership = membership, dataForStatePrediction = dataForSP, startTimeStamp = startTimeStamp, endTimeStamp = endTimeStamp)
  return(toJSON(jsonList))
}

#=================================================================================================================================
# Function to compute the VQ Object that has to be sent to the State Prediction agent when the market is closed
#=================================================================================================================================

computeVQObjForMarketsClosed <- function(model, inputJson, startTimeStamp, endTimeStamp){
  
  tryCatch({convJsonToMat(inputJson)},  error=function(err){flog.error("error occoured in json to mat conversion:%s",err, name=logNS)}, finally={})
  currVec <- as.vector(corMat[which(lower.tri(corMat))])
  
  # Compute the nearest neighbour
  tryCatch({currentCluster <- nearestNeighbour(currVec, model$codebooks, model$clusters)}, error=function(err){flog.error("error occoured in nearestNeighbour:%s",err, name=logNS)}, finally={})
  membership <- currentCluster$index
  
  # Reduce the dimensionality of the codebooks to 2
  codebooks <- do.call(rbind, model$codebooks)
  distances <- dist(codebooks)
  if(length(distances) != 0){
    tryCatch({twoDCodebooks <- trilaterate(distances)}, error=function(err){flog.error("error occoured in trilaterate:%s",err, name=logNS)}, finally={})
  } else {
    twoDCodebooks <- model$codebooks[[1]][1:2]
  }
  # Make json
  tryCatch({json <- makeVQJson(twoDCodebooks, membership = membership, clusterBreakFlag="closed", dataForStatePrediction, startTimeStamp, endTimeStamp)}, error=function(err){flog.error("error occoured in makeVQJson:%s",err, name=logNS)}, finally={})
            
  return(json)
 
}

#===========================================================================
# Main function 
#===========================================================================
VQ_main <- function(inputJson=NA){
  if(firstCall || refFlag){
    flog.info("Call made..", name=logNS)
    flog.info("First call made", name=logNS)
    # Read the data
	if(firstCall){
	  tryCatch({dat <<- scanTable(correlationsTable, colFam); startTimeStamp <<- dat[1,1]; dat <<- dat[,-1]}, error=function(err){flog.error("Correlation/MST matrices not found", name=logNS); errorCode <<- 900}, finally={})
    }
	if(errorCode != 900){
      # Run VQ
	  if(firstCall){
	    if(!is.null(dat)){
          tryCatch({system.time(model <<- vectorQuantize(dat))}, error=function(err){flog.error("error occoured in vectorQuantize:%s",err, name=logNS)}, finally={})
        } else {
		  flog.info("Algo charging up..", name=logNS)
	      return("ignore")
	    }
	  } 
      if(exists('model')){	  
        # Reduce the dimensionality of the codebooks to 2
        codebooks <- do.call(rbind, model$codebooks)
        distances <- dist(codebooks)
        if(length(distances) != 0){
          tryCatch({twoDCodebooks <- trilaterate(distances)}, error=function(err){flog.error("error occoured in trilaterate:%s",err, name=logNS)}, finally={})
        } else {
          twoDCodebooks <- model$codebooks[[1]][1:2]
        }
		flog.info("Clusters computed.. Finding state sequence", name=logNS)
		# Find the state sequence
		tryCatch({seque <<- findSeque(dat, model$clusters)}, error=function(err){flog.error("error occoured in findMembers: to compute sequence:%s",err, name=logNS)}, finally={})
		
		# Write the sequence of states computed to file
		ffseque <- ffdf(ff(seque))
		write.csv.ffdf(ffseque, paste(common__, "/stateSequenceData",sep=""))
		flog.info("Wrote sequence to file on first call", name=logNS)
		
		# Make Json
		tryCatch({json <- makeVQJson(twoDCodebooks)}, error=function(err){flog.error("error occoured in makeVQJson:%s",err, name=logNS)}, finally={})
        
		prevJson <<- json
		flog.info("First call successful", name=logNS)
	    firstCall <<- FALSE
		refFlag <<- FALSE
		
		
		save(prevJson ,file=paste(common__,"/LastTessJson.rdata",sep="")) 
		
		   
		return(json)
	  } else {
	      return('error')
	  }
	} else {
	   errorCode <<- 0
	   return("error")
	}
    
  } else {    
    flog.info("Call made..", name=logNS)
    if(!is.na(inputJson)){
	
	  # Extract the error code and the market status from the Cor/MST agent
	  tryCatch({errorCode <<- as.numeric(gsub(".*?ErrorCode.*?:(.*?)}","\\1",inputJson))
	  marketStatus <<- gsub(".*?marketStatus.*?:\"(.*?)\"(.*)", "\\1", inputJson)
	  endTimeStamp <<- gsub(".*?TimeStamp.*?:\"(.*?)\"(.*)", "\\1", inputJson)},  error=function(err){flog.error("error occoured extractoin fields of json:%s",err, name=logNS)}, finally={})
	  	  
	  if(marketStatus == "open" && !refFlag && closedFlag){
	  	refFlag <<- TRUE
		closedFlag <<- FALSE
	  }
	  
	  if(marketStatus == "closed"){
	    closedFlag <<- TRUE
	  }
	  
	  if(is.na(errorCode)){ errorCode <<- 100}
	  if(errorCode != 100){
	    if(marketStatus == "open"){
	  
	      # Convert the json to matrix
          tryCatch({convJsonToMat(inputJson)},  error=function(err){flog.error("error occoured in json to mat conversion:%s",err, name=logNS)}, finally={})
	  
          tryCatch({currVec <- as.vector(corMat[which(lower.tri(corMat))])},  error=function(err){flog.error("error occoured in mat to vec conversion:%s",err, name=logNS)}, finally={})
		  # Append the row to the data in memeory
		  if(is.null(dat)){
		    firstCall <<- TRUE
			return("ignore")
		  } else {
		    dat <<- rbind(dat, currVec)
          }        
          # check for cluster stability
          tryCatch({model <<- pred_VQ(currVec, model)}, error=function(err){flog.error("error occoured in pred_VQ:%s",err, name=logNS); errorCode <<- 1}, finally={})
          if(errorCode != 1){
            # Reduce the dimensionality of the codebooks to 2
            codebooks <- do.call(rbind, model$codebooks)
            distances <- dist(codebooks)
            if(length(distances) != 0){
              tryCatch({twoDCodebooks <- trilaterate(distances)}, error=function(err){flog.error("error occoured in trilaterate:%s",err, name=logNS)}, finally={})
            } else {
              twoDCodebooks <- model$codebooks[[1]][1:2]
            }	  
         	
		    # Make json
            tryCatch({json <- makeVQJson(twoDCodebooks, membership= membership, clusterBreakFlag, dataForStatePrediction, startTimeStamp, endTimeStamp)}, error=function(err){flog.error("error occoured in makeVQJson:%s",err, name=logNS)}, finally={})
            # Reassign the flag to false
		    if(clusterBreakFlag){
		      clusterBreakFlag <<- FALSE
	        }
		    prevJson <<- json
			save(prevJson ,file=paste(common__,"/LastTessJson.rdata",sep=""))
			return(json)
		  } else {
	        errorCode <<- 0 
	        return("error")
	      }
		} else {
		    
			# Compute the json for the new graph 
			tryCatch({json <- computeVQObjForMarketsClosed(model, inputJson, startTimeStamp, endTimeStamp)}, error = function(err){flog.error("Error occoured in computeVQObjForMarketsClosed : %s ", err, name= logNS)}, finally={})
			return(json)
		}
	  } else {
	    errorCode <<- 0 
		flog.info("Dataframe charging..", name=logNS) 
	    return("charging")
	  }
	}
	flog.error("Call made without argument", name=logNS) 
	return("error")
  }
  
}


#_______________________________________________________________________________________
# Call the function for the first time
#prevJson <- VQ_main()
#_______________________________*end*___________________________________________________

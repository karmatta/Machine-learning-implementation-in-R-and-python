require(RCurl)

getURL(url="http://hawttrends.appspot.com/api/terms/")


require(rhbase)
hb.defaults()
# Put a try catch around this:
hb.init(host='172.25.1.39', port=9090, serialize="char", transporttype=0)

#-------------------------------------------------------------------------------------
# The below two functions are used to convert the result of the query which is a R list
# to a R data frame
#-------------------------------------------------------------------------------------

getLengthAllEntries <- function(hbaseList){
  ret <- lapply(hbaseList,FUN=function(x) length(x[[2]]))
  return(unlist(ret))
}

convertListtoMat <- function(hbaseList, rowLength = NULL){
  
  lengthVec <- getLengthAllEntries(hbaseList)
  
  if(!is.null(rowLength)){
    hbaseList[[which(lengthVec != rowLength)]] <- NULL #Drop corrupt entries
  }
  
  tmpList <- lapply(hbaseList,FUN=function(x) as.numeric(unlist(x[[3]])))
  primKey <- lapply(hbaseList,FUN=function(x) as.numeric(unlist(x[[1]])))
  mat <- cbind(do.call(rbind, primKey),do.call(rbind,tmpList))
  #mat <- mat[order(mat[,1]),]
  
  #colnames(mat) <- hbaseList[[1]][[2]]
  
  return(mat)
}

#------------------------------------------------------------------------------------------


# Check if the table exists on HBASE
"ctable" %in% names(hb.list.tables())


# Scan to extract all the records from 'ctable'
rowKeys <- read.csv("/home/musigma/correlationRowKeys.csv")


startTime <- Sys.time()
#rowsD<-hb.scan("ctable",start=1,colspec=c("relations:"))

rowsD<-hb.get("ctable",rowKeys1,colspec=c("relations:"))
rowsList <- rowsD$get()

# Convert the list to a data frame
rowsMat <- convertListtoMat(rowsList)

#--------------------------------------------------------------------
# Fetch the data from the scan obj into R
#--------------------------------------------------------------------
getRows = 1
rowsData <- list(rowsMat)
while(getRows==1){
  rowsList <- rowsD$get()
  if(length(unlist(rowsList))>0){
    rowsMat <- convertListtoMat(rowsList)
    rowsData[[length(rowsData)+1]] <- rowsMat
  } else {
    getRows = 0
  }
}
rowsDataFin <- do.call(rbind, rowsData)
rowsDataFin <- rowsDataFin[order(rowsDataFin[,1]),]
endTime <- Sys.time()
difftime(endTime, startTime)

#----------------------------------------------------------------------------

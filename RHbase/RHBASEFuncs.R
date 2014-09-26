

rowsGot <- hb.scan("ctable",start=1,colspec=c("relations:"))
hbaseList <- rowsGot$get(21)


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
  mat <- mat[order(mat[,1]),]
  
  #colnames(mat) <- hbaseList[[1]][[2]]
  
  return(mat)
}



# Location and port of the thrift server sample<-function(){ hostLoc = '172.25.1.39'
port = 9090
mustangTableName = "MustangTweetsTable"

# load the rhbase package and initialize the hbase connection using thrift.
require(rhbase)
hb.init(host=hostLoc, port=port, serialize="char", transporttype=0) print("HBase initiated") # rows <- hb.scan(mustangTableName,start=1,colspec=c('tweetdata:text'))
rows<-hb.scan.ex(mustangTableName,filterstring="RowFilter(=,'regexstring:(hdfs|database|big data|analytics)')")


getRows = 1
rowsData <- list(rowsD)
while(getRows==1){
  rows <- rows$get(1000)
  if(length(as.numeric(rows))>0){
    rowsData <<- rowsData[[]]
  } else {
    getRows = 0
  }
}

exception<-try(sample())

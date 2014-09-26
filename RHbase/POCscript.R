require(RCurl)

getURL(url="http://hawttrends.appspot.com/api/terms/")


require(rhbase)
hb.defaults()
# Put a try catch around this:
hb.init(host='172.25.1.146', port=9090, serialize="char", transporttype=0)


# -----------------------------------------------------------------------------------------
#initialize rhbase with "raw" serialization
library(rhbase)


#make sure the table does not exist
if ("ctable" %in% names(hb.list.tables())) {
  hb.delete.table("CorrelationDataTable")
}

colNamesOfRelations<-  paste("relations:", colnames(corTab), sep="")
#create new table with columns 'relations'

hb.delete.table("CorrelationDataTable")
hb.new.table("CorrelationDataTable", "relations",opts=list(y=list(compression='GZ')))
hb.insert("CorrelationDataTable",list( list(corTab[1:3,1],colNamesOfRelations[2],list(corTab[1:3,2]))))
 
 
rowsD<-hb.scan("ctable",start=1,colspec=c("relations:"))
rowsD$get()



#insert some values into the table
hb.insert("CorrelationDataTable", list(list(corTab[1,1], colnames(corTab), list(as.numeric(corTab[1,])))))
 Z<-paste("relations:", colnames(corT), sep="")


hb.insert("ctable",list( list(rowx[1], "relations:", list(rowx))))


hb.insert("mytable",list( list(20,c("x","y","z"),list(10001,14,575))))
hb.insert("mytable",list( list(30,c("x","y","z"),list("a string",1000.23,FALSE))))

##scan using a filterstring. This ONLY works on HBase 0.92 or greater!
##
##**NOTE** if you make any mistake on the filterstring syntax, Thrift will throw an TTransportException
##This basically means that the socket connection is dead, and you will have to reinitialize your connection
##That means calling 'hb.init()' again. After you have done that, you are good to go.
rows<-hb.scan.ex("mytable",filterstring="ValueFilter(=,'substring:10001')")
rows$get()

#read all rows of the of the table
rows<-hb.scan("mytable",start=1,colspec=c("x"))
rows$get()

#read just the first row of the table
rows<-hb.scan("mytable",start=1,end=2,colspec=c("x","y","z"))
rows$get()

#read start at the second row of the table and then read the rest
rows<-hb.scan("mytable",start=2,colspec=c("x","y","z"))
rows$get()

hb.new.table("kvpPOC", "value",opts=list(y=list(compression='GZ')))

for(i in 1:5000){ 
timestamp = as.numeric(Sys.time())
AgencyID = paste(c("Agency",(i%%3)+1),collapse=":  ")
key = paste(c(AgencyID,timestamp),collapse=' :: ')
hb.insert("kvpPOC",list( list(key,c("value"),list(paste(c("value is ",i),collapse='')))))
}

cols = list(CF1 = c("F1C1","F1C2"), CF2 = c("F2C1","F2C2"))
hb.new.table("kvpPOCmulCF", "F1C1","F1C2" ,opts=list(y=list(compression='GZ')))
hb.new.table("kvpPOCmulCF-1", "cols" ,opts=list(y=list(compression='GZ')))


rows<-hb.scan("kvpPOC",start=1,end =4,colspec=c("value"))
rows$get()

hb.new.table("test2", "cf1","cf2",opts=list(y=list(compression='GZ')))

hb.new.table("cftablePOC", "x","y",opts=list(y=list(compression='GZ')))
#insert some values into the table
hb.insert("cftablePOC",list( list("row1",c("x:x1","x:x2","x:x3"),list("apple","berry","cherry"))))
hb.insert("cftablePOC",list( list("row2",c("y:y1","y:y2","y:y3"),list(10001,14,575))))
hb.insert("cftablePOC",list( list("row3",c("x:x1","x:x2","y:y1"),list("a string",1000.23,FALSE))))
hb.insert("cftablePOC",list( list("row4",c("x:x1","x:x2","y:y1"),c("vec1","vec2","vec3"))))


##scan using a filterstring. This ONLY works on HBase 0.92 or greater!
##
##**NOTE** if you make any mistake on the filterstring syntax, Thrift will throw an TTransportException
##This basically means that the socket connection is dead, and you will have to reinitialize your connection
##That means calling 'hb.init()' again. After you have done that, you are good to go.
rows<-hb.scan.ex("cftablePOC",filterstring="ValueFilter(=,'substring:ber')")
rows$get()

#read all rows of the of the table
rows<-hb.scan("cftablePOC",start=1,colspec=c("x:x1","y:y2"))
rows$get()

#read just the first row of the table
rows<-hb.scan("cftablePOC",start=1,end=100,colspec=c("x:x1","y:y1"))
rows$get()

#read start at the second row of the table and then read the rest
rows<-hb.scan("cftablePOC",start=2,colspec=c("x:x2"))
rows$get()

key = 'asdfsfg7:12345678:43347984565'
colnames = c('x','y','z')
vals = list("apple","berry","cherry")
hb.insert("mytable",list( list(key,colnames,vals)))

# Rowfiler
rows<-hb.scan.ex(mustangTableName,filterstring="RowFilter(=,'regexstring:.+?')")
pulledtweets = rows$get()

# Make regex patterns

tidregex = "[0-9]{18}"
useridregex = "[0-9]{9}:"
keywordsregex = "[a-zA-Z0-9, ]+,(Samsung|Nokia)"
x = ":;\\)\\(@'\\."

roughRegex = system("python breakRegEx.py 1373300000 1373344800",intern=TRUE)
timeregex = gsub("\\^|\\$",":",roughRegex)

finalpattern = paste(tidregex,timeregex,useridregex,sep="")

grepl(finalpattern,keysample)

regexpattern = '[0-9]{2,}'

regexfilterstring = paste(c("RowFilter(=,'regexstring:",regexpattern,"')"),collapse="")

rows<-hb.scan.ex(mustangTableName,filterstring=regexfilterstring)


"TimestampsFilter"
# Rowfiler
rows<-hb.scan.ex(mustangTableName,filterstring="TimestampsFilter(=,'regexstring:<TS>')")
pulledtweets = rows$get()

rows<-hb.scan.ex("MUESPMonitoring_QA",filterstring="ValueFilter(=,'substring:12')")
rows$get()

hb.init(host = "172.25.1.146",port=9090,serialize="char",transporttype=0)
rows<-hb.scan(mustangTableName,start=1,colspec=c("json:tweetJSON"))
tweets = rows$get(1000)


getmore = 1
while(getmore == 1){
  tweets = rows$get(10000)
  if(length(tweets)>1){
    tweetsvec = c(tweetsvec,tweets)
  } else{
    getmore = 0
  }
}


TweetArray_TopicModel = unlist(sapply(tweetsvec,FUN=function(x) {return(x[[3]])}))
TweetArray_TopicModel = sapply(TweetArray_TopicModel, FUN=function(x) {fromJSON(x)$text})


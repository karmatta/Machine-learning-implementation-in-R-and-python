# ======================================================================================#
#-- Project Name: MINT : Nasdaq News agent
#-- Task : Fetch RSS feeds from Nasdaq and send to UI
#-- Version : 1.0
#-- Date : 25 Apr 2013
#-- Author : Karthik
#-- SVN Directory: \xxxx
#-- Dependent files : 
#-- Dependent agents : 
#-- Comments : 1. 
#=======================================================================================#
#-- Glossary of Error Codes : 1. 400 : Connection to activeMQ on 162.192.100.46 failed
#--                           2.  0  : No error
#--                           3. 800 : Connection error
#--                           4. 11  : R package installation error
#--                           5. 700 : URLs not found
#--                           6. 600 : URLList not found
#=======================================================================================#

#==============================
# Get the environment variables
#==============================
mripPath = Sys.getenv("MRIP_HOME")

#=================================
# Source the agency utilities file
#=================================
source(paste(common__,"/mintUtilities.R",sep=""))

#=================================
# Setting futile logger thresholds
#=================================
logNS <- "nasdaqNews"
flog.threshold(INFO, name=logNS)
flog.appender(appender.file(paste(mripPath,"/MINTlogs/nasdaqNews_info.log",sep="")), name=logNS)

flog.info("R libraries loaded succesfully", name=logNS)

a<-0
number <- 3
newsJson <- ""
firstCall <- TRUE

#___________________________________________________________________________________________________________________________
# Check package version
#____________________________________________________________________________________________________________________________

a[1]<-CheckPackVer("stringr",'0.6.2', logNS)
a[2]<-CheckPackVer("XML",'3.96-1.1', logNS)
a[3]<-CheckPackVer("plyr",'1.8', logNS)
a[4]<-CheckPackVer("lubridate",'1.2.0', logNS)
a[5]<-CheckPackVer("RPostgreSQL",'0.3-3', logNS)
a[6]<-CheckPackVer("futile.logger",'1.3.0', logNS)

if(a[1]|a[2]|a[3]|a[4]|a[5]|a[6]){
  errorCode<<-11
}

#________________________________________________________________________________________
#Loading the required libraries
#_________________________________________________________________________________________

require("XML") 
require("plyr") 
require("stringr")
require("lubridate")
library("RPostgreSQL")
library("futile.logger")

#load the urlList obj
tryCatch({load(paste(common__, "/urlList.RData", sep=""))}, error=function(err){errorCode<<-600;flog.error("RData file not found", name= logNS)}, finally={})
#________________________________________________________________________________________
# Function to extract news from NASDAQ
#_________________________________________________________________________________________

getNewsFromNASDAQ <- function(url){
  
  sym <- symbolVec[which(lapply(symbolVec, FUN=function(X) grep(X, url))==1)]
  
  tryCatch({doc   = xmlTreeParse(url, useInternalNodes = T);}, error=function(err){errorCode<<- 800}, finally={})
  if(errorCode !=800){
    nodes = getNodeSet(doc, "//item");
    newsDf  = ldply(nodes, as.data.frame(xmlToList))
  
    if(nrow(newsDf)!=0){
      # clean up names of data frame
      names(newsDf) = str_replace_all(names(newsDf), "value\\.", "")
      #rearrange the columns
      newsDf$link <- newsDf$origLink
       newsDf <- newsDf[c("title", "link","pubDate", "description")]
    
      # convert pubDate to date-time object and convert time zone
      newsDf$pubDate = strptime(newsDf$pubDate,format = '%a, %d %b %Y %H:%M:%S', tz = 'GMT')
      newsDf$pubDate = with_tz(newsDf$pubDate,  'America/New_York')
      
	  # Check if daylight savings is active or not and append the correct timezone accordingly
	  if(nrow(newsDf) != 0){
	    if(unique(newsDf$pubDate$isdst)[1]){
          newsDf$pubDate = paste(as.character(newsDf$pubDate), 'EDT', sep =" ")
	    } else {
	      newsDf$pubDate = paste(as.character(newsDf$pubDate), 'EST', sep =" ")
	    }
	  }
      
	  # Remove the html tags
      newsDf$description <- gsub( "[(<].*","", newsDf$description)
	  # replace the "" with '' for the sake of jsons
	  newsDf$description <- gsub( "\"","''", newsDf$description)
      newsDf$title <- gsub( "\"","''", newsDf$title)
	  # replace "\n" with " "
	  newsDf$description <- gsub( "\n"," ", newsDf$description)
      newsDf$title <- gsub( "\n"," ", newsDf$title)
	  # replace "\'" with "'"
	  newsDf$description <- gsub( "'","\'", newsDf$description)
      newsDf$title <- gsub( "'","\'", newsDf$title)
    }
	
	# Add a column to indicate which symbol it belongs to
	  if(!is.null(nrow(newsDf))){
	    if(length(sym) != 0){
	      newsDf$Id <- rep(sym, nrow(newsDf))
	    }else{
	      newsDf$Id <- rep("financial", nrow(newsDf))
	    }
	  }
    # truncate the data frame with the "number" of news articles
    if(nrow(newsDf)>=number){
      newsDf <- newsDf[1:number,]
      return(newsDf)
    }else{
      return(newsDf)
    }
  }else{
    errorCode <<- 0
    return(800)
  }
}
#_______________________________________________________________________________________________________________
# Function To make Url Json
#_______________________________________________________________________________________________________________

makeUrlJson <- function(urls){
  urls <- paste(paste("\"",urls,"\"",sep=""), collapse=",")
  json <- paste("\"urls\":[", urls, "]", sep = "")
  return(json)
}
# Function to remove duplicate news
removeDupes <- function(dfr){
  if(anyDuplicated(dfr[,c(1,3,4)])){
     return(removeDupes(dfr[-anyDuplicated(dfr[,c(1,3,4)]),]))
   }
   return(dfr)
}

#________________________________________________________________________________________
# Function to make the news JSON
#________________________________________________________________________________________

makeNewsJson <- function(newsObj, nasdaqUrls){
  if(!is.data.frame(newsObj)){
    # converting the list to a data frame
    dfr <- do.call("rbind", newsObj)
	# sort the news items according to pubTime
	dfr <- dfr[order(dfr$pubDate, decreasing=T),]
	#remove duplicate news
	dfr <- removeDupes(dfr)
  }else{
    dfr <- newsObj
  }
  json <- paste("{\"nasdaqNews\":[{\"title\":", "\"", dfr[1,1], "\"",",\"link\":", "\"",dfr[1,2], "\"",",\"date\":", "\"",dfr[1,3],"\"", ",\"desc\":", "\"",dfr[1,4],"\"", ",\"id\":", "\"", dfr[1,5],"\"", "}", sep="")
  for(i in 2:nrow(dfr)){   
    json <- paste(json, paste("{\"title\":", "\"", dfr[i,1], "\"",",\"link\":", "\"",dfr[i,2], "\"",",\"date\":", "\"",dfr[i,3],"\"", ",\"desc\":", "\"",dfr[i,4],"\"", ",\"id\":", "\"", dfr[i,5],"\"", "}", sep=""),sep=",")
  }
  json <- paste(json,  "],", makeUrlJson(nasdaqUrls), sep="")
  return(json)   
}

#________________________________________________________________________________________
# Update Json function
#________________________________________________________________________________________

updateJson <- function(..., list = character(), file) {
  err<-0
  tryCatch({source(paste(common__,"/latestNewsJsonsLoader.R",sep=""))}, error=function(err){flog.error("can't load the .RData file; Possible reasons: latestNewsJsonsLoader.R not found/latestNewsJsons.RData not found", name= logNS); err<<- 1}, finally={})
  if(err==0){
    previous <- load(file)
  }else{
    previous <- NULL
  }
    var.names <- c(list, as.character(substitute(list(...)))[-1L])
    for (var in var.names) assign(var, get(var, envir = parent.frame()))
    save(list = unique(c(previous, var.names)), file = file)
}

#________________________________________________________________________________________
# Main function
#________________________________________________________________________________________

getNasdaqNews <- function(){ 
  #startTime <- Sys.time()
  # return the last updated json during a redeployment
  if(firstCall){
  
    firstCall <<- FALSE
    flog.info("First call. Loading the last News json and sending the same", name=logNS)
	tryCatch({source(paste(common__,"/latestNewsJsonsLoader.R",sep=""))}, error=function(err){flog.error("can't load the .RData file; Possible reasons: latestNewsJsonsLoader.R not found/latestNewsJsons.RData not found", name= logNS)}, finally={})
    
	if(exists('nasdaqJson')){
	  flog.info("loading successful", name=logNS)
      return(nasdaqJson)
	}
  }


  # Source the urlList 
  tryCatch({source(paste(common__, "/urlListLoader.R", sep=""))}, error=function(err){errorCode<<-600;flog.error("RData file not found", name= logNS)}, finally={})
  
  if(errorCode != 600){
  urlList <<- urlList
  nasdaqUrls <- urlList[grep("nasdaq", urlList)]
  if(length(nasdaqUrls)!=0){
    dframe <- lapply(nasdaqUrls, FUN=getNewsFromNASDAQ)
    if(800 %in% dframe){
	  if(newsJson == ""){
	    flog.error("Some Connection failure, internet down on server", name= logNS)
		errorCode <<- 0
	    nasdaqJson <- "{\"ErrorCode\":800}"
		updateJson(nasdaqJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
        return(nasdaqJson)
	  }else{
	    flog.error("Some Connection failure, internet down on server", name= logNS)
		errorCode <<- 0
        nasdaqJson <- paste(newsJson, ",\"ErrorCode\":800}", sep = "")
		updateJson(nasdaqJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
        return(nasdaqJson)
	  }
    }
    newsJson <<- makeNewsJson(dframe, nasdaqUrls)
	json <- paste(newsJson, ",\"ErrorCode\":", errorCode, "}", sep="")
	nasdaqJson <- json
	updateJson(nasdaqJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
	# To compute total processing time
	#procTime <- difftime(Sys.time(), startTime)
	#flog.info("Total processing time: %s secs",procTime, name="info_news")
	flog.info("Json sent successfully", name=logNS)
    return(nasdaqJson)
    
   }else{
	      flog.warn("The nasdaq Urls's not found", name= logNS)
		  errorCode <<- 0
	      nasdaqJson <- paste("{\"nasdaqNews\":[],", makeUrlJson(nasdaqUrls),  ",\"ErrorCode\":700}", sep="")
		  updateJson(nasdaqJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
          return(nasdaqJson)
	 }
  }else{
	  errorCode <<- 0
      nasdaqJson <- "{\"ErrorCode\":600}"
	  updateJson(nasdaqJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
      return(nasdaqJson)
  }
}



#_______________________________________________*END*_____________________________________________________________
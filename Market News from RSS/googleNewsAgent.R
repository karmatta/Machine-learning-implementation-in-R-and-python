# ======================================================================================#
#-- Project Name: MINT : Google News agent
#-- Task : Fetch RSS feeds from Google and send to UI
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
#source(paste(mripPath,"/wd/mintUtilities.R",sep=""))
source(paste(common__,"/mintUtilities.R",sep=""))
#=================================
# Setting futile logger thresholds
#=================================
logNS <- "googleNews"
flog.threshold(INFO, name=logNS)
flog.appender(appender.file(paste(mripPath,"/MINTlogs/googleNews.log",sep="")), name=logNS)

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
# Function to extract news from Google finance
#_________________________________________________________________________________________

getNewsFromGoogle <- function(url){  

  sym <- symbolVec[which(lapply(symbolVec, FUN=function(X) grep(X, url))==1)]
  
  #to attach the number of urls to be extracted
  url <- paste(url, number, sep="")
  # parse xml tree, get item nodes, extract data and return data frame
  tryCatch({doc   = xmlTreeParse(url, useInternalNodes = T);}, error=function(err){errorCode<<- 800}, finally={})
  if(errorCode !=800){
    nodes = getNodeSet(doc, "//item");
  
    newsDf  = ldply(nodes, as.data.frame(xmlToList))
	
  
    # clean up names of data frame
    names(newsDf) = str_replace_all(names(newsDf), "value\\.", "")
  
    # convert pubDate to date-time object and convert time zone
    newsDf$pubDate = strptime(newsDf$pubDate, format = '%a, %d %b %Y %H:%M:%S', tz = 'GMT')
    newsDf$pubDate = with_tz(newsDf$pubDate,  'America/New_York')
	
	if(nrow(newsDf) != 0){
      # Check if daylight savings is active or not and append the correct timezone accordingly
	  if(unique(newsDf$pubDate$isdst)[1]){
         newsDf$pubDate = paste(as.character(newsDf$pubDate), 'EDT', sep =" ")
	  } else {
	     newsDf$pubDate = paste(as.character(newsDf$pubDate), 'EST', sep =" ")
	  }
	}
  
    # drop guid.text and guid..attrs
    newsDf$guid.text = newsDf$guid..attrs = NULL
  
    # Remove the HTML tags from the description  
    if(nrow(newsDf)!=0){
      desc = getNodeSet(nodes[[1]], "//description")
      desc[[1]] <- NULL
      descVec <- lapply(getNodeSet(htmlParse(unlist(lapply(desc, FUN=xmlValue))), "//div"), FUN=xmlValue)
      
        if(length(descVec) == nrow(newsDf)){
		   newsDf$description <- descVec
		}else {
		  if(length(descVec) %% nrow(newsDf) ==0){
            descVec <- descVec[-which(1:length(descVec) %% 2 ==1)]
          }else{
            descVec[length(descVec)+1] <- descVec[length(descVec)]
            descVec <- descVec[-which(1:(length(descVec)) %% 2 ==1)]      
          }
		  descCol<-do.call("rbind", descVec)
          newsDf$description <- descCol
		}  
	  
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
	
    return(newsDf)  
  }else{
    errorCode <<- 0
    return(800)
  }  
}
#_______________________________________________________________________________________________________________
# Function To make Url Json
#_______________________________________________________________________________________________________________

makeUrlJson <- function(urls){
  urls <- paste(urls, number, sep = "")
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

makeNewsJson <- function(newsObj, googleUrls){
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
  json <- paste("{\"googleNews\":[{\"title\":", "\"", dfr[1,1], "\"",",\"link\":", "\"",dfr[1,2], "\"",",\"date\":", "\"",dfr[1,3],"\"", ",\"desc\":", "\"",dfr[1,4],"\"", ",\"id\":", "\"", dfr[1,5],"\"", "}", sep="")
  for(i in 2:nrow(dfr)){   
    json <- paste(json, paste("{\"title\":", "\"", dfr[i,1], "\"",",\"link\":", "\"",dfr[i,2], "\"",",\"date\":", "\"",dfr[i,3],"\"", ",\"desc\":", "\"",dfr[i,4],"\"", ",\"id\":", "\"", dfr[i,5],"\"", "}", sep=""),sep=",")
  }
  json <- paste(json,  "],", makeUrlJson(googleUrls), sep="")
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

getGoogleNews <- function(){ 
  #startTime <- Sys.time()
  # return the last updated json during a redeployment
  if(firstCall){
  
    firstCall <<- FALSE
    flog.info("First call. Loading the last News json and sending the same", name=logNS)
	tryCatch({source(paste(common__,"/latestNewsJsonsLoader.R",sep=""))}, error=function(err){flog.error("can't load the .RData file; Possible reasons: latestNewsJsonsLoader.R not found/latestNewsJsons.RData not found", name= logNS)}, finally={})
    
	if(exists('googleJson')){
	  flog.info("loading successful", name=logNS)
      return(googleJson)
	}
  }

  # Source the urlList 
  tryCatch({source(paste(common__, "/urlListLoader.R", sep=""))}, error=function(err){errorCode<<-600;flog.error("RData file not found", name= logNS)}, finally={})
  
  if(errorCode != 600){
    urlList <<- urlList
    googleUrls <- urlList[grep("google", urlList)]
	if(length(googleUrls)!=0){
      dframe <- lapply(googleUrls, FUN=getNewsFromGoogle)
      if(800 %in% dframe){
	    if(newsJson == ""){
	      flog.error("Some Connection failure, internet down on server", name= logNS)
		  errorCode <<- 0
	      googleJson <- "{\"ErrorCode\":800}"
		  updateJson(googleJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
          return(googleJson)
	    }else{
	      flog.error("Some Connection failure, internet down on server", name= logNS)
		  errorCode <<- 0
          googleJson <- paste(newsJson, ",\"ErrorCode\":800}", sep = "")
		  updateJson(googleJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
          return(googleJson)
	    }
      }
      newsJson <<- makeNewsJson(dframe, googleUrls)
	  json <- paste(newsJson, ",\"ErrorCode\":", errorCode, "}", sep="")
      googleJson <- json
	  updateJson(googleJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
	  # To compute total processing time
	  #procTime <- difftime(Sys.time(), startTime)
	  #flog.info("Total processing time: %s secs",procTime, name="info_news")
	  flog.info("Json sent successfully", name=logNS)
      return(googleJson)
	}else{
	      flog.warn("The google Urls's not found", name= logNS)
		  errorCode <<- 0
	      googleJson <- paste("{\"googleNews\":[],", makeUrlJson(googleUrls),  ",\"ErrorCode\":700}", sep="")
		  updateJson(googleJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
          return(googleJson)
	 }
  }else{
	  errorCode <<- 0
      googleJson <- "{\"ErrorCode\":600}"
	  updateJson(googleJson, file= paste(common__,"/latestNewsJsons.RData",sep=""))
      return(googleJson)
  }
}



#_______________________________________________*END*_____________________________________________________________
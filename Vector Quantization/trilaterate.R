x <- runif(1000)
y <- runif(1000)
p<- cbind(x,y)
d <- dist(p)
d <- as.matrix(d)

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
  return(pdash)
}

#===================================================================
system.time(pdash<- trilaterate(d))

plot(p[,1], p[,2])
plot(pdash[,1], pdash[,2])
as.matrix(dist(pdash))[1:10, 1:10]
d[1:10, 1:10]

which(round(d, 7) != round(as.matrix(dist(pdash)), 7))
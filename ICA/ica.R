#_________________________________________________________
# Title  : Independent Component Analysis 
# Author : Karthik D
# Date   : Mar 2012 
#_________________________________________________________

#_________________________________________________________
# Kernel-ing
#_________________________________________________________

kern = function(x){
  for(i in 1:nrow(x)){
    for(j in 1:ncol(x)){
      x[i,j] <- (3/4) * (1 - x[i,j] * x[i,j])
    }
  }
  return (x)
}
#_________________________________________________________
# Whitening
#_________________________________________________________

whiten = function(X1, neigen = ncol(X1)){
  # Mean Centring
  for (i in 1:ncol(X1)){
    X1[,i] <- X1[,i] - mean(X1[,i])
  }  
  
  # Non zero eigen values of the cov matrix
  c <- cov(X1)
  b <- eigen(c, only.values = FALSE)
  b$values = sort(b$values, decreasing = TRUE)
  for( i in 1:length(b$values)){
    if(b$values[i] == 0.00){
      b$values <- b$values[-i]
    }
  }
  print(paste("There are", toString(length(b$values)), "non zero Eigen Values"))
  if(neigen > length(b$values)){
    neigen <- length(b$values)
  }
  suppressWarnings(E <- matrix(b$vectors, nrow(c), neigen))
  ET <- t(E)
  D <- NULL
  suppressWarnings(D <- diag(b$values, neigen, neigen))
  # y is the whitened data
  y <- as.matrix(X1) %*% E %*% solve(sqrt(D)) %*% ET 
  return (y)
}

#_________________________________________________________
# Sigmoid density function
#_________________________________________________________

g = function(s){ 
  return (1 / (1 + exp(-s)))
}

#_________________________________________________________
# ICA
#_________________________________________________________

ica = function(X, niter = 50, neigen = ncol(X), alpha = 0.5){
  #y <- kern(X)
  y <- whiten(X, neigen)
  list1 = NULL
  W = matrix(abs(rnorm(nrow(y)^2)), nrow(y), nrow(y))
  for( i in 1:ncol(y)){
    for(k in 1:niter){
      for(j in 1:nrow(W)){
        list1[j] <- 1 - 2 * as.numeric(g(t(as.matrix(W[j,])) %*% as.matrix(y[,i])))
      }
      z <- as.matrix(list1) %*% t(y[,i]) + solve(t(W))
      # (for time series) When the source amplitudes are matching, set alpha <> 0.1 
      # (for time series) When the source amplitudes differ, set alpha <> 1
      W <- W + alpha * z
    }
  }
  S = W %*% X
  return (list(W=W, S=S))
}
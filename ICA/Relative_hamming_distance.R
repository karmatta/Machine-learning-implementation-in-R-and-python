#__________________________________________________________________________
# Title  : Relative Hamming Distance for ICA
# Author : Karthik D
# Date   : Mar 2012
#__________________________________________________________________________

#__________________________________________________________________________
# sign function
#__________________________________________________________________________

sign = function(r1, r2){
  if((r2 - r1) == 0){
    return (0)
  }
  else if((r2 - r1) > 0){
    return (1)
  }
  else{
    return (-1)
  }
}
#__________________________________________________________________________
# RHD function
#__________________________________________________________________________

RHD = function(XO, XP, result){
  sum = 0
  rhd=0
  for(i in 1:nrow(XP)){
    for(t in 1:(ncol(XP)-1)){
      sum = sum + (sign(XO[i, t], XO[i, t+1]) - sign(XP[i, t], XP[i, t+1]))^2
    }
    rhd = rhd + 1/(ncol(XP)-1) * sum
  }
  return(rhd)
}
A = solve(result$W)
A=A[,c(-3,-1, -2)]
S = result$S[c(-3,-1, -2),]
XP = A %*% S
RHD(X, XP, result)





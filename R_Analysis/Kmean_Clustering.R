library("evir")

#list <- c("AIG_PRD.csv", "AIG_QTR.csv", "AIG_WKD.csv", "AIG_YR.csv", "AMZN_PRD.csv", "AMZN_QTR.csv", "AMZN_WKD.csv", "AMZN_YR.csv", "PEP_PRD.csv", "PEP_QTR.csv", "PEP_WKD.csv", "PEP_YR.csv")
list <- c("AIG_PRD.csv", "AMZN_PRD.csv", "PEP_PRD.csv")
#list <- c("AIG_QTR.csv", "AMZN_QTR.csv", "PEP_QTR.csv")
#list <- c("AIG_WKD.csv", "AMZN_WKD.csv", "PEP_WKD.csv")
#list <- c("AIG_YR.csv", "AMZN_YR.csv", "PEP_YR.csv")
#setwd("~/FinalProject")
numcluster <- 3
plotpath <- NULL
plotpath1 <- NULL
plotpath2 <- NULL
plotpath3 <- NULL
for (n in 1:3)
{

SYMBOL <- read.csv(list[n], header = TRUE, sep = ",")
dim(SYMBOL)
names(SYMBOL)
str(SYMBOL)

SYMBOL[1:5,]
SYMBOL[1:10, "VLM"]
SYMBOL[1:10, 2]
#SYMBOL$VLM[1:10]
summary(SYMBOL)
var(SYMBOL[, 2])
var(SYMBOL[, 4])
var(SYMBOL[, 3])
var(SYMBOL[, 2])
cov(SYMBOL[, 1], SYMBOL[, 4])
cov(SYMBOL[, 2], SYMBOL[, 4])
cov(SYMBOL[, 3], SYMBOL[, 4])
cor(SYMBOL[, 1], SYMBOL[, 4])
cor(SYMBOL[, 2], SYMBOL[, 4])
cor(SYMBOL[, 3], SYMBOL[, 4])

plotpath[n] <- file.path("/home","rstudio","FinalProject",paste("Density_", list[n], ".jpg", sep = ""))
jpeg(file=plotpath[n])
mytitle = paste("Density plot of SYMBOL_+1D Volatility", list[n])
plot(density(SYMBOL[, 4]), main = mytitle)
dev.off() 

plotpath[n] <- file.path("/home","rstudio","FinalProject",paste("Pie_Time Stamp_", list[n], ".jpg", sep = ""))
jpeg(file=plotpath[n])
mytitle = paste("Time Stamp Pie plot of SYMBOL_+1D Volatility", list[n])
pie(table(SYMBOL[, 1]), main = mytitle)
dev.off()
hist(SYMBOL[, 1])
hist(SYMBOL[, 4])

#plot(SYMBOL[, 3], SYMBOL[, 4])
#pairs(SYMBOL)

newSYMBOL <- SYMBOL
newSYMBOL$PRD <- NULL
kc <- kmeans(SYMBOL[,-4],numcluster,iter.max=1000)
Comp <- table(SYMBOL[, 1], kc$cluster)
Comp

plotpath[n] <- file.path("/home","rstudio","FinalProject",paste("Cluster_TimeStamp_+1DVol_", list[n], ".jpg", sep = ""))
jpeg(file=plotpath[n])
mytitle = paste("Cluster plot of SYMBOL_+1D Volatility vs. TimeStamp", list[n])
plot(SYMBOL[, 1], SYMBOL[, 4], col=kc$cluster, main = mytitle)
dev.off()

plotpath[n] <- file.path("/home","rstudio","FinalProject",paste("Cluster_Volume_+1DVol_", list[n], ".jpg", sep = ""))
jpeg(file=plotpath[n])
mytitle = paste("Cluster plot of SYMBOL_+1D Volatility vs. Volume", list[n])
plot(SYMBOL[, 2], SYMBOL[, 4], col=kc$cluster, main = mytitle)
dev.off()

plotpath[n] <- file.path("/home","rstudio","FinalProject",paste("Cluster_VIX_+1DVol_", list[n], ".jpg", sep = ""))
jpeg(file=plotpath[n])
mytitle = paste("Cluster plot of SYMBOL_+1D Volatility vs. VIX", list[n])
plot(SYMBOL[, 3], SYMBOL[, 4], col=kc$cluster, main = mytitle)
dev.off()
}
#install.packages("zoo")
#install.packages("forecast")
#install.packages("FinTS")
#install.packages("rugarch")
#install.packages("evir")

library("zoo")
library("forecast")
library("FinTS")
library("rugarch")
library("evir")

stock<-read.zoo("aapl.csv", sep=",", header = TRUE, format = "%Y-%m-%d")
mypath1 <- file.path("/home","rstudio","FinalProject",paste("Stock_Closing_Price", ".jpg", sep = ""))
jpeg(file=mypath1)
mytitle = paste("Closing Prices")
plot(stock, main =mytitle , ylab = "Price (USD)", xlab = "Date")
dev.off()
head(stock)
tail(stock)

#Find the max closing price
stock[which.max(stock)]
stock <- window(stock, start = '2008-01-01', end = '2009-12-31')
stock[which.max(stock)]
head(stock)
tail(stock)
#Simple_return
ret_simple <- diff(stock) / lag(stock, k = -1) 
mypath2 <- file.path("/home","rstudio","FinalProject",paste("Return_in_percent", ".jpg", sep = ""))
jpeg(file=mypath2)
mytitle = paste("% Return")
plot(ret_simple, main =mytitle , ylab = "Return (%)", xlab = "Date")
dev.off()
#ret_cont <- diff(log(stock)) * 100

#Return Stats
summary(coredata(ret_simple))

#Find the min return
ret_simple[which.min(ret_simple)]

quantile(ret_simple, probs = 0.01)
mypath3 <- file.path("/home","rstudio","FinalProject",paste("Stock_Histogram", ".jpg", sep = ""))
jpeg(file=mypath3)
mytitle = paste("Histogram of Simple Returns")
hist(ret_simple, breaks=100, main =mytitle , xlab="%")
dev.off()

Box.test(coredata(ret_simple^2), type = "Ljung-Box", lag = 12)

stock_garch11_spec <- ugarchspec(variance.model = list(garchOrder = c(1, 1)), mean.model = list(armaOrder = c(0, 0)))

stock_garch11_fit <- ugarchfit(spec = stock_garch11_spec, data = ret_simple)

stock_garch11_roll <- ugarchroll(stock_garch11_spec, ret_simple, n.start = 120, refit.every = 1, refit.window = "moving", solver = "hybrid", calculate.VaR = TRUE, VaR.alpha = 0.01, keep.coef = TRUE)

report(stock_garch11_roll, type = "VaR", VaR.alpha = 0.01, conf.level = 0.99)

stock_VaR <- zoo(stock_garch11_roll@forecast$VaR[, 1])

index(stock_VaR) <- as.yearmon(rownames(stock_garch11_roll@forecast$VaR))

stock_actual <- zoo(stock_garch11_roll@forecast$VaR[, 2])
index(stock_actual) <- as.yearmon(rownames(stock_garch11_roll@forecast$VaR))

mypath4 <- file.path("/home","rstudio","FinalProject",paste("VAR", ".jpg", sep = ""))
jpeg(file=mypath4)
mytitle = paste("99% 1 Month VaR Backtesting")
plot(stock_actual, type = "b", main = mytitle, xlab = "Date", ylab = "Return/VaR in percent")
lines(stock_VaR, col = "red")
legend("topright", inset=.05, c("Return","VaR"), col =c("black","red"), lty = c(1,1))
dev.off()

stock_garch11_fcst <- ugarchforecast(stock_garch11_fit, n.ahead = 12)
stock_garch11_fcst

emplot(stock)
emplot(stock, alog ="xy")
qplot(stock, trim=100)
meplot(stock,omit =4)
gpdfit <- gpd(stock, threshold =80)
gpdfit$converged
gpdfit$par.ests
gpdfit$par.ses
tp <- tailplot(gpdfit)
gpd.q(tp, pp=0.999,ci.p=0.95)
gpd.sfall(tp,0.99)
quantile(stock, probs = 0.999,type=l)

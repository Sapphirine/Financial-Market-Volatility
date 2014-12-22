mahout trainlogistic --input /home/ec2-user/Final_Project/logistic/train/final.txt --output /home/ec2-user/Final_Project/logistic/model/model --target Action --predictors Open Close High --types word --features 20 --passes 100 --rate 50 --categories 2

mahout runlogistic --input /home/ec2-user/Final_Project/logistic/train/final.txt --model /home/ec2-user/Final_Project/logistic/model/model --auc --confusion

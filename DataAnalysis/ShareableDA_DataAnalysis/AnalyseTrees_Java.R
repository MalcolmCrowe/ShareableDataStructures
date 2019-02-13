library(ggplot2)
library(reshape2)


source("CommonFunctions.r")

LoadDataSet <- function(filename){
  working_directory = getwd()
  
  fileName = paste(getwd(), filename, sep="/")
  print(fileName)
  
  columns <- c("TestCaseID", "CaseCounter", "TimeDifference", "TotalMemory", "FreeMemory" )
  
  dataSet <- loadFromXLCSV(fileName, TRUE, columns)
  return(dataSet)
}

SetUpDataSet <- function(){
  JavaTreeDataSet <- LoadDataSet("TestTreeTestOutput_Java.csv")
  SSTreeDataSet <- LoadDataSet("SSearchTreeTestOutput_Java.csv")
  
  JavaTreeDataSet$DA <- rep("JavaTree",nrow(JavaTreeDataSet))
  SSTreeDataSet$DA <- rep("SSearchTree",nrow(SSTreeDataSet))
  
  fullDataSet<- rbind(JavaTreeDataSet, SSTreeDataSet)
  #print(fullDataSet)
  
}

plotInsertExecutionTime <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 100 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 100 insert")
  ), ]
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("100 Inserts") + xlab("number of elements")
  
  print(p)
  ###########
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 1000 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 1000 insert")
  ), ]
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("1000 Inserts") + xlab("number of elements")
  
  print(p)
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 10000 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 10000 insert")
  ), ]
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("10000 Inserts") + xlab("number of elements")
  
  print(p)
  
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 100 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 100 insert")
  ), ]
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("100 Inserts") + xlab("number of elements")
  
  print(p)
 
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 1000 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 1000 insert")
  ), ]
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("1000 Inserts") + xlab("number of elements")
  
  print(p)
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 10000 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 10000 insert")
  ), ]
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("10000 Inserts") + xlab("number of elements")
  
  print(p)
  
}

plotplotInsertMemoryConsumptionForInsert <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 100 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 100 insert")
  ), ]
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  
  p <- ggplot(newData,aes(x=CaseCounter,y=UsedMem,colour=DA, group=DA)) + geom_line() +
    ggtitle("100 Inserts / Used Memory") + xlab("number of elements") + ylab("Used Memory Bytes")
  
  print(p)
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 1000 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 1000 insert")
  ), ]
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  
  p <- ggplot(newData,aes(x=CaseCounter,y=UsedMem,colour=DA, group=DA)) + geom_line() +
    ggtitle("1000 Inserts / Used Memory") + xlab("number of elements") + ylab("Used Memory Bytes")
  
  print(p)
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="SSearchTreeTest 10000 insert") |
                                   (fullDataSet$TestCaseID=="TestTreeTest 10000 insert")
  ), ]
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  
  p <- ggplot(newData,aes(x=CaseCounter,y=UsedMem,colour=DA, group=DA)) + geom_line() +
    ggtitle("10000 Inserts / Used Memory") + xlab("number of elements") + ylab("Used Memory Bytes")
  
  print(p)
}

plotFindElementExecutionTime <- function(fullDataset){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="Find elements in 100") |
                                   (fullDataSet$TestCaseID=="Find elements in 100")
  ), ]
  
  dfm <- melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="TimeDifference")
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("Find times") + xlab("number of elements") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
  
}

plotFindElementMemoryConsumption <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="Find elements in 100") |
                                   (fullDataSet$TestCaseID=="Find elements in 100")
  ), ]
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  newData <-newData[ !(newData$CaseCounter==0),]
  
  dfm <- melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="UsedMem")
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("MemoryUsage for Find ith element") + xlab("number of elements") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
  
}

plotDeepCopyAndFind <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="DeepCopyAndFindIn100") |
                                   (fullDataSet$TestCaseID=="DeepCopyAndFindIn100")
  ), ]
  
  newData <-newData[ !(newData$CaseCounter==0),]
  
  dfm = melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="TimeDifference")
  
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("Execution time for Deep Copy and Find element at") + xlab("number of elements") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
  
}

plotMemoryConsumptionDeepCopyAndFind <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="DeepCopyAndFindIn100") |
                                   (fullDataSet$TestCaseID=="DeepCopyAndFindIn100")
  ), ] 
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  newData <-newData[ !(newData$CaseCounter==0),]
  
  dfm <- melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="UsedMem")
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("MemoryUsage for Deep Copy + Find element ith") + xlab("number of elements") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
  
}

fullDataSet <- SetUpDataSet()
plotInsertExecutionTime(fullDataSet)
plotplotInsertMemoryConsumptionForInsert(fullDataSet)
plotFindElementExecutionTime(fullDataSet)
plotFindElementMemoryConsumption(fullDataSet)

plotDeepCopyAndFind(fullDataSet)
plotMemoryConsumptionDeepCopyAndFind(fullDataSet)
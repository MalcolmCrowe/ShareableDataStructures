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
  LinkedListDataSet <- LoadDataSet("LinkedListTestOutput_CSharp.csv")
  SListDataSet <- LoadDataSet("SListTestOutput_CSharp.csv")
  ImmListDataSet <- LoadDataSet("ImutableListTestOutput_CSharp.csv")
  
  LinkedListDataSet$DA <- rep("LinkedList",nrow(LinkedListDataSet))
  SListDataSet$DA <- rep("SList",nrow(SListDataSet))
  ImmListDataSet$DA <- rep("ImmutableList", nrow(ImmListDataSet))
  
  fullDataSet<- rbind(LinkedListDataSet, SListDataSet,ImmListDataSet )
  print(fullDataSet)
  
}

plotInsertExecutionTime <- function(fullDataSet){
  #subsetting to first experiment
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 100 insert") |
                                   (fullDataSet$TestCaseID=="SList 100 insert") |
                                   (fullDataSet$TestCaseID=="Immutable List 100 insert")
  ), ]
  
  
  #meltdf <- melt(newData,id="DA")
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("100 Inserts List in CSharp") + xlab("number of elements")

  #print(p)
  
  #subsetting to first experiment
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 1000 insert") |
                                   (fullDataSet$TestCaseID=="SList 1000 insert") |
                                   (fullDataSet$TestCaseID=="Immutable 1000 insert")
  ), ]
  
  
  #meltdf <- melt(newData,id="DA")
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("10000 Inserts List in CSharp") + xlab("number of elements")
  
  #print(p)
  
  
  #subsetting to first experiment
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 10000 insert") |
                                   (fullDataSet$TestCaseID=="SList 10000 insert") |
                                   (fullDataSet$TestCaseID=="Immutable 10000 insert")
  ), ]
  
  
  #meltdf <- melt(newData,id="DA")
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("10000 Inserts List in CSharp") + xlab("number of elements") + ylab("Elapsed time (1*10^-9")
  
  print(p)
  
  
}

plotInsertMemoryConsumptionForInsert <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 1000 insert") |
                                   (fullDataSet$TestCaseID=="SList 1000 insert") |
                                   (fullDataSet$TestCaseID=="Immutable List 100 insert")
  ), ]
  
  
  #meltdf <- melt(newData,id.vars=list("CaseCounter", "DA"), measure.vars=list("TotalMemory","FreeMemory"))
  
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  
  p <- ggplot(newData,aes(x=CaseCounter,y=UsedMem,colour=DA, group=DA)) + geom_line() +
    ggtitle("100 Inserts / Used Memory") + xlab("number of elements") + ylab("Used Memory Bytes")
  
  
  
  
  #print(p)
  
  ########################
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 1000 insert") |
                                   (fullDataSet$TestCaseID=="SList 1000 insert") |
                                   (fullDataSet$TestCaseID=="Immutable 1000 insert")
  ), ]
  
  
  #meltdf <- melt(newData,id.vars=list("CaseCounter", "DA"), measure.vars=list("TotalMemory","FreeMemory"))
  
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  
  p <- ggplot(newData,aes(x=CaseCounter,y=UsedMem,colour=DA, group=DA)) + geom_line() +
    ggtitle("1000 Inserts / Used Memory") + xlab("number of elements") + ylab("Used Memory Bytes")
  
  
  
  
  #print(p)
  
  ###################
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 10000 insert") |
                                   (fullDataSet$TestCaseID=="SList 10000 insert") |
                                   (fullDataSet$TestCaseID=="Immutable 10000 insert")
  ), ]
  
  
  #meltdf <- melt(newData,id.vars=list("CaseCounter", "DA"), measure.vars=list("TotalMemory","FreeMemory"))
  
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  
  p <- ggplot(newData,aes(x=CaseCounter,y=UsedMem,colour=DA, group=DA)) + geom_line() +
    ggtitle("10000 Inserts / Used Memory") + xlab("number of elements") + ylab("Used Memory (Bytes)")
  
  
  
  
  print(p)
}

plotFindElementExecutionTime <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="Find elements in 100") |
                                   (fullDataSet$TestCaseID=="Find elements in 100") |
                                   (fullDataSet$TestCaseID=="Find elements in 100")
  ), ]
  
  
  
  dfm = melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="TimeDifference")
  
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("Find times") + xlab("number of elements") + ylab("Elapsed time (10^-9 seconds)") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
}

plotplotMemoryConsumptionForFind <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="Find elements in 100") |
                                   (fullDataSet$TestCaseID=="Find elements in 100") |
                                   (fullDataSet$TestCaseID=="Find elements in 100")
  ), ]
  
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  newData <-newData[ !(newData$CaseCounter==0),]
  
  dfm = melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="UsedMem")
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("MemoryUsage for Find times") + xlab("number of elements") + ylab("Used Memory Bytes") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
  
}

plotRemoveExecutionTime <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="Remove elements in 100") |
                                   (fullDataSet$TestCaseID=="Remove elements in 100") |
                                   (fullDataSet$TestCaseID=="Remove elements in 100")
  ), ]
  
  
  
  dfm = melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="TimeDifference")
  
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("Remove times") + xlab("number of elements") + ylab("Elapsed time (10^-9 seconds)") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
}

plotplotMemoryConsumptionForRemove <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="Remove elements in 100") |
                                   (fullDataSet$TestCaseID=="Remove elements in 100") |
                                   (fullDataSet$TestCaseID=="Remove elements in 100")
  ), ]
  
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  newData <-newData[ !(newData$CaseCounter==0),]
  
  dfm = melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="UsedMem")
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("MemoryUsage for Remove times") + xlab("number of elements") + ylab("Used Memory Bytes") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
  
}

plotDeepCopyAndFind <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="DeepCopyAndAddIn100") |
                                   (fullDataSet$TestCaseID=="DeepCopyAndAddIn100") |
                                   (fullDataSet$TestCaseID=="DeepCopyAndAddIn100")
  ), ]
  
  
  
  dfm = melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="TimeDifference")
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("Execution time for Deep Copy and Find element at") + xlab("number of elements") +
    ylab("Elapsed time (1*10^-9") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
}

plotMemoryConsumptionForDeepCopyAndFind <- function(fullDataSet){
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="DeepCopyAndAddIn100") |
                                   (fullDataSet$TestCaseID=="DeepCopyAndAddIn100") |
                                   (fullDataSet$TestCaseID=="DeepCopyAndAddIn100")
  ), ]
  
  newData$UsedMem <- (newData$TotalMemory - newData$FreeMemory )
  
  newData <-newData[ !(newData$CaseCounter==0),]
  
  
  dfm = melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="UsedMem")
  
  p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("MemoryUsage for Deep Copy and Find element at") + xlab("number of elements") + ylab("Used Memory Bytes") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
  
}

fullDataSet <- SetUpDataSet()
#plotInsertExecutionTime(fullDataSet)
#plotInsertMemoryConsumptionForInsert(fullDataSet)
#plotFindElementExecutionTime(fullDataSet)
#plotplotMemoryConsumptionForFind(fullDataSet)
#plotRemoveExecutionTime(fullDataSet)
#plotplotMemoryConsumptionForRemove(fullDataSet)
plotDeepCopyAndFind(fullDataSet)
plotMemoryConsumptionForDeepCopyAndFind(fullDataSet)
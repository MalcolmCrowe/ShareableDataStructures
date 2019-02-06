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
  LinkedListDataSet <- LoadDataSet("LinkedListTestOutput.csv")
  SListDataSet <- LoadDataSet("SListTestOutput.csv")
  
  LinkedListDataSet$DA <- rep("LinkedList",nrow(LinkedListDataSet))
  SListDataSet$DA <- rep("SList",nrow(SListDataSet))
  
  fullDataSet<- rbind(LinkedListDataSet, SListDataSet)
  print(fullDataSet)

}

plotInsertExecutionTime <- function(fullDataSet){
  #subsetting to first experiment
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 100 insert") |
                                (fullDataSet$TestCaseID=="SList 100 insert")
                                ), ]
  
  
  #meltdf <- melt(newData,id="DA")
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("100 Inserts") + xlab("number of elements")
    
  print(p)
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 1000 insert") |
                                   (fullDataSet$TestCaseID=="SList 1000 insert")
  ), ]
  
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA, group=DA)) + geom_line() +
    ggtitle("1.000 Inserts") + xlab("number of elements")
  
  print(p)
  
  # #Zoom in to First 100 inserts
  # newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 1000 insert") |
  #                                  (fullDataSet$TestCaseID=="SList 1000 insert")
  # ), ]
  # newData <- newData[ which( (newData$Cases < 25) ), ]
  # 
  # p <- ggplot(newData,aes(x=Cases,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
  #   ggtitle("10000 Inserts [First 100 cases] ") + xlab("number of elements")
  # 
  # print(p)
  
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 10000 insert") |
                                    (fullDataSet$TestCaseID=="SList 10000 insert")
  ), ]
  
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + geom_line() +
    ggtitle("10.000 insert") + xlab("number of elements")
  
  print(p)
  
  
}

plotRemoveExecutionTime <- function(fullDataSet){
  #subsetting to first experiment
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="Remove elements in 100") |
                                   (fullDataSet$TestCaseID=="Remove elements in 100")
  ), ]
  
 
  
  dfm = melt(newData, id.vars=list("CaseCounter", "DA"), measure.vars="TimeDifference")
 
 
 p <- ggplot(dfm,aes(x=CaseCounter,y=value,colour=DA,fill=DA, group=DA)) + scale_fill_hue(l=40, c=35) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("Remove times") + xlab("number of elements") +
   # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,115), breaks = c(25,50,75,100))
  
  print(p)
}

plotFindElementsIn100 <- function(fulldataSet){
  #subsetting to first experiment
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="Find elements in 100") |
                                   (fullDataSet$TestCaseID=="Find elements in 100")
  ), ]
  
  p <- ggplot(newData,aes(x=CaseCounter,y=TimeDifference,colour=DA,group=DA)) + 
    geom_bar(stat="identity", position = "dodge") +
    ggtitle("Find times") + xlab("number of elements") +
    # scale_x_discrete(limits = c(0,25, 50, 100))
    scale_x_continuous(limits = c(0,125), breaks = c(25,50,75,100))
  
  print(p)
  
}

fullDataSet <- SetUpDataSet()
plotInsertExecutionTime(fullDataSet)
plotRemoveExecutionTime(fullDataSet)
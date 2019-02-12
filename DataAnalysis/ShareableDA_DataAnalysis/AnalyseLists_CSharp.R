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

  print(p)
}

fullDataSet <- SetUpDataSet()
plotInsertExecutionTime(fullDataSet)
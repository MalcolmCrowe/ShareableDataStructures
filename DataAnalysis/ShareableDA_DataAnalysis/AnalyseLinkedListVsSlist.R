source("CommonFunctions.r")
library("ggplot2")
library(reshape2)

LoadDataSet <- function(filename){
  working_directory = getwd()
  
    fileName = paste(getwd(), filename, sep="/")
  print(fileName)
   
  columns <- c("TestCaseID", "UniqueId", "TimeDifference", "TotalMemory", "FreeMemory" )
  
  dataSet <- loadFromXLCSV(fileName, TRUE, columns)
  # return(dataSet)
}

SetUpDataSet <- function(){
  LinkedListDataSet <- LoadDataSet("LinkedListTestOutput.csv")
  SListDataSet <- LoadDataSet("SListTestOutput.csv")
  
  LinkedListDataSet$DA <- rep("LinkedList",nrow(LinkedListDataSet))
  SListDataSet$DA <- rep("SList",nrow(SListDataSet))
  
  fullDataSet<- rbind(LinkedListDataSet, SListDataSet)
  print(fullDataSet)

}

plotExecutionTime <- function(fullDataSet){
  #subsetting to first experiment
  newData <- fullDataSet[ which( (fullDataSet$TestCaseID=="LinkedList 100 insert") |
                                (fullDataSet$TestCaseID=="SList 100 insert")
                                ), ]
  
  
  #meltdf <- melt(newData,id="DA")
  p <- ggplot(newData,aes(x=UniqueId,y=TimeDifference,colour=DA,group=DA)) + geom_line()
  print(p)
  
}

fullDataSet <- SetUpDataSet()
plotExecutionTime(fullDataSet)
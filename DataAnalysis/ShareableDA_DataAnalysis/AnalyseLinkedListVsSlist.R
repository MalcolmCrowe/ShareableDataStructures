source("CommonFunctions.r")

LoadDataSet <- function(){
  working_directory = getwd()
  
  print(working_directory)
  
  
  # fileName = paste(getwd(), "FindBugsExtraction.csv", sep="/")
  # print(fileName)
  # 
  # columns <- c("Category", "BugKind", "RuleName", "BugRank", "Project", "Observations" )
  # 
  # 
  # dataSet <- loadFromXLCSV(fileName, TRUE, columns)
  # return(dataSet)
}

LoadDataSet()
loadFromXLCSV <- function(fullyQualifiedPath, dataHasHeaders, columnsNamesDefinition){
  data_set = read.csv(fullyQualifiedPath,
                      header = dataHasHeaders,
                      col.names = columnsNamesDefinition
  )
  return(data_set)
}
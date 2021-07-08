library(redcapAPI)
library(stringr)
library(dplyr)
library(tidyr)

ReadData <- function(api.url, api.token, relevant.fields, 
                     relevant.field.types) {
  
  rcon <- redcapConnection(api.url, api.token)
  data <- exportRecords(
    rcon    = rcon,
    factors = F,
    fields  = relevant.fields,
    labels  = F,
    colClasses = relevant.field.types
  )
  
  return(data)
}

ExportDataAllHealthFacilities <- function(redcap.api.url, redcap.tokens, 
                                          variables, types) {
  # Export data from each of the ICARIA Health Facility REDCap projects and 
  # append all data sets in a unique data frame for analisys.
  #
  # Args:
  #   redcap.api.url: String representing the URL to access the REDCap API.
  #   redcap.tokens:  List of tokens (Strings) to access each of the ICARIA 
  #                   REDCap projects.
  #   variables:      List of variables to be extracted from the REDCap project.
  #   types:          List of types of the variables to be extracted from the
  #                   REDCap project.
  # 
  # Returns:
  #   Data frame with all the data together of different ICARIA Health 
  #   Facilities.
  
  data <- data.frame()
  for (hf in names(redcap.tokens)) {
    if (hf != "profile") {
      print(paste("Extracting data from", hf))
      hf.data <- ReadData(redcap.api.url, redcap.tokens[[hf]], variables, types)
      # TODO: Bind column for the HF district
      hf.data <- cbind(hf = hf, hf.data)
      data <- rbind(data, hf.data)
    }
  }
  
  return(data)
}

ExportDataScreeningLog <- function(redcap.api.url, redcap.token, 
                                          variables, types) {
  
  data <- data.frame()
  print("Extracting data from Screening Log")
  log.data <- ReadData(redcap.api.url, redcap.token, variables, types)
  
  # Collapse HF IDs in the hf column no matter in which district the HF is and
  # format the code accrodingly HFXX
  log.data$hf <- rowSums(
    x     = log.data[, c("hf_bombali", "hf_tonkolili", "hf_port_loko")], 
    na.rm = T
  )
  log.data$hf <- str_pad(log.data$hf, 2, "left", "0")
  log.data$hf <- paste0("HF", log.data$hf)
  
  return(log.data)
}

TransformRemoveEmptyRows <- function(data) {
  
  # Empty row definition 1: Any row in which we don't have any of the complete 
  #                         variables equals to 2 (Complete)
  # TODO: Improve filtering moving out the list of complete colum names
  empty.filter1 <- data$screening_complete == 2 | 
    data$intervention_complete == 2 | data$sae_complete == 2 | 
    data$withdrawal_complete == 2 | data$death_complete == 2
  
  aux <- data[which(empty.filter1), ]
  
  # Empty row definition 2: Any intervention row in which AZi/Pbo was not 
  #                         administered, i.e. we have int_date but int_azi != 1
  empty.filter2 <- (is.na(aux$int_date) & is.na(aux$int_azi)) | 
    (!is.na(aux$int_date) & aux$int_azi == 1)
  
  aux <- aux[which(empty.filter2), ]
  
  return(aux)
}

TransformAddLeadingZeros <- function(data, column, width) {
  
  aux <- data
  aux[[column]] <- str_pad(aux[[column]], width, "left", "0")
  
  return(aux)
}

TransformPivotAZiVars <- function(data) {
  
  # Create new global ID variable based on HF and record id and subset only
  # required AZi/Pbo columns and rows
  aux <- data
  aux <- aux[, c("hf", "record_id", "int_azi", "int_random_letter", "int_date")]
  aux$id <- paste(aux$hf, aux$record_id, sep = "_")
  aux <- aux[which(!is.na(aux$int_date)), ]
  
  # Number AZi/Pbo doses for each participant
  aux <- group_by(aux, id)
  aux <- mutate(aux, azi_dose = row_number())
  
  # Pivot AZi/Pbo doses from rows to columns
  aux.wide <- pivot_wider(aux, names_from = azi_dose, values_from = int_date)
  
  # Rename columns new created AZi/Pbo columns and remove auxiliar columns
  azi.col.names <- c("hf", "record_id", "int_azi", "int_random_letter", "id", 
                     "azi1_date", "azi2_date", "azi3_date")
  l <- length(azi.col.names)
  if ("3" %in% colnames(aux.wide)) {
    colnames(aux.wide) <- azi.col.names
  } else if ("2" %in% colnames(aux.wide)) {
    colnames(aux.wide) <- azi.col.names[-l]
    aux.wide[azi.col.names[l]] <- NA
  } else {
    colnames(aux.wide) <- azi.col.names[-c(l - 1, l)]
    aux.wide[azi.col.names[l - 1]] <- NA
    aux.wide[azi.col.names[l]] <- NA
  }
  
  aux.wide <- aux.wide[, c("hf", "record_id", "int_random_letter", "azi1_date", 
                           "azi2_date", "azi3_date")]
  
  return(aux.wide)
}

TransformCreateParticipantTable <- function(data) {
  
  
}

TransformCreateSAETable <- function(data) {
  
}
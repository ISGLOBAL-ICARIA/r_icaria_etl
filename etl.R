library(redcapAPI)
library(stringr)
library(dplyr)
library(tidyr)

ReadData <- function(api.url, api.token, relevant.fields, 
                     relevant.field.types) {
  # Read data through the REDCap API. It retrieves only the indicated fields and
  # casts them according to the provided data types.
  #
  # Args:
  #   api.url:              String representing the URL to access the REDCap 
  #                         API.
  #   api.token:            Strings representing the token to access ICARIA 
  #                         HF-specific REDCap project.
  #   relevant.fields:      List of variables to be extracted from the REDCap 
  #                         project.
  #   relevant.field.types: List of types of the variables to be extracted from 
  #                         the REDCap project.
  # 
  # Returns:
  #   Data frame with the ICARIA HF data.
  
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
  # Export data from the ICARIA Trial Profile project that stores aggregated 
  # dailyd data coming from the screening log by health facility.
  #
  # Args:
  #   redcap.api.url: String representing the URL to access the REDCap API.
  #   redcap.token:   Strings representing the token to access the ICARIA Trial 
  #                   Profile REDCap project.
  #   variables:      List of variables to be extracted from the REDCap project.
  #   types:          List of types of the variables to be extracted from the
  #                   REDCap project.
  # 
  # Returns:
  #   Data frame with the daily aggregated data of the screening log by HF.
  
  data <- data.frame()
  print("Extracting data from Screening Log")
  log.data <- ReadData(redcap.api.url, redcap.token, variables, types)
  
  return(log.data)
}

TransformRemoveEmptyRows <- function(data) {
  # Remove empty rows from the HFs data frame according to the following 
  # definitions of empty rows:
  #   - Definition 1: Any row in which we don't have any of the complete 
  #                   variables equals to 2 (Complete).
  #   - Definition 2: Any intervention row in which AZi/Pbo was not 
  #                   administered, i.e. we have int_date but int_azi != 1.
  #
  # Args:
  #   data: Data frame with the trial data of all ICARIA Health Facilites.
  # 
  # Returns:
  #   Data frame without the empty rows.
  
  # Empty row definition 1
  # TODO: Improve filtering moving out the list of complete colum names
  empty.filter1 <- data$screening_complete == 2 | 
    data$intervention_complete == 2 | data$sae_complete == 2 | 
    data$withdrawal_complete == 2 | data$death_complete == 2 |
    data$household_follow_up_complete == 2
  
  aux <- data[which(empty.filter1), ]
  
  # Empty row definition 2
  empty.filter2 <- (is.na(aux$int_date) & is.na(aux$int_azi)) | 
    (!is.na(aux$int_date) & aux$int_azi == 1)
  
  aux <- aux[which(empty.filter2), ]
  
  return(aux)
}

TransformAddLeadingZeros <- function(data, column, width) {
  # Add leading zeros to all the values of the indicated column to have the
  # defined fixed width.
  #
  # Args:
  #   data:   Data frame with the data.
  #   column: String representing the name of the column in which the leading
  #           zeros have to be added.
  #   width:  Integer representing the fixed width of the values after adding
  #           the leading zeros.
  # 
  # Returns:
  #   Data frame with the leading zeros in the indicated column.
  
  aux <- data
  aux[[column]] <- str_pad(aux[[column]], width, "left", "0")
  
  return(aux)
}

TransformCollapseColumns <- function(data, columns, new.column) {
  # Collapse the indicated set of columns in the new indicated column. This is
  # useful when the variable values are spread in several REDCap fields.
  #
  # Args:
  #   data:       Data frame with the data.
  #   columns:    Vector with the names of the columns to be collapsed.
  #   new.column: String representing the name of the new column to be created
  #               and where the collapse is going to be done.
  # 
  # Returns:
  #   Data frame equals to the one passed to the function but with a new column
  #   in which the spread values have been collapsed.
  
  data[new.column] <- rowSums(
    x     = data[, columns], 
    na.rm = T
  )
  
  return(data)
}

TransformAddPrefix <- function(data, column, prefix) {
  data[column] <- paste0("HF", data[[column]])
  
  return(data)
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
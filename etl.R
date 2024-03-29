library(redcapAPI)
library(stringr)
library(dplyr)
library(tidyr)
library(httr)
library(jsonlite)
library(googledrive)

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

TransformAddICD10Description <- function(data, icd.10.column, new.desc.column, 
                                         bioportal.api.url, bioportal.api.key) {
  # Retrieve the ICD10 code description from the BioPortal API and add a new
  # column to the data frame with this data.
  #
  # Args:
  #   data:              Data frame with the data.
  #   icd.10.column:     String representing the name of the column in which the
  #                      ICD10 code is stored.
  #   new.desc.column:   String representing the name of the new column to be 
  #                      created where the ICD10 code description will be 
  #                      stored.
  #   bioportal.api.url: String representing the BioPortal's API URL.
  #   bioportal.api.key: String representing the key to authenticate in the 
  #                      BioPortal's API.
  # 
  # Returns:
  #   Data frame equals to the one passed to the function but with a new column
  #   in which the ICD10 code description has been stored.

  kICD10BioportalOntology <- "ICD10"
  
  icd.10.codes <- unique(data[!is.na(data[icd.10.column]), icd.10.column])
  for (icd.10.code in icd.10.codes) {
    print(paste("Retrieving BioPortal", kICD10BioportalOntology, 
                "Ontology Class for code", icd.10.code))
    res = GET(bioportal.api.url, query = list(
      apikey     = bioportal.api.key, 
      ontologies = kICD10BioportalOntology, 
      q          = icd.10.code
    ))
    
    icd.10.class <- fromJSON(rawToChar(res$content))
    data[which(data[icd.10.column] == icd.10.code), new.desc.column] <- 
      icd.10.class$collection$prefLabel[1]
  }
  
  return(data)
}

TransformCreateParticipantTable <- function(data) {
  
  
}

TransformCreateSAETable <- function(data) {
  
}

LoadAuthorize <- function(token) {
  # Authorize google drive to view and manage the ICARIA Drive files. This 
  # authorization is done through a service account (in Google Cloud 
  # Organization) and by using a token. This service account is managed in the
  # context of a Google Cloud Project. This project and its resources can be
  # configured through https://console.cloud.google.com/
  #
  # Args:
  #   token: String representing the path to the Google Service Account token 
  #          json file containing the private key to authenticate and access 
  #          Google Drive.
  # 
  # Returns:
  #   Nothing
  
  # options(gargle_verbosity = "debug")
  print("Authorizing into Google")
  drive_auth(path = token)
}

LoadDataFile <- function(data.date, drive.data.path, file.path, filename) {
  # Upload a data file to Google Drive. Before using this function the 
  # authentication with Google Cloud should have happened sucessfully (See
  # function LoadAuthorize). Furthermore, the Google Drive API must be enabled
  # into the Google Cloud Platform and the authenticated service account should
  # have permissions to use this API. Data files will be uploaded in the 
  # specified Drive path and organized by year and month. These year and month
  # sub-directories are created automatically vy this functions if they are not
  # in place yet.
  #
  # Args:
  #   data.date:       POSIXct date when the data file was produced.
  #   drive.data.path: String representing the base folder where ETL data files 
  #                    will be arranged and stored.
  #   file.path:       String representing the specific folder where this 
  #                    concrete data file will be stored. The specific folder 
  #                    should be created under the base folder (indicated in 
  #                    drive.data.path).
  #   filename:        String representing the name of the data file to be 
  #                    uploaded.
  # 
  # Returns:
  #   Nothing
  
  # Build Google Drive file path and check if it exits or create it otherwise
  year <- format(data.date, format = "%Y")
  month <- format(data.date, format = "%m")
  
  file.path <- paste(drive.data.path, file.path, year, sep = "/")
  drive.path <- drive_get(file.path)
  if (nrow(drive.path) == 0) {
    drive_mkdir(file.path)
  }
  
  file.path <- paste(file.path, month, sep = "/")
  drive.path <- drive_get(file.path)
  if (nrow(drive.path) == 0) {
    drive_mkdir(file.path)
  }
  
  file.path <- paste0(file.path, "/")
  
  print(paste("Writing CSV files into Google:", filename, "at",  file.path))
  drive <- drive_upload(
    media = filename,
    path  = file.path,
    name  = filename
  )
}
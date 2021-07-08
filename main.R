source("etl.R")
source("tokens.R")

# READ MODEL (ENTITIES) --------------------------------------------------------
participant <- read.csv(
  file             = "participant.csv", 
  stringsAsFactors = F, 
  strip.white      = T,
  comment.char = "#"
)
azi <- read.csv(
  file             = "azi.csv", 
  stringsAsFactors = F, 
  strip.white      = T,
  comment.char = "#"
)
withdrawal <- read.csv(
  file             = "withdrawal.csv", 
  stringsAsFactors = F, 
  strip.white      = T,
  comment.char = "#"
)
death <- read.csv(
  file             = "death.csv", 
  stringsAsFactors = F, 
  strip.white      = T,
  comment.char = "#"
)
sae <- read.csv(
  file             = "sae.csv",
  stringsAsFactors = F,
  strip.white      = T,
  comment.char = "#"
)

model <- rbind(participant, azi, withdrawal, death, sae)

# EXTRACT DATA -----------------------------------------------------------------
# Extract data from all databases (one per ICARIA HF) accroding to the data
# model to be produced
db.filter <- which(model$source == "db")
db.variables <- model$type[db.filter]
names(db.variables) <- model$variable[db.filter]

data <- ExportDataAllHealthFacilities(
  redcap.api.url = kRedcapAPIURL, 
  redcap.tokens  = kRedcapTokens, 
  variables      = names(db.variables),
  # TODO: Force interpretation of column types to improve efficiency while 
  #       reading and bad type casting
  types          = NA
)

# TRANSFORM DATA ---------------------------------------------------------------
data <- TransformRemoveEmptyRows(data)

# TODO: This transformation won't be required once we export data forcing types
#       as this variables will be exported as a character with the corresponding
#       leading zeros.
data <- TransformAddLeadingZeros(data, "screening_number", 5)


# ORGANIZE DATA (TABLES): PARTICIPANTS AND SAES --------------------------------
# Get screening variables for all participants (recruited + screening failures)
participants <- data[which(!is.na(data$screening_number)), participant$variable]

# Merge withdrawal and death data to the participants table
withdrawals <- data[which(!is.na(data$wdrawal_reported_date)), 
                    c("hf", "record_id", withdrawal$variable)]
participants <- merge(
  x     = participants, 
  y     = withdrawals, 
  by    = c("hf", "record_id"), 
  all.x = TRUE
)

deaths <- data[which(!is.na(data$death_reported_date)), 
               c("hf", "record_id", death$variable)]
participants <- merge(
  x     = participants, 
  y     = deaths, 
  by    = c("hf", "record_id"), 
  all.x = TRUE
)

# Get all AZi/Pbo doses by participant in a one-row-per-participant fashion and
# merge these data to the participants table
azi.doses <- TransformPivotAZiVars(data)
participants <- merge(
  x     = participants, 
  y     = azi.doses, 
  by    = c("hf", "record_id"), 
  all.x = TRUE
)

# Order participants columns and rows
participants$record_id <- as.integer(participants$record_id)
participants <- participants[order(participants$hf, participants$record_id), ]


# TODO: Merge End of Follow Up data - last visit to the household

# Get SAE information to create the SAEs table
saes <- data[which(data$sae_complete == 2), c("hf", "record_id", sae$variable)]
saes$study_number <- substr(saes$sae_number, 1, 9)

# Order saes columns and rows
saes$record_id <- as.integer(saes$record_id)
saes <- saes[order(saes$hf, saes$record_id), 
             c("hf", "record_id", "study_number", sae$variable)]
                           

# Load data
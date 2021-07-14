source("etl.R")
source("tokens.R")

# READ MODEL (ENTITIES) --------------------------------------------------------
participant <- read.csv(
  file             = "participant.csv", 
  stringsAsFactors = F, 
  strip.white      = T,
  comment.char     = "#"
)
azi <- read.csv(
  file             = "azi.csv", 
  stringsAsFactors = F, 
  strip.white      = T,
  comment.char     = "#"
)
withdrawal <- read.csv(
  file             = "withdrawal.csv", 
  stringsAsFactors = F, 
  strip.white      = T,
  comment.char     = "#"
)
death <- read.csv(
  file             = "death.csv", 
  stringsAsFactors = F, 
  strip.white      = T,
  comment.char     = "#"
)
sae <- read.csv(
  file             = "sae.csv",
  stringsAsFactors = F,
  strip.white      = T,
  comment.char     = "#"
)
household <- read.csv(
  file             = "household.csv",
  stringsAsFactors = F,
  strip.white      = T,
  comment.char     = "#"
)

screening.log <- read.csv(
  file             = "screening_log.csv",
  stringsAsFactors = F,
  strip.white      = T,
  comment.char = "#"
)

model.trial <- rbind(participant, azi, withdrawal, death, sae, household)
model.log <- rbind(screening.log)

# EXTRACT DATA -----------------------------------------------------------------
# Extract data from all databases (one per ICARIA HF) accroding to the data
# model to be produced
db.filter <- which(model.trial$source == "db")
db.variables <- model.trial$type[db.filter]
names(db.variables) <- model.trial$variable[db.filter]

data <- ExportDataAllHealthFacilities(
  redcap.api.url = kRedcapAPIURL, 
  redcap.tokens  = kRedcapTokens, 
  variables      = names(db.variables),
  # TODO: Force interpretation of column types to improve efficiency while 
  #       reading and bad type casting
  types          = NA
)

# Extract data from the screening log database according to the data model to be
# produced
db.filter <- which(model.log$source == "db")
db.variables <- model.log$type[db.filter]
names(db.variables) <- model.log$variable[db.filter]

log <- ExportDataScreeningLog(
  redcap.api.url = kRedcapAPIURL, 
  redcap.token   = kRedcapTokens[["profile"]], 
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

log <- TransformCollapseColumns(
  data       = log, 
  columns    = c("hf_bombali", "hf_tonkolili", "hf_port_loko"), 
  new.column = "hf"
)

log <- TransformAddLeadingZeros(log, "hf", 2)

log <- TransformAddPrefix(log, "hf", "HF")


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
  all.x = T
)

deaths <- data[which(!is.na(data$death_reported_date)), 
               c("hf", "record_id", death$variable)]
participants <- merge(
  x     = participants, 
  y     = deaths, 
  by    = c("hf", "record_id"), 
  all.x = T
)

# Get all AZi/Pbo doses by participant in a one-row-per-participant fashion and
# merge these data to the participants table
azi.doses <- TransformPivotAZiVars(data)
participants <- merge(
  x     = participants, 
  y     = azi.doses, 
  by    = c("hf", "record_id"), 
  all.x = T
)

# Merge last household visit at the end of the follow up, 
# i.e. event HH-At 18th month of age
end.fu.visits <- data[
  which(data$redcap_event_name == "hhat_18th_month_of_arm_1"), 
  c("hf", "record_id", household$variable)]
participants <- merge(
  x     = participants,
  y     = end.fu.visits,
  by    = c("hf", "record_id"),
  all.x = T
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

# Get Screening Log information to create the Screening Log table
logs <- log[which(!is.na(log$screening_date)), 
                 screening.log$variable[which(screening.log$load == 1)]]
                           

# Load data
kVersionFormat <- "%Y%m%d"
kCSVExtension <- ".csv"
kParticipantsFile <- "participants"
kSAEs <- "saes"
kLog <- "screening_log"

data.date <- Sys.time()

participants.filename <- paste0(
  kParticipantsFile, 
  "_", 
  format(data.date, format = kVersionFormat), 
  kCSVExtension
)
write.csv(participants, file = participants.filename, row.names = F)

saes.filename <- paste0(
  kSAEs, 
  "_", 
  format(data.date, format = kVersionFormat), 
  kCSVExtension
)
write.csv(saes, file = saes.filename, row.names = F)

log.filename <- paste0(
  kLog, 
  "_", 
  format(data.date, format = kVersionFormat), 
  kCSVExtension
)
write.csv(logs, file = log.filename, row.names = F)
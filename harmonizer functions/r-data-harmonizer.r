# Data Harmonizer for opendata.swiss Files
# This script helps standardize and merge data files based on inspection results

# Load required libraries
library(readr)        # For reading CSV files
library(readxl)       # For reading Excel files
library(jsonlite)     # For reading JSON files
library(dplyr)        # For data manipulation
library(purrr)        # For functional programming
library(stringr)      # For string manipulation
library(lubridate)    # For date handling
library(tidyr)        # For data tidying
library(tools)        # For file operations

# Set up logging
log_info <- function(message) {
  cat(paste(Sys.time(), "- INFO -", message, "\n"))
  
  # Also append to log file
  write(paste(Sys.time(), "- INFO -", message), 
        file = "data_harmonization.log", 
        append = TRUE)
}

log_warning <- function(message) {
  cat(paste(Sys.time(), "- WARNING -", message, "\n"))
  
  # Also append to log file
  write(paste(Sys.time(), "- WARNING -", message), 
        file = "data_harmonization.log", 
        append = TRUE)
}

log_error <- function(message) {
  cat(paste(Sys.time(), "- ERROR -", message, "\n"))
  
  # Also append to log file
  write(paste(Sys.time(), "- ERROR -", message), 
        file = "data_harmonization.log", 
        append = TRUE)
}

# Function to detect CSV delimiter
detect_delimiter <- function(file_path) {
  # Read first few lines
  conn <- file(file_path, "r")
  first_lines <- readLines(conn, n = 5)
  close(conn)
  
  # Count potential delimiters
  delimiters <- c(",", ";", "\t", "|")
  counts <- sapply(delimiters, function(d) {
    sum(stringr::str_count(first_lines, fixed(d)))
  })
  
  # Return most common delimiter
  return(delimiters[which.max(counts)])
}

# Function to standardize field name
standardize_field_name <- function(field_name) {
  # Convert to lowercase
  standardized <- tolower(field_name)
  
  # Replace spaces and special characters with underscores
  standardized <- str_replace_all(standardized, "[^a-z0-9]+", "_")
  
  # Convert CamelCase to snake_case
  standardized <- str_replace_all(standardized, "([a-z0-9])([A-Z])", "\\1_\\2")
  standardized <- tolower(standardized)
  
  # Remove consecutive underscores
  standardized <- str_replace_all(standardized, "_+", "_")
  
  # Remove leading and trailing underscores
  standardized <- str_trim(standardized, "both")
  standardized <- str_replace_all(standardized, "^_+|_+$", "")
  
  return(standardized)
}

# Function to convert column to datetime
convert_to_datetime <- function(df, column) {
  # Save original values
  original <- df[[column]]
  success <- FALSE
  
  # Try direct conversion with lubridate
  tryCatch({
    # First try automatic parsing
    df[[column]] <- parse_date_time(df[[column]], orders = c(
      "ymd", "dmy", "mdy", "ydm", "ymd HMS", "dmy HMS", "mdy HMS",
      "ymd HM", "dmy HM", "mdy HM"
    ))
    
    # Calculate success rate
    success_rate <- 1.0 - (sum(is.na(df[[column]])) / length(df[[column]]))
    
    if (success_rate >= 0.8) {
      log_info(paste("Converted column", column, "to datetime with", 
                     round(success_rate * 100, 2), "% success rate"))
      success <- TRUE
    } else {
      # Restore original values
      df[[column]] <- original
    }
  }, error = function(e) {
    log_warning(paste("Failed to convert", column, "to datetime using lubridate:", e$message))
  })
  
  # If direct conversion failed, try specific formats
  if (!success) {
    date_formats <- c(
      "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y", 
      "%Y.%m.%d", "%d.%m.%Y", "%Y%m%d", "%d.%m.%Y %H:%M",
      "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"
    )
    
    best_format <- NULL
    best_success_rate <- 0
    
    for (date_format in date_formats) {
      tryCatch({
        # Convert strings to datetime using the current format
        temp_values <- as.POSIXct(as.character(df[[column]]), format = date_format)
        
        # Count successful conversions
        success_rate <- 1.0 - (sum(is.na(temp_values)) / length(temp_values))
        
        if (success_rate > best_success_rate) {
          best_success_rate <- success_rate
          best_format <- date_format
          
          # If more than 80% converted successfully, we're good
          if (success_rate >= 0.8) {
            break
          }
        }
      }, error = function(e) {
        # Skip formats that cause errors
      })
    }
    
    # Apply the best format if found
    if (!is.null(best_format) && best_success_rate >= 0.5) {
      tryCatch({
        df[[column]] <- as.POSIXct(as.character(df[[column]]), format = best_format)
        log_info(paste("Converted column", column, "to datetime using format", best_format))
        success <- TRUE
      }, error = function(e) {
        log_warning(paste("Failed final datetime conversion for", column, ":", e$message))
        df[[column]] <- original
      })
    } else {
      log_warning(paste("Could not find suitable datetime format for", column))
      df[[column]] <- original
    }
  }
  
  return(list(df = df, success = success))
}

# Function to convert column to numeric
convert_to_numeric <- function(df, column) {
  # Save original values
  original <- df[[column]]
  success <- FALSE
  
  # Try direct conversion
  tryCatch({
    df[[column]] <- as.numeric(df[[column]])
    
    # Calculate success rate
    success_rate <- 1.0 - (sum(is.na(df[[column]])) / length(df[[column]]))
    
    if (success_rate >= 0.8) {
      log_info(paste("Converted column", column, "to numeric with", 
                     round(success_rate * 100, 2), "% success rate"))
      success <- TRUE
    } else {
      # Restore original values
      df[[column]] <- original
    }
  }, error = function(e) {
    log_warning(paste("Failed to convert", column, "to numeric:", e$message))
  })
  
  # If direct conversion failed, try extracting numeric values
  if (!success) {
    tryCatch({
      # Extract numbers with regex
      numeric_values <- str_extract(as.character(df[[column]]), "[-+]?\\d*\\.?\\d+")
      df[[column]] <- as.numeric(numeric_values)
      
      # Calculate success rate
      success_rate <- 1.0 - (sum(is.na(df[[column]])) / length(df[[column]]))
      
      if (success_rate >= 0.5) {
        log_info(paste("Extracted numeric values from", column, "with", 
                       round(success_rate * 100, 2), "% success rate"))
        success <- TRUE
      } else {
        # Restore original values
        df[[column]] <- original
        log_warning(paste("Low success rate extracting numeric values from", column, 
                          ", skipping"))
      }
    }, error = function(e) {
      log_warning(paste("Failed to extract numeric values from", column, ":", e$message))
      df[[column]] <- original
    })
  }
  
  return(list(df = df, success = success))
}

# Function to load inspection report
load_inspection_report <- function(report_path) {
  tryCatch({
    report <- fromJSON(report_path)
    log_info(paste("Loaded inspection report with", 
                   length(report$file_summaries), "file summaries"))
    return(report)
  }, error = function(e) {
    log_error(paste("Failed to load inspection report:", e$message))
    return(NULL)
  })
}

# Function to load data files
load_files <- function(report, directory_path) {
  dataframes <- list()
  
  if (is.null(report)) {
    log_error("No inspection report loaded")
    return(dataframes)
  }
  
  for (filename in names(report$file_summaries)) {
    file_path <- file.path(directory_path, filename)
    summary <- report$file_summaries[[filename]]
    
    if (!file.exists(file_path)) {
      log_warning(paste("File", file_path, "does not exist, skipping"))
      next
    }
    
    if (!is.null(summary$error)) {
      log_warning(paste("Skipping file", filename, "due to error in inspection report"))
      next
    }
    
    tryCatch({
      if (!is.null(summary$sheets_data)) {
        # Excel file with multiple sheets
        for (sheet_name in summary$sheets) {
          tryCatch({
            df <- read_excel(file_path, sheet = sheet_name)
            dataframes[[paste0(filename, "|", sheet_name)]] <- df
            log_info(paste("Loaded sheet", sheet_name, "from", filename))
          }, error = function(e) {
            log_error(paste("Failed to load sheet", sheet_name, "from", filename, ":", e$message))
          })
        }
      } else if (endsWith(filename, ".csv")) {
        # CSV file
        tryCatch({
          delimiter <- detect_delimiter(file_path)
          df <- read_delim(file_path, delimiter)
          dataframes[[filename]] <- df
          log_info(paste("Loaded CSV file", filename))
        }, error = function(e) {
          log_error(paste("Failed to load CSV", filename, ":", e$message))
        })
      } else if (endsWith(filename, ".json")) {
        # JSON file
        tryCatch({
          json_data <- fromJSON(file_path)
          
          if (is.data.frame(json_data) || 
              (is.list(json_data) && all(sapply(json_data, is.list)))) {
            df <- as.data.frame(json_data)
            dataframes[[filename]] <- df
            log_info(paste("Loaded JSON file", filename))
          } else {
            log_warning(paste("JSON file", filename, "is not in tabular format, skipping"))
          }
        }, error = function(e) {
          log_error(paste("Failed to load JSON", filename, ":", e$message))
        })
      }
    }, error = function(e) {
      log_error(paste("Failed to load", filename, ":", e$message))
    })
  }
  
  log_info(paste("Loaded", length(dataframes), "dataframes from", 
                 length(report$file_summaries), "files"))
  return(dataframes)
}

# Function to generate harmonization plan
generate_harmonization_plan <- function(report) {
  if (is.null(report)) {
    log_error("No inspection report loaded")
    return(NULL)
  }
  
  plan <- list()
  
  # Process common fields that appear in multiple files
  for (field in names(report$common_fields$common_fields)) {
    occurrences <- report$common_fields$common_fields[[field]]
    
    # Get all field details from the inspection report
    field_details <- list()
    
    for (file_or_sheet in occurrences) {
      if (grepl(" \\(sheet: ", file_or_sheet)) {
        # Excel sheet
        parts <- strsplit(file_or_sheet, " \\(sheet: ")[[1]]
        filename <- parts[1]
        sheet <- sub("\\)$", "", parts[2])
        
        if (!is.null(report$file_summaries[[filename]]$sheets_data[[sheet]])) {
          sheet_data <- report$file_summaries[[filename]]$sheets_data[[sheet]]
          if (!is.null(sheet_data$column_details[[field]])) {
            field_details <- c(field_details, list(list(
              source = file_or_sheet,
              details = sheet_data$column_details[[field]]
            )))
          }
        }
      } else {
        # Single table file
        filename <- file_or_sheet
        if (!is.null(report$file_summaries[[filename]]$column_details[[field]])) {
          field_details <- c(field_details, list(list(
            source = file_or_sheet,
            details = report$file_summaries[[filename]]$column_details[[field]]
          )))
        }
      }
    }
    
    # Determine the most appropriate data type for harmonization
    types <- sapply(field_details, function(d) d$details$inferred_type)
    type_counts <- table(types)
    
    # Select the most common type, prioritizing datetime > numeric > text
    if ("datetime" %in% names(type_counts) || "potential_date" %in% names(type_counts)) {
      target_type <- "datetime"
    } else if ("numeric" %in% names(type_counts)) {
      target_type <- "numeric"
    } else {
      target_type <- "text"
    }
    
    plan[[field]] <- list(
      field_name = standardize_field_name(field),
      occurrences = occurrences,
      source_details = field_details,
      target_type = target_type,
      harmonization_actions = list()
    )
    
    # Determine actions needed for harmonization
    for (detail in field_details) {
      current_type <- detail$details$inferred_type
      source <- detail$source
      
      if (current_type == target_type) {
        plan[[field]]$harmonization_actions <- c(plan[[field]]$harmonization_actions, list(list(
          source = source,
          action = "keep",
          details = "Field already in target format"
        )))
      } else if (current_type == "potential_date" && target_type == "datetime") {
        plan[[field]]$harmonization_actions <- c(plan[[field]]$harmonization_actions, list(list(
          source = source,
          action = "convert_to_datetime",
          details = "Convert string dates to datetime objects"
        )))
      } else if (current_type == "text" && target_type == "numeric") {
        plan[[field]]$harmonization_actions <- c(plan[[field]]$harmonization_actions, list(list(
          source = source,
          action = "convert_to_numeric",
          details = "Extract numeric values from text"
        )))
      } else if (current_type == "numeric" && target_type == "text") {
        plan[[field]]$harmonization_actions <- c(plan[[field]]$harmonization_actions, list(list(
          source = source,
          action = "convert_to_text",
          details = "Convert numeric to text"
        )))
      } else {
        plan[[field]]$harmonization_actions <- c(plan[[field]]$harmonization_actions, list(list(
          source = source,
          action = "best_effort_conversion",
          details = paste("Attempt to convert from", current_type, "to", target_type)
        )))
      }
    }
  }
  
  log_info(paste("Generated harmonization plan for", length(plan), "fields"))
  return(plan)
}

# Function to harmonize dataframes based on the plan
harmonize_dataframes <- function(dataframes, harmonization_plan, output_dir = "harmonized_data") {
  if (is.null(harmonization_plan) || length(dataframes) == 0) {
    log_error("Harmonization plan or dataframes not available")
    return(NULL)
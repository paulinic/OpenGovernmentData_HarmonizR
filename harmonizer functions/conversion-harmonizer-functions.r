#' Convert a column to datetime format
#'
#' Attempts to convert a column in a dataframe to datetime format using multiple strategies.
#' First tries direct conversion with lubridate, then falls back to trying specific formats.
#'
#' @param df A dataframe containing the column to convert
#' @param column Character string with name of the column to convert
#'
#' @return A list containing:
#'   \item{df}{The dataframe with the column converted (if successful)}
#'   \item{success}{Logical indicating if conversion was successful}
#'
#' @importFrom lubridate parse_date_time
#' @export
#'
#' @examples
#' \dontrun{
#' data <- data.frame(date_col = c("2022-01-01", "2022/02/01", "01.03.2022"))
#' result <- convert_to_datetime(data, "date_col")
#' }
convert_to_datetime <- function(df, column) {
  # Save original values
  original <- df[[column]]
  success <- FALSE
  
  # Try direct conversion with lubridate
  tryCatch({
    # First try automatic parsing
    df[[column]] <- lubridate::parse_date_time(df[[column]], orders = c(
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

#' Convert a column to numeric format
#'
#' Attempts to convert a column in a dataframe to numeric format, first trying direct
#' conversion and then falling back to extracting numeric values with regex.
#'
#' @param df A dataframe containing the column to convert
#' @param column Character string with name of the column to convert
#'
#' @return A list containing:
#'   \item{df}{The dataframe with the column converted (if successful)}
#'   \item{success}{Logical indicating if conversion was successful}
#'
#' @export
#'
#' @examples
#' \dontrun{
#' data <- data.frame(value_col = c("123", "456.7", "$89.99"))
#' result <- convert_to_numeric(data, "value_col")
#' }
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
      numeric_values <- stringr::str_extract(as.character(df[[column]]), "[-+]?\\d*\\.?\\d+")
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

#' Detect the delimiter used in a CSV file
#'
#' Examines the first few lines of a file to determine the most likely delimiter.
#'
#' @param file_path Character string with path to the CSV file
#'
#' @return Character with the detected delimiter
#'
#' @export
#'
#' @examples
#' \dontrun{
#' delimiter <- detect_delimiter("data.csv")  # might return "," or ";"
#' }
detect_delimiter <- function(file_path) {
  # Read first few lines
  conn <- file(file_path, "r")
  first_lines <- readLines(conn, n = 5)
  close(conn)
  
  # Count potential delimiters
  delimiters <- c(",", ";", "\t", "|")
  counts <- sapply(delimiters, function(d) {
    sum(stringr::str_count(first_lines, stringr::fixed(d)))
  })
  
  # Return most common delimiter
  return(delimiters[which.max(counts)])
}

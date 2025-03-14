#' Log an informational message
#'
#' Writes an informational message to the console and appends to a log file.
#'
#' @param message Character string with the message to log
#' @param log_file Character string with path to the log file. Default is "data_harmonization.log".
#'
#' @return Invisibly returns the message that was logged
#'
#' @export
log_info <- function(message, log_file = "data_harmonization.log") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  formatted_msg <- paste(timestamp, "- INFO -", message)
  
  # Print to console
  cat(formatted_msg, "\n")
  
  # Append to log file
  write(formatted_msg, file = log_file, append = TRUE)
  
  invisible(message)
}

#' Log a warning message
#'
#' Writes a warning message to the console and appends to a log file.
#'
#' @param message Character string with the message to log
#' @param log_file Character string with path to the log file. Default is "data_harmonization.log".
#'
#' @return Invisibly returns the message that was logged
#'
#' @export
log_warning <- function(message, log_file = "data_harmonization.log") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  formatted_msg <- paste(timestamp, "- WARNING -", message)
  
  # Print to console
  cat(formatted_msg, "\n")
  
  # Append to log file
  write(formatted_msg, file = log_file, append = TRUE)
  
  invisible(message)
}

#' Log an error message
#'
#' Writes an error message to the console and appends to a log file.
#'
#' @param message Character string with the message to log
#' @param log_file Character string with path to the log file. Default is "data_harmonization.log".
#'
#' @return Invisibly returns the message that was logged
#'
#' @export
log_error <- function(message, log_file = "data_harmonization.log") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  formatted_msg <- paste(timestamp, "- ERROR -", message)
  
  # Print to console
  cat(formatted_msg, "\n")
  
  # Append to log file
  write(formatted_msg, file = log_file, append = TRUE)
  
  invisible(message)
}

#' Standardize a field name
#'
#' Converts a field name to a standardized format:
#' - Converts to lowercase
#' - Replaces spaces and special characters with underscores
#' - Converts CamelCase to snake_case
#'
#' @param field_name Character string with the field name to standardize
#'
#' @return Character string with the standardized field name
#'
#' @export
#'
#' @examples
#' standardize_field_name("First Name")  # returns "first_name"
#' standardize_field_name("CustomerID")  # returns "customer_id"
standardize_field_name <- function(field_name) {
  # Convert to lowercase
  standardized <- tolower(field_name)
  
  # Replace spaces and special characters with underscores
  standardized <- stringr::str_replace_all(standardized, "[^a-z0-9]+", "_")
  
  # Convert CamelCase to snake_case
  standardized <- stringr::str_replace_all(standardized, "([a-z0-9])([A-Z])", "\\1_\\2")
  standardized <- tolower(standardized)
  
  # Remove consecutive underscores
  standardized <- stringr::str_replace_all(standardized, "_+", "_")
  
  # Remove leading and trailing underscores
  standardized <- stringr::str_trim(standardized, "both")
  standardized <- stringr::str_replace_all(standardized, "^_+|_+$", "")
  
  return(standardized)
}

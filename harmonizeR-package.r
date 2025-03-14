#' @title harmonizeR: Data Harmonization Tools for Swiss Open Data
#' @description Tools for harmonizing, standardizing, and linking datasets from various 
#' sources, with special support for Swiss open data formats and geospatial services.
#' @docType package
#' @name harmonizeR
NULL

#==============================================================================
# 1. CORE UTILITY FUNCTIONS
#==============================================================================

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

#==============================================================================
# 2. INSPECTION PHASE
#==============================================================================

#-------------------------------------------------------------------------------
# 2.1 File Detection
#-------------------------------------------------------------------------------

#' Scan a directory for supported data files
#'
#' This function scans a directory and identifies supported data files for analysis
#'
#' @param directory_path Path to the directory containing data files
#' @return A data frame with information about the discovered files
#' @export
#' @examples
#' \dontrun{
#' file_info <- scan_directory("path/to/data")
#' }
scan_directory <- function(directory_path) {
  # List all files in the directory
  files <- list.files(directory_path, full.names = TRUE)
  
  # Filter by supported extensions
  supported_extensions <- c("csv", "xlsx", "xls", "json")
  data_files <- files[tools::file_ext(files) %in% supported_extensions]
  
  # Get file info
  file_info <- data.frame(
    path = data_files,
    filename = basename(data_files),
    extension = tools::file_ext(data_files),
    size_bytes = file.size(data_files),
    stringsAsFactors = FALSE
  )
  
  return(file_info)
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

#' Check if a column might contain dates
#'
#' This function samples values from a column and checks if they appear to be dates
#'
#' @param values Vector of values to check
#' @return Logical indicating if the column might contain dates
#' @keywords internal
might_be_date <- function(values) {
  # Sample some non-NA values
  sample_values <- na.omit(values)[1:min(10, length(na.omit(values)))]
  
  if (length(sample_values) == 0) {
    return(FALSE)
  }
  
  # Try to parse with various date formats
  success <- sapply(sample_values, function(x) {
    if (!is.character(x)) {
      x <- as.character(x)
    }
    
    # Try different date formats
    formats <- c(
      "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y",
      "%Y.%m.%d", "%d.%m.%Y", "%Y%m%d"
    )
    
    for (fmt in formats) {
      result <- tryCatch({
        !is.na(as.Date(x, format = fmt))
      }, error = function(e) {
        FALSE
      })
      
      if (result) {
        return(TRUE)
      }
    }
    
    return(FALSE)
  })
  
  # If most values appear to be dates, return TRUE
  return(mean(success) > 0.7)
}

#-------------------------------------------------------------------------------
# 2.2 File Analysis
#-------------------------------------------------------------------------------

#' Analyze a data frame's structure
#'
#' This function examines a data frame and provides detailed analysis of its structure
#'
#' @param df Data frame to analyze
#' @param file_info File information
#' @param sheet_name Optional sheet name for Excel files
#' @return A list with analysis results
#' @keywords internal
analyze_dataframe <- function(df, file_info, sheet_name = NULL) {
  # Get column information
  column_info <- lapply(names(df), function(col) {
    values <- df[[col]]
    null_count <- sum(is.na(values))
    unique_count <- length(unique(values))
    
    # Determine data type
    if (is.numeric(values)) {
      inferred_type <- "numeric"
    } else if (inherits(values, "Date") || inherits(values, "POSIXct")) {
      inferred_type <- "datetime"
    } else if (is.character(values) && might_be_date(values)) {
      inferred_type <- "potential_date"
    } else {
      inferred_type <- "text"
    }
    
    # Get sample values (up to 5)
    sample_values <- values[!is.na(values)][1:min(5, sum(!is.na(values)))]
    
    return(list(
      column = col,
      null_count = null_count,
      null_percentage = round(100 * null_count / length(values), 2),
      unique_values = unique_count,
      dtype = class(values)[1],
      inferred_type = inferred_type,
      sample_values = as.character(sample_values)
    ))
  })
  
  names(column_info) <- names(df)
  
  # Prepare result
  result <- list(
    filename = file_info$filename,
    sheet_name = sheet_name,
    row_count = nrow(df),
    column_count = ncol(df),
    columns = names(df),
    column_details = column_info,
    duplicate_rows = sum(duplicated(df))
  )
  
  return(result)
}

#' Analyze a CSV file
#'
#' This function analyzes the structure and content of a CSV file
#'
#' @param file_info File information from scan_directory
#' @return A list with analysis results
#' @importFrom readr read_delim cols col_character locale
#' @export
analyze_csv <- function(file_info) {
  tryCatch({
    # Detect delimiter
    delimiter <- detect_delimiter(file_info$path)
    
    # Read the CSV
    df <- readr::read_delim(file_info$path, delimiter, 
                            col_types = readr::cols(.default = readr::col_character()),
                            locale = readr::locale(encoding = "UTF-8"))
    
    return(analyze_dataframe(df, file_info))
    
  }, error = function(e) {
    # Return error information
    return(list(
      error = paste("Failed to parse CSV:", e$message),
      filename = file_info$filename
    ))
  })
}

#' Analyze an Excel file
#'
#' This function analyzes the structure and content of an Excel file
#'
#' @param file_info File information from scan_directory
#' @return A list with analysis results
#' @importFrom readxl excel_sheets read_excel
#' @export
analyze_excel <- function(file_info) {
  tryCatch({
    # Get sheet names
    sheets <- readxl::excel_sheets(file_info$path)
    
    # Analyze each sheet
    sheets_data <- lapply(sheets, function(sheet) {
      df <- readxl::read_excel(file_info$path, sheet = sheet)
      return(analyze_dataframe(df, file_info, sheet_name = sheet))
    })
    
    names(sheets_data) <- sheets
    
    return(list(
      filename = file_info$filename,
      sheets = sheets,
      sheet_count = length(sheets),
      sheets_data = sheets_data
    ))
    
  }, error = function(e) {
    # Return error information
    return(list(
      error = paste("Failed to parse Excel file:", e$message),
      filename = file_info$filename
    ))
  })
}

#' Analyze a JSON file
#'
#' This function analyzes the structure and content of a JSON file
#'
#' @param file_info File information from scan_directory
#' @return A list with analysis results
#' @importFrom jsonlite read_json
#' @export
analyze_json <- function(file_info) {
  tryCatch({
    # Read the JSON
    json_data <- jsonlite::read_json(file_info$path)
    
    # Check if it's a list of objects (can be converted to data frame)
    if (is.data.frame(json_data) || (is.list(json_data) && all(sapply(json_data, is.list)))) {
      # Convert to data frame
      df <- as.data.frame(json_data, stringsAsFactors = FALSE)
      return(analyze_dataframe(df, file_info))
    } else {
      # Just return basic structure info
      return(list(
        filename = file_info$filename,
        structure = "Non-tabular JSON",
        is_tabular = FALSE
      ))
    }
    
  }, error = function(e) {
    # Return error information
    return(list(
      error = paste("Failed to parse JSON:", e$message),
      filename = file_info$filename
    ))
  })
}

#' Analyze all files
#'
#' This function analyzes all files in the provided file information
#'
#' @param file_info File information from scan_directory
#' @return A list with analysis results for all files
#' @export
analyze_all_files <- function(file_info) {
  summaries <- list()
  
  for (i in 1:nrow(file_info)) {
    file <- file_info[i, ]
    
    if (file$extension == "csv") {
      summary <- analyze_csv(file)
    } else if (file$extension %in% c("xlsx", "xls")) {
      summary <- analyze_excel(file)
    } else if (file$extension == "json") {
      summary <- analyze_json(file)
    } else {
      summary <- list(
        filename = file$filename,
        error = paste("File type", file$extension, "analysis not implemented")
      )
    }
    
    summaries[[file$filename]] <- summary
  }
  
  return(summaries)
}

#-------------------------------------------------------------------------------
# 2.3 Cross-file Analysis
#-------------------------------------------------------------------------------

#' Find common fields across files
#'
#' This function identifies fields that appear in multiple files
#'
#' @param file_summaries Analysis results from analyze_all_files
#' @return A list with common field information
#' @export
find_common_fields <- function(file_summaries) {
  all_fields <- list()
  
  for (filename in names(file_summaries)) {
    summary <- file_summaries[[filename]]
    
    if (!is.null(summary$error)) {
      next
    }
    
    if (!is.null(summary$sheets_data)) {
      # Excel file with multiple sheets
      for (sheet in names(summary$sheets_data)) {
        sheet_data <- summary$sheets_data[[sheet]]
        
        if (!is.null(sheet_data$columns)) {
          for (column in sheet_data$columns) {
            if (is.null(all_fields[[column]])) {
              all_fields[[column]] <- character(0)
            }
            all_fields[[column]] <- c(all_fields[[column]], 
                                      paste0(filename, " (sheet: ", sheet, ")"))
          }
        }
      }
    } else if (!is.null(summary$columns)) {
      # Single table file
      for (column in summary$columns) {
        if (is.null(all_fields[[column]])) {
          all_fields[[column]] <- character(0)
        }
        all_fields[[column]] <- c(all_fields[[column]], filename)
      }
    }
  }
  
  # Filter to fields that appear in multiple files
  common_fields <- all_fields[sapply(all_fields, length) > 1]
  
  return(list(
    all_fields = all_fields,
    common_fields = common_fields,
    common_field_count = length(common_fields)
  ))
}

#' Detect inconsistencies across files
#'
#' This function identifies inconsistencies in field types across files
#'
#' @param file_summaries Analysis results from analyze_all_files
#' @return A list with inconsistency information
#' @export
detect_inconsistencies <- function(file_summaries) {
  field_types <- list()
  
  # First collect all field types
  for (filename in names(file_summaries)) {
    summary <- file_summaries[[filename]]
    
    if (!is.null(summary$error)) {
      next
    }
    
    if (!is.null(summary$sheets_data)) {
      # Excel file with multiple sheets
      for (sheet in names(summary$sheets_data)) {
        sheet_data <- summary$sheets_data[[sheet]]
        
        if (!is.null(sheet_data$column_details)) {
          for (column in names(sheet_data$column_details)) {
            details <- sheet_data$column_details[[column]]
            
            if (is.null(field_types[[column]])) {
              field_types[[column]] <- list()
            }
            
            field_types[[column]] <- c(field_types[[column]], list(list(
              file = filename,
              sheet = sheet,
              type = details$inferred_type,
              sample = details$sample_values[1:min(2, length(details$sample_values))]
            )))
          }
        }
      }
    } else if (!is.null(summary$column_details)) {
      # Single table file
      for (column in names(summary$column_details)) {
        details <- summary$column_details[[column]]
        
        if (is.null(field_types[[column]])) {
          field_types[[column]] <- list()
        }
        
        field_types[[column]] <- c(field_types[[column]], list(list(
          file = filename,
          type = details$inferred_type,
          sample = details$sample_values[1:min(2, length(details$sample_values))]
        )))
      }
    }
  }
  
  # Now check for inconsistencies
  inconsistencies <- list()
  
  for (field in names(field_types)) {
    occurrences <- field_types[[field]]
    
    if (length(occurrences) > 1) {
      types <- sapply(occurrences, function(o) o$type)
      
      if (length(unique(types)) > 1) {
        inconsistencies <- c(inconsistencies, list(list(
          field = field,
          issue = "inconsistent_types",
          details = occurrences
        )))
      }
    }
  }
  
  return(list(
    inconsistencies = inconsistencies,
    inconsistency_count = length(inconsistencies)
  ))
}

#-------------------------------------------------------------------------------
# 2.4 Reporting
#-------------------------------------------------------------------------------

#' Generate a comprehensive report
#'
#' This function generates a comprehensive report of the data analysis
#'
#' @param file_info File information from scan_directory
#' @param file_summaries Analysis results from analyze_all_files
#' @return A list with the comprehensive report
#' @export
generate_report <- function(file_info, file_summaries) {
  common_fields <- find_common_fields(file_summaries)
  inconsistencies <- detect_inconsistencies(file_summaries)
  
  return(list(
    files_analyzed = nrow(file_info),
    file_summaries = file_summaries,
    common_fields = common_fields,
    inconsistencies = inconsistencies,
    harmonization_candidates = common_fields$common_fields
  ))
}

#' Save report to JSON file
#'
#' This function saves the analysis report to a JSON file
#'
#' @param report Report from generate_report
#' @param file_path Path where to save the JSON file
#' @return Invisibly returns the path to the saved file
#' @importFrom jsonlite toJSON write_json
#' @export
save_report <- function(report, file_path = "data_inspection_report.json") {
  jsonlite::write_json(report, file_path, auto_unbox = TRUE, pretty = TRUE)
  message(paste("Report saved to", file_path))
  return(invisible(file_path))
}

#' Load inspection report from JSON file
#'
#' @param report_path Character string with path to the JSON file
#'
#' @return A list with the loaded report
#'
#' @importFrom jsonlite read_json
#' @export
load_inspection_report <- function(report_path) {
  tryCatch({
    report <- jsonlite::read_json(report_path, simplifyVector = TRUE)
    log_info(paste("Loaded inspection report from", report_path))
    return(report)
  }, error = function(e) {
    log_error(paste("Failed to load inspection report:", e$message))
    return(NULL)
  })
}

#-------------------------------------------------------------------------------
# 2.5 Inspection Workflow
#-------------------------------------------------------------------------------

#' Inspect data in a directory
#'
#' This function runs a complete data inspection workflow on files in a directory
#'
#' @param directory_path Path to the directory containing data files
#' @param save_to_file Whether to save the report to a JSON file
#' @param output_file Path where to save the JSON file if save_to_file is TRUE
#' @return A list with the comprehensive inspection report
#' @export
#' @examples
#' \dontrun{
#' report <- inspect_data("path/to/data")
#' }
inspect_data <- function(directory_path, save_to_file = TRUE, 
                         output_file = "data_inspection_report.json") {
  message("Scanning directory for data files...")
  file_info <- scan_directory(directory_path)
  message(paste("Found", nrow(file_info), "files to analyze"))
  
  message("Analyzing files...")
  file_summaries <- analyze_all_files(file_info)
  
  message("Generating report...")
  report <- generate_report(file_info, file_summaries)
  
  message(paste("Analysis complete. Found", report$inconsistencies$inconsistency_count, 
                "potential inconsistencies"))
  message(paste("Found", report$common_fields$common_field_count, 
                "fields that appear in multiple files"))
  
  # Save the report if requested
  if (save_to_file) {
    save_report(report, output_file)
  }
  
  return(report)
}

#==============================================================================
# 3. HARMONIZATION PHASE
#==============================================================================

#-------------------------------------------------------------------------------
# 3.1 Data Loading
#-------------------------------------------------------------------------------

#' Load files based on inspection report
#'
#' Loads data files according to the information in an inspection report
#'
#' @param report A list containing the inspection report data
#' @param directory_path Character string with path to the directory containing data files
#'
#' @return A list of dataframes
#'
#' @importFrom readr read_delim
#' @importFrom readxl read_excel
#' @importFrom jsonlite read_json
#' @export
load_files <- function(report, directory_path) {
  dataframes <- list()
  
  # Get file summaries from the report
  file_summaries <- report$file_summaries
  
  for (filename in names(file_summaries)) {
    summary <- file_summaries[[filename]]
    file_path <- file.path(directory_path, filename)
    
    # Skip files that had errors during inspection
    if (!is.null(summary$error)) {
      log_warning(paste("Skipping", filename, "due to previous error:", summary$error))
      next
    }
    
    # Load based on file type
    if (!is.null(summary$sheets_data)) {
      # Excel file with multiple sheets
      for (sheet in names(summary$sheets_data)) {
        log_info(paste("Loading Excel sheet", sheet, "from", filename))
        tryCatch({
          df <- readxl::read_excel(file_path, sheet = sheet)
          dataframes[[paste0(filename, "_", sheet)]] <- df
        }, error = function(e) {
          log_error(paste("Failed to load sheet", sheet, "from", filename, ":", e$message))
        })
      }
    } else {
      # CSV or other single-table format
      log_info(paste("Loading", filename))
      tryCatch({
        if (tools::file_ext(filename) == "csv") {
          delimiter <- detect_delimiter(file_path)
          df <- readr::read_delim(file_path, delimiter, guess_max = 10000)
          dataframes[[filename]] <- df
        } else if (tools::file_ext(filename) %in% c("xlsx", "xls")) {
          df <- readxl::read_excel(file_path)
          dataframes[[filename]] <- df
        } else if (tools::file_ext(filename) == "json") {
          json_data <- jsonlite::read_json(file_path, simplifyVector = TRUE)
          if (is.data.frame(json_data) || (is.list(json_data) && all(sapply(json_data, is.list)))) {
            df <- as.data.frame(json_data, stringsAsFactors = FALSE)
            dataframes[[filename]] <- df
          } else {
            log_warning(paste("JSON file", filename, "does not contain tabular data"))
          }
        }
      }, error = function(e) {
        log_error(paste("Failed to load", filename, ":", e$message))
      })
    }
  }
  
  return(dataframes)
}

#-------------------------------------------------------------------------------
# 3.2 Type Conversion
#-------------------------------------------------------------------------------

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

#-------------------------------------------------------------------------------
# 3.3 Harmonization Plan Generation
#-------------------------------------------------------------------------------

#' Generate a harmonization plan
#'
#' Creates a plan for harmonizing multiple dataframes based on inspection results
#'
#' @param report Inspection report from inspect_data
#'
#' @return A list with the harmonization plan
#'
#' @export
generate_harmonization_plan <- function(report) {
  plan <- list()
  
  # Extract common fields that could be harmonized
  common_fields <- report$common_fields$common_fields
  
  # Analyze data types for each common field
  for (field in names(common_fields)) {
    field_info <- list(
      field_name = field,
      standardized_name = standardize_field_name(field),
      sources = common_fields[[field]],
      data_types = list()
    )
    
    # Collect data types across files
    for (source in common_fields[[field]]) {
      # Extract filename (and sheet if applicable)
      if (grepl(" \\(sheet: ", source)) {
        filename <- sub(" \\(sheet: .*\\)$", "", source)
        sheet <- sub("^.* \\(sheet: (.*)\\)$", "\\1", source)
        
        # Get data type from the report
        if (!is.null(report$file_summaries[[filename]]$sheets_data[[sheet]]$column_details[[field]])) {
          data_type <- report$file_summaries[[filename]]$sheets_data[[sheet]]$column_details[[field]]$inferred_type
          field_info$data_types[[source]] <- data_type
        }
      } else {
        # Single table file
        if (!is.null(report$file_summaries[[source]]$column_details[[field]])) {
          data_type <- report$file_summaries[[source]]$column_details[[field]]$inferred_type
          field_info$data_types[[source]] <- data_type
        }
      }
    }
    
    # Determine target data type
    type_counts <- table(unlist(field_info$data_types))
    if (length(type_counts) > 0) {
      field_info$target_type <- names(type_counts)[which.max(type_counts)]
    } else {
      field_info$target_type <- "text"
    }
    
    plan[[field]] <- field_info
  }
  
  return(list(
    fields = plan,
    standardize_names = TRUE
  ))
}

#' Enhance harmonization plan with key variable information
#'
#' Adds information about potential key variables to the harmonization plan
#'
#' @param harmonization_plan Harmonization plan from generate_harmonization_plan
#' @param key_variables List of identified key variables
#'
#' @return Enhanced harmonization plan
#'
#' @export
enhance_harmonization_plan <- function(harmonization_plan, key_variables) {
  # Add key variable information to the plan
  for (field in names(harmonization_plan$fields)) {
    if (field %in% names(key_variables)) {
      harmonization_plan$fields[[field]]$is_key <- TRUE
      harmonization_plan$fields[[field]]$key_info <- key_variables[[field]]
    } else {
      harmonization_plan$fields[[field]]$is_key <- FALSE
    }
  }
  
  return(harmonization_plan)
}

#' Harmonize dataframes
#'
#' Applies a harmonization plan to standardize dataframes
#'
#' @param dataframes List of dataframes to harmonize
#' @param harmonization_plan Plan generated by generate_harmonization_plan
#' @param output_dir Directory where to save harmonized files
#'
#' @return List of harmonized dataframes
#'
#' @export
harmonize_dataframes <- function(dataframes, harmonization_plan, output_dir) {
  harmonized <- list()
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Process each dataframe
  for (df_name in names(dataframes)) {
    df <- dataframes[[df_name]]
    log_info(paste("Harmonizing dataframe:", df_name))
    
    # Rename columns if standardize_names is TRUE
    if (harmonization_plan$standardize_names) {
      old_names <- names(df)
      new_names <- old_names
      
      for (i in seq_along(old_names)) {
        field <- old_names[i]
        
        if (field %in% names(harmonization_plan$fields)) {
          new_names[i] <- harmonization_plan$fields[[field]]$standardized_name
        } else {
          new_names[i] <- standardize_field_name(field)
        }
      }
      
      names(df) <- new_names
      log_info(paste("Standardized column names for", df_name))
    }
    
    # Apply data type conversions based on the plan
    for (field in names(harmonization_plan$fields)) {
      field_info <- harmonization_plan$fields[[field]]
      standardized_name <- field_info$standardized_name
      
      # Skip if field is not in this dataframe
      if (!(standardized_name %in% names(df))) {
        next
      }
      
      # Apply conversion based on target type
      if (field_info$target_type == "datetime") {
        result <- convert_to_datetime(df, standardized_name)
        df <- result$df
        if (result$success) {
          log_info(paste("Converted column", standardized_name, "to datetime in", df_name))
        }
      } else if (field_info$target_type == "numeric") {
        result <- convert_to_numeric(df, standardized_name)
        df <- result$df
        if (result$success) {
          log_info(paste("Converted column", standardized_name, "to numeric in", df_name))
        }
      } else if (field_info$target_type == "potential_date") {
        # For potential_date, try datetime conversion
        result <- convert_to_datetime(df, standardized_name)
        df <- result$df
        if (result$success) {
          log_info(paste("Converted potential date column", standardized_name, "to datetime in", df_name))
        }
      }
    }
    
    # Save harmonized data
    harmonized[[df_name]] <- df
    
    # Write to CSV in output directory
    output_filename <- file.path(output_dir, paste0("harmonized_", gsub("\\..*$", "", df_name), ".csv"))
    write.csv(df, output_filename, row.names = FALSE)
    log_info(paste("Saved harmonized data to", output_filename))
  }
  
  return(harmonized)
}

#==============================================================================
# 4. LINKING PHASE
#==============================================================================

#' Identify potential key variables for linking datasets
#'
#' Analyzes dataframes to identify variables that could be used for linking
#'
#' @param dataframes List of dataframes
#' @param report Inspection report
#'
#' @return List of potential key variables with their properties
#'
#' @export
identify_key_variables <- function(dataframes, report) {
  key_candidates <- list()
  
  # Get common fields from the report
  common_fields <- report$common_fields$common_fields
  
  # Analyze each common field to determine if it's a candidate for linking
  for (field in names(common_fields)) {
    # Check how many unique values the field has in each dataframe
    uniqueness_ratio <- list()
    value_overlap <- list()
    
    for (df_name in names(dataframes)) {
      df <- dataframes[[df_name]]
      
      # Skip if field is not in this dataframe
      if (!(field %in% names(df))) {
        next
      }
      
      # Get values and calculate uniqueness ratio
      values <- df[[field]]
      n_values <- length(values)
      n_unique <- length(unique(na.omit(values)))
      
      if (n_values > 0) {
        uniqueness <- n_unique / n_values
        uniqueness_ratio[[df_name]] <- uniqueness
        
        # Store non-NA values for overlap calculation
        value_overlap[[df_name]] <- unique(na.omit(values))
      }
    }
    
    # Calculate average uniqueness ratio
    avg_uniqueness <- mean(unlist(uniqueness_ratio))
    
    # Calculate overlap between datasets
    overlap_scores <- list()
    df_names <- names(value_overlap)
    
    if (length(df_names) >= 2) {
      for (i in 1:(length(df_names)-1)) {
        for (j in (i+1):length(df_names)) {
          df1 <- df_names[i]
          df2 <- df_names[j]
          
          values1 <- value_overlap[[df1]]
          values2 <- value_overlap[[df2]]
          
          if (length(values1) > 0 && length(values2) > 0) {
            # Calculate Jaccard similarity (intersection / union)
            intersection <- length(intersect(values1, values2))
            union <- length(union(values1, values2))
            
            if (union > 0) {
              similarity <- intersection / union
              overlap_scores[[paste(df1, df2, sep = "_vs_")]] <- similarity
            }
          }
        }
      }
    }
    
    avg_overlap <- if (length(overlap_scores) > 0) mean(unlist(overlap_scores)) else 0
    
    # If field has good uniqueness or overlap, consider it a key candidate
    if (avg_uniqueness > 0.7 || avg_overlap > 0.5) {
      key_candidates[[field]] <- list(
        uniqueness_ratio = uniqueness_ratio,
        average_uniqueness = avg_uniqueness,
        overlap_scores = overlap_scores,
        average_overlap = avg_overlap,
        score = (avg_uniqueness * 0.5) + (avg_overlap * 0.5)
      )
    }
  }
  
  # Sort key candidates by score
  if (length(key_candidates) > 0) {
    scores <- sapply(key_candidates, function(k) k$score)
    key_candidates <- key_candidates[order(scores, decreasing = TRUE)]
  }
  
  return(key_candidates)
}

#' Link datasets
#'
#' Links multiple dataframes based on key variables
#'
#' @param dataframes List of harmonized dataframes
#' @param key_variables List of key variables from identify_key_variables
#' @param output_dir Directory where to save linked files
#'
#' @return A list containing:
#'   \item{linked_data}{The linked dataframe}
#'   \item{link_report}{A report on the linking process}
#'
#' @export
link_datasets <- function(dataframes, key_variables, output_dir) {
  if (length(dataframes) < 2) {
    log_warning("At least two dataframes are needed for linking")
    return(list(linked_data = NULL, link_report = list(status = "failed", reason = "insufficient_dataframes")))
  }
  
  if (length(key_variables) == 0) {
    log_warning("No key variables identified for linking")
    return(list(linked_data = NULL, link_report = list(status = "failed", reason = "no_key_variables")))
  }
  
  # Get the top key variable
  top_key <- names(key_variables)[1]
  log_info(paste("Linking datasets using key variable:", top_key))
  
  # Filter dataframes that have the key variable
  dfs_with_key <- list()
  for (df_name in names(dataframes)) {
    df <- dataframes[[df_name]]
    if (top_key %in% names(df)) {
      dfs_with_key[[df_name]] <- df
    }
  }
  
  if (length(dfs_with_key) < 2) {
    log_warning(paste("Not enough dataframes have the key variable:", top_key))
    return(list(linked_data = NULL, link_report = list(status = "failed", reason = "insufficient_key_coverage")))
  }
  
  # Start with the first dataframe
  df_names <- names(dfs_with_key)
  result <- dfs_with_key[[df_names[1]]]
  
  # Track linking statistics
  link_stats <- list()
  
  # Merge with remaining dataframes
  for (i in 2:length(df_names)) {
    current_df <- dfs_with_key[[df_names[i]]]
    original_rows <- nrow(result)
    
    # Add suffix to avoid column name conflicts
    names(current_df) <- ifelse(
      names(current_df) %in% names(result) & names(current_df) != top_key,
      paste0(names(current_df), "_", df_names[i]),
      names(current_df)
    )
    
    # Merge dataframes
    result <- merge(result, current_df, by = top_key, all = TRUE)
    
    # Calculate statistics
    merged_rows <- nrow(result)
    matched_keys <- sum(!is.na(result[[top_key]]))
    
    link_stats[[paste0(df_names[1], "_to_", df_names[i])]] <- list(
      original_rows = original_rows,
      merged_rows = merged_rows,
      matched_keys = matched_keys
    )
  }
  
  # Save linked data
  output_filename <- file.path(output_dir, "linked_data.csv")
  write.csv(result, output_filename, row.names = FALSE)
  log_info(paste("Saved linked data to", output_filename))
  
  return(list(
    linked_data = result,
    link_report = list(
      status = "success",
      key_variable = top_key,
      dataframes_linked = df_names,
      statistics = link_stats
    )
  ))
}

#==============================================================================
# 5. GEOSPATIAL FUNCTIONS
#==============================================================================

#' Create a WMS or WFS service description file
#'
#' Creates a simple text file that describes a Web Map Service (WMS) or
#' Web Feature Service (WFS) for later use.
#'
#' @param output_dir Character string with path to save the service description file
#' @param service_type Character string with the service type, either "wms" or "wfs"
#' @param service_url Character string with the URL of the service
#' @param layer_or_feature Character string with the layer name (for WMS) or feature type (for WFS)
#'
#' @return Logical indicating success or failure
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Create a WMS service description file
#' create_service_description(
#'   output_dir = "data",
#'   service_type = "wms",
#'   service_url = "https://wms.geo.admin.ch/",
#'   layer_or_feature = "ch.swisstopo.pixelkarte-farbe"
#' )
#' }
create_service_description <- function(output_dir, service_type, service_url, layer_or_feature) {
  if (!(service_type %in% c("wms", "wfs"))) {
    log_error("Service type must be 'wms' or 'wfs'")
    return(FALSE)
  }
  
  # Create filename
  service_name <- tolower(gsub("[^a-zA-Z0-9]", "_", layer_or_feature))
  filename <- file.path(output_dir, paste0(service_name, ".", service_type))
  
  # Create content
  if (service_type == "wms") {
    content <- paste0("URL=", service_url, "\n", "LAYER=", layer_or_feature)
  } else {
    content <- paste0("URL=", service_url, "\n", "FEATURETYPE=", layer_or_feature)
  }
  
  # Write to file
  tryCatch({
    write(content, filename)
    log_info(paste("Created", service_type, "service description file:", filename))
    return(TRUE)
  }, error = function(e) {
    log_error(paste("Failed to create service description file:", e$message))
    return(FALSE)
  })
}

#' Parse a WMS GetCapabilities response
#'
#' Extracts layer information from a WMS GetCapabilities XML response.
#'
#' @param capabilities_xml Character string with the XML response from a WMS GetCapabilities request
#'
#' @return A list of layer information, each containing name, title, abstract, and bounding box
#'
#' @importFrom xml2 as_xml_document xml_find_all xml_find_first xml_text xml_attr
#' @export
parse_wms_capabilities <- function(capabilities_xml) {
  # Parse the XML
  xml_doc <- xml2::as_xml_document(capabilities_xml)
  
  # Extract layers information
  layers <- xml2::xml_find_all(xml_doc, "//Layer[Name]")
  
  layer_info <- lapply(layers, function(layer) {
    name <- xml2::xml_text(xml2::xml_find_first(layer, "./Name"))
    title <- xml2::xml_text(xml2::xml_find_first(layer, "./Title"))
    abstract <- xml2::xml_text(xml2::xml_find_first(layer, "./Abstract"))
    
    # Extract bounding box if available
    bbox <- NULL
    bbox_node <- xml2::xml_find_first(layer, "./BoundingBox[@CRS]")
    if (!is.na(bbox_node)) {
      bbox <- list(
        crs = xml2::xml_attr(bbox_node, "CRS"),
        minx = as.numeric(xml2::xml_attr(bbox_node, "minx")),
        miny = as.numeric(xml2::xml_attr(bbox_node, "miny")),
        maxx = as.numeric(xml2::xml_attr(bbox_node, "maxx")),
        maxy = as.numeric(xml2::xml_attr(bbox_node, "maxy"))
      )
    }
    
    return(list(
      name = name,
      title = title,
      abstract = abstract,
      bbox = bbox
    ))
  })
  
  return(layer_info)
}

#' Load data from a Web Map Service (WMS)
#'
#' Connects to a WMS service and retrieves metadata for a specified layer.
#'
#' @param wms_url Character string with the URL of the WMS service
#' @param layer_name Character string with the name of the layer to load
#'
#' @return A dataframe with layer metadata or NULL if loading fails
#'
#' @importFrom httr GET content status_code
#' @export
#'
#' @examples
#' \dontrun{
#' wms_data <- load_wms(
#'   "https://wms.geo.admin.ch/",
#'   "ch.swisstopo.pixelkarte-farbe"
#' )
#' }
load_wms <- function(wms_url, layer_name) {
  log_info(paste("Loading WMS layer:", layer_name, "from URL:", wms_url))
  
  # Get capabilities to check if the layer exists
  capabilities_url <- paste0(wms_url, 
                             ifelse(grepl("\\?", wms_url), "&", "?"),
                             "SERVICE=WMS&REQUEST=GetCapabilities")
  
  tryCatch({
    response <- httr::GET(capabilities_url)
    if (httr::status_code(response) != 200) {
      log_error(paste("Failed to get WMS capabilities: HTTP", httr::status_code(response)))
      return(NULL)
    }
    
    # Parse capabilities
    capabilities_xml <- httr::content(response, "text")
    layer_info <- parse_wms_capabilities(capabilities_xml)
    
    # Check if the requested layer exists
    layer_names <- sapply(layer_info, function(l) l$name)
    if (!(layer_name %in% layer_names)) {
      log_warning(paste("Layer", layer_name, "not found in WMS service"))
      return(NULL)
    }
    
    # Get the layer information
    layer <- layer_info[[which(layer_names == layer_name)]]
    
    # Create a dataframe with layer metadata
    metadata_df <- data.frame(
      layer_name = layer$name,
      title = layer$title,
      abstract = layer$abstract,
      wms_url = wms_url,
      stringsAsFactors = FALSE
    )
    
    # Add bounding box information if available
    if (!is.null(layer$bbox)) {
      metadata_df$crs <- layer$bbox$crs
      metadata_df$minx <- layer$bbox$minx
      metadata_df$miny <- layer$bbox$miny
      metadata_df$maxx <- layer$bbox$maxx
      metadata_df$maxy <- layer$bbox$maxy
    }
    
    log_info(paste("Successfully loaded metadata for WMS layer:", layer_name))
    return(metadata_df)
    
  }, error = function(e) {
    log_error(paste("Error loading WMS layer:", e$message))
    return(NULL)
  })
}

#' Load data from a Web Feature Service (WFS)
#'
#' Connects to a WFS service and retrieves features for a specified feature type.
#'
#' @param wfs_url Character string with the URL of the WFS service
#' @param feature_type Character string with the name of the feature type to load
#'
#' @return An sf object with the loaded features or NULL if loading fails
#'
#' @importFrom httr GET status_code
#' @importFrom sf st_read
#' @export
#'
#' @examples
#' \dontrun{
#' wfs_data <- load_wfs(
#'   "https://data.geo.admin.ch/api/stac/v0.9/collections/ch.swisstopo.swissboundaries3d-gemeinde-flaeche.fill/items",
#'   "ch.swisstopo.swissboundaries3d-gemeinde-flaeche.fill"
#' )
#' }
load_wfs <- function(wfs_url, feature_type) {
  log_info(paste("Loading WFS feature type:", feature_type, "from URL:", wfs_url))
  
  # Get capabilities to check if the feature type exists
  capabilities_url <- paste0(wfs_url, 
                             ifelse(grepl("\\?", wfs_url), "&", "?"),
                             "SERVICE=WFS&REQUEST=GetCapabilities")
  
  tryCatch({
    # Get capabilities
    response <- httr::GET(capabilities_url)
    if (httr::status_code(response) != 200) {
      log_error(paste("Failed to get WFS capabilities: HTTP", httr::status_code(response)))
      return(NULL)
    }
    
    # Get the actual features
    feature_url <- paste0(wfs_url, 
                          ifelse(grepl("\\?", wfs_url), "&", "?"),
                          "SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=", feature_type)
    
    # Read the data using sf package
    sf_data <- sf::st_read(feature_url, quiet = TRUE)
    
    log_info(paste("Successfully loaded WFS feature type:", feature_type))
    
    return(sf_data)
    
  }, error = function(e) {
    log_error(paste("Error loading WFS feature type:", e$message))
    return(NULL)
  })
}

#==============================================================================
# 6. MAIN WORKFLOW FUNCTION
#==============================================================================

#' Process and harmonize multiple data files
#'
#' This is the main workflow function that combines inspection, harmonization, and linking
#' of multiple datasets. It takes either an existing inspection report or loads one from
#' a file, then processes the data files according to the information in the report.
#'
#' @param report A list containing the inspection report data. Default is NULL.
#' @param report_path Character string with path to an inspection report JSON file. Used only if report is NULL.
#' @param directory_path Character string with path to the directory containing data files.
#' @param output_dir Character string with path where harmonized files will be saved. Default is "harmonized_data".
#'
#' @return A list containing:
#' \itemize{
#'   \item harmonized_data: List of harmonized dataframes
#'   \item key_variables: List of identified key variables for linking
#'   \item harmonization_plan: The plan used for harmonization
#'   \item linked_data: The resulting linked dataset (if linking was performed)
#' }
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Using an inspection report from this package
#' report <- inspect_data("path/to/data")
#' result <- process_data(report = report, directory_path = "path/to/data")
#'
#' # Using a saved inspection report JSON
#' result <- process_data(report_path = "data_inspection_report.json", 
#'                        directory_path = "path/to/data")
#' }
process_data <- function(report = NULL, 
                         report_path = NULL, 
                         directory_path, 
                         output_dir = "harmonized_data") {
  
  # Load report if path is provided
  if (is.null(report) && !is.null(report_path)) {
    report <- load_inspection_report(report_path)
  }
  
  if (is.null(report)) {
    log_error("No inspection report provided or loaded")
    return(NULL)
  }
  
  # Load the data files
  log_info("Loading data files...")
  dataframes <- load_files(report, directory_path)
  
  if (length(dataframes) == 0) {
    log_error("No dataframes loaded, cannot proceed with harmonization")
    return(NULL)
  }
  
  # Identify potential key variables
  log_info("Identifying potential key variables...")
  key_candidates <- identify_key_variables(dataframes, report)
  
  # Generate harmonization plan
  log_info("Generating harmonization plan...")
  harmonization_plan <- generate_harmonization_plan(report)
  
  # Enhance the plan with key variable information
  if (!is.null(key_candidates)) {
    log_info("Enhancing harmonization plan with key variable information...")
    harmonization_plan <- enhance_harmonization_plan(harmonization_plan, key_candidates)
  }
  
  # Proceed with harmonization using the enhanced plan
  log_info("Executing harmonization plan...")
  harmonized_data <- harmonize_dataframes(dataframes, harmonization_plan, output_dir)
  
  # Link datasets if key variables were identified
  linked_data <- NULL
  if (!is.null(key_candidates) && length(key_candidates) > 0) {
    log_info("Linking datasets based on identified key variables...")
    linked_result <- link_datasets(harmonized_data, key_candidates, output_dir)
    linked_data <- linked_result$linked_data
  }
  
  # Return the results
  return(list(
    harmonized_data = harmonized_data,
    key_variables = key_candidates,
    harmonization_plan = harmonization_plan,
    linked_data = linked_data
  ))
}
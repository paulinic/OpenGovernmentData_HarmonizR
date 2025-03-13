# Data Inspector for opendata.swiss Files
# This script helps identify and inspect data files, detect inconsistencies,
# and prepare them for harmonization

# Load required libraries
library(readr)      # For reading CSV files
library(readxl)     # For reading Excel files
library(jsonlite)   # For reading JSON files
library(dplyr)      # For data manipulation
library(purrr)      # For functional programming
library(stringr)    # For string manipulation
library(lubridate)  # For date handling

# Function to scan a directory for supported data files
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

# Function to determine if a column might contain dates
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

# Function to analyze a data frame's structure
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

# Function to analyze a CSV file
analyze_csv <- function(file_info) {
  tryCatch({
    # Detect delimiter
    delimiter <- detect_delimiter(file_info$path)
    
    # Read the CSV
    df <- read_delim(file_info$path, delimiter, 
                     col_types = cols(.default = col_character()),
                     locale = locale(encoding = "UTF-8"))
    
    return(analyze_dataframe(df, file_info))
    
  }, error = function(e) {
    # Return error information
    return(list(
      error = paste("Failed to parse CSV:", e$message),
      filename = file_info$filename
    ))
  })
}

# Function to analyze an Excel file
analyze_excel <- function(file_info) {
  tryCatch({
    # Get sheet names
    sheets <- excel_sheets(file_info$path)
    
    # Analyze each sheet
    sheets_data <- lapply(sheets, function(sheet) {
      df <- read_excel(file_info$path, sheet = sheet)
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

# Function to analyze a JSON file
analyze_json <- function(file_info) {
  tryCatch({
    # Read the JSON
    json_data <- read_json(file_info$path)
    
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

# Function to analyze all files
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

# Function to find common fields across files
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

# Function to detect inconsistencies across files
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

# Function to generate a comprehensive report
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

# Main function to run the data inspector
inspect_data <- function(directory_path) {
  cat("Scanning directory for data files...\n")
  file_info <- scan_directory(directory_path)
  cat(paste("Found", nrow(file_info), "files to analyze\n"))
  
  cat("Analyzing files...\n")
  file_summaries <- analyze_all_files(file_info)
  
  cat("Generating report...\n")
  report <- generate_report(file_info, file_summaries)
  
  cat(paste("Analysis complete. Found", report$inconsistencies$inconsistency_count, 
            "potential inconsistencies\n"))
  cat(paste("Found", report$common_fields$common_field_count, 
            "fields that appear in multiple files\n"))
  
  # Save the report
  report_json <- jsonlite::toJSON(report, auto_unbox = TRUE, pretty = TRUE)
  write(report_json, "data_inspection_report.json")
  cat("Report saved to data_inspection_report.json\n")
  
  return(report)
}

# Example usage
# To use this script, set your directory path and run:
# report <- inspect_data("./opendata_swiss_files")
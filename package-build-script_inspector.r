#!/usr/bin/env Rscript

# Script to create an R package from the data inspector code
# Run this script from a directory where you want to create the package

# Install required packages
if (!requireNamespace("devtools", quietly = TRUE)) {
  install.packages("devtools")
}
if (!requireNamespace("roxygen2", quietly = TRUE)) {
  install.packages("roxygen2")
}
if (!requireNamespace("testthat", quietly = TRUE)) {
  install.packages("testthat")
}

library(devtools)

# Set package name
pkg_name <- "openswissdata"

# Create package structure
cat("Creating package structure...\n")
create_package(pkg_name)

# Navigate to package directory
setwd(pkg_name)

# Create R directory structure
dir.create("R", showWarnings = FALSE)

# Create individual R files
cat("Creating R source files...\n")

# utils.R
cat('# Utility functions

#\' Detect CSV delimiter
#\'
#\' This function tries to detect the delimiter used in a CSV file
#\'
#\' @param file_path Path to the CSV file
#\' @return Character representing the most likely delimiter
#\' @keywords internal
detect_delimiter <- function(file_path) {
  # Read first few lines
  conn <- file(file_path, "r")
  first_lines <- readLines(conn, n = 5)
  close(conn)
  
  # Count potential delimiters
  delimiters <- c(",", ";", "\\t", "|")
  counts <- sapply(delimiters, function(d) {
    sum(stringr::str_count(first_lines, fixed(d)))
  })
  
  # Return most common delimiter
  return(delimiters[which.max(counts)])
}

#\' Check if a column might contain dates
#\'
#\' This function samples values from a column and checks if they appear to be dates
#\'
#\' @param values Vector of values to check
#\' @return Logical indicating if the column might contain dates
#\' @keywords internal
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
}', file = "R/utils.R")

# scan_files.R
cat('#\' Scan a directory for supported data files
#\'
#\' This function scans a directory and identifies supported data files for analysis
#\'
#\' @param directory_path Path to the directory containing data files
#\' @return A data frame with information about the discovered files
#\' @export
#\' @examples
#\' \\dontrun{
#\' file_info <- scan_directory("path/to/data")
#\' }
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
}', file = "R/scan_files.R")

# analyze_files.R
cat('#\' Analyze a data frame\'s structure
#\'
#\' This function examines a data frame and provides detailed analysis of its structure
#\'
#\' @param df Data frame to analyze
#\' @param file_info File information
#\' @param sheet_name Optional sheet name for Excel files
#\' @return A list with analysis results
#\' @keywords internal
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

#\' Analyze a CSV file
#\'
#\' This function analyzes the structure and content of a CSV file
#\'
#\' @param file_info File information from scan_directory
#\' @return A list with analysis results
#\' @importFrom readr read_delim cols col_character locale
#\' @export
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

#\' Analyze an Excel file
#\'
#\' This function analyzes the structure and content of an Excel file
#\'
#\' @param file_info File information from scan_directory
#\' @return A list with analysis results
#\' @importFrom readxl excel_sheets read_excel
#\' @export
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

#\' Analyze a JSON file
#\'
#\' This function analyzes the structure and content of a JSON file
#\'
#\' @param file_info File information from scan_directory
#\' @return A list with analysis results
#\' @importFrom jsonlite read_json
#\' @export
analyze_json <- function(file_info) {
  tryCatch({
    # Read the JSON
    json_data <- jsonlite::read_json(file_info$path)
    
    # Check if it\'s a list of objects (can be converted to data frame)
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

#\' Analyze all files
#\'
#\' This function analyzes all files in the provided file information
#\'
#\' @param file_info File information from scan_directory
#\' @return A list with analysis results for all files
#\' @export
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
}', file = "R/analyze_files.R")

# detect_issues.R
cat('#\' Find common fields across files
#\'
#\' This function identifies fields that appear in multiple files
#\'
#\' @param file_summaries Analysis results from analyze_all_files
#\' @return A list with common field information
#\' @export
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

#\' Detect inconsistencies across files
#\'
#\' This function identifies inconsistencies in field types across files
#\'
#\' @param file_summaries Analysis results from analyze_all_files
#\' @return A list with inconsistency information
#\' @export
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
}', file = "R/detect_issues.R")

# reporting.R
cat('#\' Generate a comprehensive report
#\'
#\' This function generates a comprehensive report of the data analysis
#\'
#\' @param file_info File information from scan_directory
#\' @param file_summaries Analysis results from analyze_all_files
#\' @return A list with the comprehensive report
#\' @export
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

#\' Save report to JSON file
#\'
#\' This function saves the analysis report to a JSON file
#\'
#\' @param report Report from generate_report
#\' @param file_path Path where to save the JSON file
#\' @return Invisibly returns the path to the saved file
#\' @importFrom jsonlite toJSON write_json
#\' @export
save_report <- function(report, file_path = "data_inspection_report.json") {
  jsonlite::write_json(report, file_path, auto_unbox = TRUE, pretty = TRUE)
  message(paste("Report saved to", file_path))
  return(invisible(file_path))
}', file = "R/reporting.R")

# inspector.R
cat('#\' Inspect data in a directory
#\'
#\' This function runs a complete data inspection workflow on files in a directory
#\'
#\' @param directory_path Path to the directory containing data files
#\' @param save_to_file Whether to save the report to a JSON file
#\' @param output_file Path where to save the JSON file if save_to_file is TRUE
#\' @return A list with the comprehensive inspection report
#\' @export
#\' @examples
#\' \\dontrun{
#\' report <- inspect_data("path/to/data")
#\' }
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
}', file = "R/inspector.R")

# Set up package dependencies
cat("Setting up package metadata...\n")
use_mit_license()

# Add package dependencies
use_package("readr")
use_package("readxl")
use_package("jsonlite")
use_package("dplyr")
use_package("purrr")
use_package("stringr")
use_package("lubridate")

# Set up testing infrastructure
use_testthat()

# Create a README file
use_readme_md()

# Update the README with basic usage information
cat('# openswissdata

Tools for inspecting, analyzing, and harmonizing data files from the OpenData.Swiss platform.

## Installation

You can install the development version of openswissdata from GitHub with:

```r
# install.packages("devtools")
devtools::install_github("yourusername/openswissdata")
```

## Usage

```r
library(openswissdata)

# Run inspection on a directory of data files
report <- inspect_data("path/to/data/directory")

# Run inspection without saving to file
report <- inspect_data("path/to/data/directory", save_to_file = FALSE)

# Save report to a custom location
report <- inspect_data("path/to/data/directory", output_file = "custom_report.json")
```

## Features

- Scans directories for supported data files (CSV, Excel, JSON)
- Analyzes file structure and content
- Identifies common fields across files
- Detects type inconsistencies
- Generates comprehensive reports
', file = "README.md")

# Generate documentation
cat("Generating documentation...\n")
document()

# Run checks
cat("Running package checks...\n")
check()

# Build the package
cat("Building package...\n")
build()

# Install the package
cat("Installing package...\n")
install()

cat("\nPackage successfully built and installed!\n")
cat("You can now use the package with: library(openswissdata)\n")

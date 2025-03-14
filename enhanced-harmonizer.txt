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

# Function to create WMS/WFS service description files
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

# Example usage
# 1. Using an inspection report from the inspector package
# library(openswissdata)
# report <- inspect_data("path/to/data")
# result <- process_data(report = report, directory_path = "path/to/data")

# 2. Using a saved inspection report JSON
# result <- process_data(report_path = "data_inspection_report.json", 
#                       directory_path = "path/to/data")

# 3. Access the results
# harmonized_data <- result$harmonized_data
# linked_data <- result$linked_data
# key_variables <- result$key_variables

# 4. Examples of working with WMS/WFS services:
# 
# # Create a WMS service description file
# create_service_description(
#   output_dir = "data",
#   service_type = "wms",
#   service_url = "https://wms.geo.admin.ch/",
#   layer_or_feature = "ch.swisstopo.pixelkarte-farbe"
# )
# 
# # Create a WFS service description file
# create_service_description(
#   output_dir = "data",
#   service_type = "wfs",
#   service_url = "https://data.geo.admin.ch/api/stac/v0.9/collections/ch.swisstopo.swissboundaries3d-gemeinde-flaeche.fill/items",
#   layer_or_feature = "ch.swisstopo.swissboundaries3d-gemeinde-flaeche.fill"
# )
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
library(sf)           # For spatial data handling
library(httr)         # For HTTP requests
library(xml2)         # For XML parsing

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

# Function to parse WMS GetCapabilities response
parse_wms_capabilities <- function(capabilities_xml) {
  # Parse the XML
  xml_doc <- as_xml_document(capabilities_xml)
  
  # Extract layers information
  layers <- xml_find_all(xml_doc, "//Layer[Name]")
  
  layer_info <- lapply(layers, function(layer) {
    name <- xml_text(xml_find_first(layer, "./Name"))
    title <- xml_text(xml_find_first(layer, "./Title"))
    abstract <- xml_text(xml_find_first(layer, "./Abstract"))
    
    # Extract bounding box if available
    bbox <- NULL
    bbox_node <- xml_find_first(layer, "./BoundingBox[@CRS]")
    if (!is.na(bbox_node)) {
      bbox <- list(
        crs = xml_attr(bbox_node, "CRS"),
        minx = as.numeric(xml_attr(bbox_node, "minx")),
        miny = as.numeric(xml_attr(bbox_node, "miny")),
        maxx = as.numeric(xml_attr(bbox_node, "maxx")),
        maxy = as.numeric(xml_attr(bbox_node, "maxy"))
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

# Function to load WMS data
load_wms <- function(wms_url, layer_name) {
  log_info(paste("Loading WMS layer:", layer_name, "from URL:", wms_url))
  
  # Get capabilities to check if the layer exists
  capabilities_url <- paste0(wms_url, 
                            ifelse(grepl("\\?", wms_url), "&", "?"),
                            "SERVICE=WMS&REQUEST=GetCapabilities")
  
  tryCatch({
    response <- GET(capabilities_url)
    if (status_code(response) != 200) {
      log_error(paste("Failed to get WMS capabilities: HTTP", status_code(response)))
      return(NULL)
    }
    
    # Parse capabilities
    capabilities_xml <- content(response, "text")
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

# Function to load WFS data
load_wfs <- function(wfs_url, feature_type) {
  log_info(paste("Loading WFS feature type:", feature_type, "from URL:", wfs_url))
  
  # Get capabilities to check if the feature type exists
  capabilities_url <- paste0(wfs_url, 
                            ifelse(grepl("\\?", wfs_url), "&", "?"),
                            "SERVICE=WFS&REQUEST=GetCapabilities")
  
  tryCatch({
    # Get capabilities
    response <- GET(capabilities_url)
    if (status_code(response) != 200) {
      log_error(paste("Failed to get WFS capabilities: HTTP", status_code(response)))
      return(NULL)
    }
    
    # Get the actual features
    feature_url <- paste0(wfs_url, 
                         ifelse(grepl("\\?", wfs_url), "&", "?"),
                         "SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=", feature_type)
    
    # Read the data using sf package
    sf_data <- st_read(feature_url, quiet = TRUE)
    
    # Convert to dataframe, keeping geometry
    log_info(paste("Successfully loaded WFS feature type:", feature_type))
    
    return(sf_data)
    
  }, error = function(e) {
    log_error(paste("Error loading WFS feature type:", e$message))
    return(NULL)
  })
}

# Function to read GeoJSON file
read_geojson <- function(file_path) {
  log_info(paste("Reading GeoJSON file:", file_path))
  
  tryCatch({
    sf_data <- st_read(file_path, quiet = TRUE)
    log_info(paste("Successfully read GeoJSON file with", nrow(sf_data), "features"))
    return(sf_data)
  }, error = function(e) {
    log_error(paste("Error reading GeoJSON file:", e$message))
    return(NULL)
  })
}

# Function to extract attributes from spatial data
extract_attributes_from_spatial <- function(sf_data) {
  log_info("Extracting attributes from spatial data")
  
  tryCatch({
    # Extract non-geometry attributes
    attributes_df <- st_drop_geometry(sf_data)
    
    # Add a column with WKT representation of geometries for potential linking
    attributes_df$geometry_wkt <- st_as_text(st_geometry(sf_data))
    
    log_info(paste("Extracted", ncol(attributes_df), "attributes from spatial data"))
    return(attributes_df)
  }, error = function(e) {
    log_error(paste("Error extracting attributes from spatial data:", e$message))
    return(data.frame())
  })
}

# Function to load data files, including WMS/WFS
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
      } else if (endsWith(filename, ".json") || endsWith(filename, ".geojson")) {
        # JSON/GeoJSON file
        tryCatch({
          if (endsWith(filename, ".geojson")) {
            # Handle GeoJSON
            sf_data <- read_geojson(file_path)
            if (!is.null(sf_data)) {
              # Extract attributes
              df <- extract_attributes_from_spatial(sf_data)
              dataframes[[filename]] <- df
              log_info(paste("Loaded GeoJSON file", filename))
            }
          } else {
            # Regular JSON
            json_data <- fromJSON(file_path)
            
            if (is.data.frame(json_data) || 
                (is.list(json_data) && all(sapply(json_data, is.list)))) {
              df <- as.data.frame(json_data)
              dataframes[[filename]] <- df
              log_info(paste("Loaded JSON file", filename))
            } else {
              log_warning(paste("JSON file", filename, "is not in tabular format, skipping"))
            }
          }
        }, error = function(e) {
          log_error(paste("Failed to load JSON/GeoJSON", filename, ":", e$message))
        })
      } else if (endsWith(filename, ".wms") || endsWith(filename, ".wfs")) {
        # WMS/WFS service description file
        tryCatch({
          # Read the service description file
          service_desc <- readLines(file_path)
          
          # Parse the URL and layer/feature name
          service_url <- service_desc[grep("^URL=", service_desc)]
          service_url <- sub("^URL=", "", service_url)
          
          if (endsWith(filename, ".wms")) {
            # WMS file
            layer_name <- service_desc[grep("^LAYER=", service_desc)]
            layer_name <- sub("^LAYER=", "", layer_name)
            
            if (length(service_url) > 0 && length(layer_name) > 0) {
              wms_data <- load_wms(service_url, layer_name)
              if (!is.null(wms_data)) {
                dataframes[[filename]] <- wms_data
                log_info(paste("Loaded WMS service description", filename))
              }
            } else {
              log_warning(paste("Invalid WMS service description in", filename))
            }
          } else {
            # WFS file
            feature_type <- service_desc[grep("^FEATURETYPE=", service_desc)]
            feature_type <- sub("^FEATURETYPE=", "", feature_type)
            
            if (length(service_url) > 0 && length(feature_type) > 0) {
              wfs_data <- load_wfs(service_url, feature_type)
              if (!is.null(wfs_data)) {
                # Extract attributes
                df <- extract_attributes_from_spatial(wfs_data)
                dataframes[[filename]] <- df
                log_info(paste("Loaded WFS service description", filename))
              }
            } else {
              log_warning(paste("Invalid WFS service description in", filename))
            }
          }
        }, error = function(e) {
          log_error(paste("Failed to load WMS/WFS service description", filename, ":", e$message))
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
  }
  
  # Create output directory if it doesn't exist
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
    log_info(paste("Created output directory:", output_dir))
  }
  
  # Initialize list to store harmonized dataframes
  harmonized <- list()
  
  # Process each dataframe
  for (df_name in names(dataframes)) {
    df <- dataframes[[df_name]]
    log_info(paste("Harmonizing", df_name))
    
    # Create a copy of the dataframe to modify
    harmonized_df <- df
    
    # Process each field in the harmonization plan
    for (field in names(harmonization_plan)) {
      plan_item <- harmonization_plan[[field]]
      
      # Check if this field exists in the current dataframe
      if (!(field %in% names(df))) {
        next
      }
      
      # Find the action for this field in this dataframe
      action_for_df <- NULL
      for (action in plan_item$harmonization_actions) {
        if (action$source == df_name) {
          action_for_df <- action
          break
        }
      }
      
      if (is.null(action_for_df)) {
        next
      }
      
      # Standardize field name
      old_name <- field
      new_name <- plan_item$field_name
      
      # Perform the specified action
      if (action_for_df$action == "convert_to_datetime") {
        result <- convert_to_datetime(harmonized_df, old_name)
        harmonized_df <- result$df
      } else if (action_for_df$action == "convert_to_numeric") {
        result <- convert_to_numeric(harmonized_df, old_name)
        harmonized_df <- result$df
      } else if (action_for_df$action == "convert_to_text") {
        harmonized_df[[old_name]] <- as.character(harmonized_df[[old_name]])
      } else if (action_for_df$action == "best_effort_conversion") {
        # Try various conversions based on target type
        if (plan_item$target_type == "datetime") {
          result <- convert_to_datetime(harmonized_df, old_name)
          harmonized_df <- result$df
        } else if (plan_item$target_type == "numeric") {
          result <- convert_to_numeric(harmonized_df, old_name)
          harmonized_df <- result$df
        } else {
          harmonized_df[[old_name]] <- as.character(harmonized_df[[old_name]])
        }
      }
      
      # Rename the column if standardized name is different
      if (old_name != new_name) {
        names(harmonized_df)[names(harmonized_df) == old_name] <- new_name
        log_info(paste("Renamed column", old_name, "to", new_name, "in", df_name))
      }
    }
    
    # Add source column to indicate origin
    harmonized_df$data_source <- df_name
    
    # Store the harmonized dataframe
    harmonized[[df_name]] <- harmonized_df
    
    # Save individual harmonized file
    output_file <- file.path(output_dir, paste0("harmonized_", gsub("[^a-zA-Z0-9]", "_", df_name), ".csv"))
    write_csv(harmonized_df, output_file)
    log_info(paste("Saved harmonized data to", output_file))
  }
  
  return(harmonized)
}

# NEW FUNCTION: Identify potential key variables for linking datasets
identify_key_variables <- function(dataframes, report) {
  if (length(dataframes) < 2) {
    log_warning("Need at least two dataframes to identify linking keys")
    return(NULL)
  }
  
  log_info("Analyzing potential key variables for dataset linkage...")
  
  # Find common fields across datasets
  common_fields <- names(report$common_fields$common_fields)
  if (length(common_fields) == 0) {
    log_warning("No common fields found across datasets")
    return(NULL)
  }
  
  # Store key variable candidates with their properties
  key_candidates <- list()
  
  # For each common field, evaluate its potential as a key
  for (field in common_fields) {
    # Track which datasets contain this field
    containing_datasets <- list()
    
    # Store field uniqueness scores
    uniqueness_scores <- c()
    
    # Store field completeness (non-NA values)
    completeness_scores <- c()
    
    # For each dataset, check this field's properties
    for (df_name in names(dataframes)) {
      df <- dataframes[[df_name]]
      
      # Standardized field name
      std_field_name <- standardize_field_name(field)
      
      # Check for both original and standardized field name
      field_in_df <- field
      if (!(field %in% names(df)) && (std_field_name %in% names(df))) {
        field_in_df <- std_field_name
      } else if (!(field %in% names(df))) {
        next
      }
      
      # Add to list of datasets containing this field
      containing_datasets <- c(containing_datasets, df_name)
      
      # Calculate uniqueness (ratio of unique values to total values)
      total_values <- nrow(df)
      unique_values <- length(unique(df[[field_in_df]]))
      uniqueness_score <- unique_values / total_values
      uniqueness_scores <- c(uniqueness_scores, uniqueness_score)
      
      # Calculate completeness (ratio of non-NA values to total values)
      non_na_values <- sum(!is.na(df[[field_in_df]]))
      completeness_score <- non_na_values / total_values
      completeness_scores <- c(completeness_scores, completeness_score)
    }
    
    # Skip fields that don't appear in at least 2 datasets
    if (length(containing_datasets) < 2) {
      next
    }
    
    # Calculate overall scores
    mean_uniqueness <- mean(uniqueness_scores)
    mean_completeness <- mean(completeness_scores)
    
    # Composite key quality score (prioritize uniqueness and completeness)
    key_quality <- (mean_uniqueness * 0.7) + (mean_completeness * 0.3)
    
    # Store candidate information
    key_candidates[[field]] <- list(
      field_name = field,
      standardized_name = standardize_field_name(field),
      datasets = containing_datasets,
      dataset_coverage = length(containing_datasets) / length(dataframes),
      mean_uniqueness = mean_uniqueness,
      mean_completeness = mean_completeness,
      key_quality = key_quality
    )
  }
  
  # If no key candidates were found
  if (length(key_candidates) == 0) {
    log_warning("No suitable key variables found for linking datasets")
    return(NULL)
  }
  
  # Sort candidates by key quality
  key_qualities <- sapply(key_candidates, function(x) x$key_quality)
  key_candidates <- key_candidates[order(key_qualities, decreasing = TRUE)]
  
  # Log the top candidates
  log_info(paste("Found", length(key_candidates), "potential key variables for linking datasets"))
  for (i in 1:min(3, length(key_candidates))) {
    candidate <- key_candidates[[i]]
    log_info(paste0(
      "Key candidate #", i, ": ", candidate$field_name,
      " (Quality: ", round(candidate$key_quality, 2),
      ", Uniqueness: ", round(candidate$mean_uniqueness, 2),
      ", Completeness: ", round(candidate$mean_completeness, 2),
      ", Coverage: ", round(candidate$dataset_coverage * 100), "% of datasets)"
    ))
  }
  
  return(key_candidates)
}

# NEW FUNCTION: Enhance the harmonization plan with key variable information
enhance_harmonization_plan <- function(harmonization_plan, key_candidates) {
  if (is.null(key_candidates) || length(key_candidates) == 0) {
    log_warning("No key candidates available to enhance harmonization plan")
    return(harmonization_plan)
  }
  
  # Get the top key candidate
  top_key <- key_candidates[[1]]
  
  # Check if the top key is in the harmonization plan
  if (top_key$field_name %in% names(harmonization_plan)) {
    # Add key variable flag to the harmonization plan
    harmonization_plan[[top_key$field_name]]$is_key_variable <- TRUE
    harmonization_plan[[top_key$field_name]]$key_quality <- top_key$key_quality
    
    log_info(paste("Marked", top_key$field_name, "as primary key variable in harmonization plan"))
  }
  
  # Add secondary keys if they have good quality (over 0.6)
  for (i in 2:min(3, length(key_candidates))) {
    candidate <- key_candidates[[i]]
    if (candidate$key_quality >= 0.6 && candidate$field_name %in% names(harmonization_plan)) {
      harmonization_plan[[candidate$field_name]]$is_secondary_key <- TRUE
      harmonization_plan[[candidate$field_name]]$key_quality <- candidate$key_quality
      
      log_info(paste("Marked", candidate$field_name, "as secondary key variable in harmonization plan"))
    }
  }
  
  return(harmonization_plan)
}

# NEW FUNCTION: Link datasets based on identified key variables
link_datasets <- function(harmonized_dataframes, key_candidates, output_dir = "harmonized_data") {
  if (is.null(key_candidates) || length(key_candidates) == 0 || length(harmonized_dataframes) < 2) {
    log_warning("Cannot link datasets: missing key candidates or fewer than 2 dataframes")
    return(NULL)
  }
  
  # Get the primary key for linking
  primary_key <- key_candidates[[1]]
  primary_key_name <- primary_key$standardized_name
  
  log_info(paste("Linking datasets using primary key:", primary_key_name))
  
  # Determine which dataframes have the primary key
  linkable_dfs <- list()
  for (df_name in names(harmonized_dataframes)) {
    df <- harmonized_dataframes[[df_name]]
    if (primary_key_name %in% names(df) || primary_key$field_name %in% names(df)) {
      # Use the actual column name in this dataframe
      key_col <- if (primary_key_name %in% names(df)) primary_key_name else primary_key$field_name
      linkable_dfs[[df_name]] <- list(df = df, key_col = key_col)
    }
  }
  
  if (length(linkable_dfs) < 2) {
    log_warning(paste("Fewer than 2 dataframes contain the primary key", primary_key_name))
    return(NULL)
  }
  
  # Start with the first dataframe as the base
  first_df_name <- names(linkable_dfs)[1]
  first_df <- linkable_dfs[[first_df_name]]$df
  key_col <- linkable_dfs[[first_df_name]]$key_col
  
  # Standardize key column name in the first dataframe if needed
  if (key_col != primary_key_name) {
    names(first_df)[names(first_df) == key_col] <- primary_key_name
  }
  
  # Merge with subsequent dataframes
  merged_df <- first_df
  for (i in 2:length(linkable_dfs)) {
    df_name <- names(linkable_dfs)[i]
    next_df <- linkable_dfs[[df_name]]$df
    next_key_col <- linkable_dfs[[df_name]]$key_col
    
    # Standardize key column name in the next dataframe if needed
    if (next_key_col != primary_key_name) {
      names(next_df)[names(next_df) == next_key_col] <- primary_key_name
    }
    
    # Prepare suffixes to avoid column name conflicts
    left_suffix <- paste0("_", gsub("[^a-zA-Z0-9]", "", first_df_name))
    right_suffix <- paste0("_", gsub("[^a-zA-Z0-9]", "", df_name))
    
    # Merge dataframes
    log_info(paste("Merging", first_df_name, "with", df_name, "using key", primary_key_name))
    
    # Perform a full join to keep all records
    merged_df <- full_join(
      merged_df, next_df, 
      by = primary_key_name,
      suffix = c(left_suffix, right_suffix)
    )
  }
  
  # Save the merged dataset
  output_file <- file.path(output_dir, "linked_datasets.csv")
  write_csv(merged_df, output_file)
  log_info(paste("Saved linked dataset to", output_file))
  
  # Return both the merged dataframe and key information
  return(list(
    linked_data = merged_df,
    primary_key = primary_key,
    dataframes_linked = names(linkable_dfs)
  ))
}

# NEW FUNCTION: Main workflow function combining inspection, harmonization, and linking
process_data <- function(report = NULL, report_path = NULL, directory_path, 
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
  
  #
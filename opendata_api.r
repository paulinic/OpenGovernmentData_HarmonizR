# Script to download all (or selected) datasets from opendata.swiss
# ------------------------------------------------------

# 1. Load required libraries (installing if needed)
required_packages <- c("httr", "jsonlite", "dplyr", "tidyr", "progress")

# Install missing packages
for(package in required_packages) {
  if(!require(package, character.only = TRUE, quietly = TRUE)) {
    install.packages(package)
    library(package, character.only = TRUE)
  }
}

# Create a directory structure
main_dir <- "swiss_open_data_full"
dirs <- c("raw", "metadata", "logs")

# Create main directory and subdirectories
for(dir in dirs) {
  dir_path <- file.path(main_dir, dir)
  if(!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
    cat("Created directory:", dir_path, "\n")
  }
}

# Create a log file
log_file <- file.path(main_dir, "logs", "download_log.txt")
cat(paste0("Download session started: ", Sys.time(), "\n"), file = log_file)

# Function to log messages
log_message <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_entry <- paste0(timestamp, " - ", message, "\n")
  cat(log_entry)
  cat(log_entry, file = log_file, append = TRUE)
}

# Function to get all dataset IDs from opendata.swiss
get_all_dataset_ids <- function() {
  log_message("Retrieving list of all datasets...")
  
  # Get the list of all package names
  url <- "https://opendata.swiss/api/3/action/package_list"
  response <- httr::GET(url)
  
  if(httr::status_code(response) == 200) {
    # Parse the JSON response
    package_list <- jsonlite::fromJSON(rawToChar(response$content))
    
    if(package_list$success) {
      dataset_ids <- package_list$result
      log_message(paste("Found", length(dataset_ids), "datasets"))
      return(dataset_ids)
    } else {
      log_message("API returned success=FALSE")
      return(NULL)
    }
  } else {
    log_message(paste("Failed to get dataset list. HTTP status:", httr::status_code(response)))
    return(NULL)
  }
}

# Function to get dataset information
get_dataset_info <- function(dataset_id) {
  # Construct the URL to get dataset information
  metadata_url <- paste0("https://opendata.swiss/api/3/action/package_show?id=", dataset_id)
  
  # Get dataset metadata
  response <- httr::GET(metadata_url)
  
  # Check if request was successful
  if(httr::status_code(response) == 200) {
    # Parse the JSON response
    dataset_info <- jsonlite::fromJSON(rawToChar(response$content))
    
    if(dataset_info$success) {
      return(dataset_info$result)
    } else {
      log_message(paste("API returned success=FALSE for dataset ID:", dataset_id))
      return(NULL)
    }
  } else {
    log_message(paste("Failed to get info for dataset ID:", dataset_id, 
                      "HTTP status:", httr::status_code(response)))
    return(NULL)
  }
}

# Function to download a resource
download_resource <- function(resource, dataset_name, dataset_id) {
  # Skip resources without a URL
  if(is.null(resource$url) || resource$url == "") {
    log_message(paste("  Skipping resource (no URL):", resource$name))
    return(FALSE)
  }
  
  # Create a safe filename based on resource name or ID
  resource_name <- if(!is.null(resource$name) && resource$name != "") {
    gsub("[^a-zA-Z0-9]", "_", resource$name)
  } else {
    resource$id
  }
  
  # Determine file extension from format or URL
  file_ext <- if(!is.null(resource$format) && resource$format != "") {
    tolower(resource$format)
  } else {
    # Extract extension from URL
    url_parts <- strsplit(resource$url, "\\.")
    if(length(url_parts[[1]]) > 1) {
      tolower(tail(url_parts[[1]], 1))
    } else {
      "unknown"
    }
  }
  
  # Clean up file extension
  file_ext <- gsub("[^a-zA-Z0-9]", "", file_ext)
  if(file_ext == "") file_ext <- "unknown"
  
  # Create the output filename
  output_file <- file.path(main_dir, "raw", 
                           paste0(dataset_name, "__", resource_name, ".", file_ext))
  
  # Don't re-download if file exists
  if(file.exists(output_file)) {
    log_message(paste("  Resource already downloaded:", resource_name))
    return(TRUE)
  }
  
  # Download the resource
  tryCatch({
    log_message(paste("  Downloading resource:", resource_name))
    
    # Some URLs might have spaces or special characters
    download_url <- URLencode(resource$url)
    
    # Try to download with a timeout
    response <- httr::GET(download_url, 
                          httr::timeout(60),  # 60 second timeout
                          httr::write_disk(output_file, overwrite = TRUE))
    
    if(httr::status_code(response) == 200) {
      log_message(paste("  Successfully downloaded:", resource_name))
      
      # Create resource metadata
      metadata_file <- file.path(main_dir, "metadata", 
                                 paste0(dataset_name, "__", resource_name, "_meta.json"))
      jsonlite::write_json(resource, metadata_file, pretty = TRUE)
      
      return(TRUE)
    } else {
      log_message(paste("  Failed to download resource. HTTP status:", 
                        httr::status_code(response)))
      return(FALSE)
    }
  }, error = function(e) {
    log_message(paste("  Error downloading resource:", e$message))
    return(FALSE)
  })
}

# Function to download all resources for a dataset
download_dataset <- function(dataset_id) {
  log_message(paste("Processing dataset ID:", dataset_id))
  
  # Get dataset info
  dataset_info <- get_dataset_info(dataset_id)
  
  if(is.null(dataset_info)) {
    log_message(paste("Could not get info for dataset ID:", dataset_id))
    return(FALSE)
  }
  
  # Create a safe dataset name
  dataset_name <- gsub("[^a-zA-Z0-9]", "_", dataset_info$title)
  if(nchar(dataset_name) > 50) dataset_name <- substr(dataset_name, 1, 50)
  dataset_name <- paste0(dataset_name, "_", substr(dataset_id, 1, 8))
  
  log_message(paste("Dataset name:", dataset_info$title))
  
  # Save dataset metadata
  metadata_file <- file.path(main_dir, "metadata", paste0(dataset_name, "_info.json"))
  jsonlite::write_json(dataset_info, metadata_file, pretty = TRUE)
  
  # Check if there are resources
  if(length(dataset_info$resources) == 0) {
    log_message("No resources found for this dataset")
    return(FALSE)
  }
  
  # Download each resource
  resources_count <- length(dataset_info$resources)
  log_message(paste("Found", resources_count, "resources"))
  
  success_count <- 0
  for(i in 1:resources_count) {
    resource <- dataset_info$resources[[i]]
    if(download_resource(resource, dataset_name, dataset_id)) {
      success_count <- success_count + 1
    }
  }
  
  log_message(paste("Downloaded", success_count, "out of", resources_count, "resources"))
  return(success_count > 0)
}

#  Main function to download all datasets with controls
#download_all_datasets <- function(start_index = 1, max_datasets = NULL, 
                                #  formats = NULL, organizations = NULL) {
  # Get all dataset IDs
 # all_dataset_ids <- get_all_dataset_ids()
  
  #if(is.null(all_dataset_ids)) {
    #log_message("Failed to retrieve dataset IDs. Aborting.")
    #return(FALSE)
 # }
  
  # Determine how many datasets to process
  #total_datasets <- length(all_dataset_ids)
  #if(is.null(max_datasets) || max_datasets > total_datasets) {
    #max_datasets <- total_datasets
  #}
  
  # Make sure start_index is valid
  #if(start_index < 1 || start_index > total_datasets) {
    #start_index <- 1
  #}
  
  # Calculate end index
  #end_index <- min(start_index + max_datasets - 1, total_datasets)
  
  # Process the datasets with a progress bar
  #log_message(paste("Will download datasets from index", start_index, "to", end_index))
  
  #pb <- progress::progress_bar$new(
    #format = "[:bar] :percent | Elapsed: :elapsed | ETA: :eta | :current/:total datasets",
    #total = end_index - start_index + 1,
    #clear = FALSE,
    #width = 80
  #)
  
  #dataset_counter <- 0
  #success_counter <- 0
  
  #for(i in start_index:end_index) {
    #dataset_id <- all_dataset_ids[i]
    #pb$tick()
    
    #dataset_counter <- dataset_counter + 1
    #if(download_dataset(dataset_id)) {
      #success_counter <- success_counter + 1
    #}
    
    # Add a small delay to avoid overwhelming the server
    #Sys.sleep(0.5)
  #}
  
  #log_message(paste("Download complete! Successfully processed", 
                    #success_counter, "out of", dataset_counter, "datasets"))
  #return(TRUE)
#}

# 8. Download a sample (first 10 datasets) to test

# Function to discover and download datasets from opendata.swiss
discover_and_download <- function(num_datasets = 10) {
  # Install and load required packages
  if (!require("httr")) install.packages("httr")
  if (!require("jsonlite")) install.packages("jsonlite")
  library(httr)
  library(jsonlite)
  
  # Create directories
  main_dir <- "swiss_opendata_samples"
  raw_dir <- file.path(main_dir, "raw")
  metadata_dir <- file.path(main_dir, "metadata")
  
  for (dir in c(raw_dir, metadata_dir)) {
    if (!dir.exists(dir)) {
      dir.create(dir, recursive = TRUE)
      cat("Created directory:", dir, "\n")
    }
  }
  
  # Step 1: Get a list of dataset IDs
  cat("Retrieving dataset list from opendata.swiss...\n")
  dataset_list_url <- "https://opendata.swiss/api/3/action/package_list"
  
  tryCatch({
    response <- GET(dataset_list_url)
    
    if (status_code(response) != 200) {
      stop("Failed to get dataset list. HTTP status: ", status_code(response))
    }
    
    dataset_list <- fromJSON(rawToChar(response$content))
    
    if (!dataset_list$success) {
      stop("API returned success=FALSE")
    }
    
    all_dataset_ids <- dataset_list$result
    cat("Found", length(all_dataset_ids), "datasets\n")
    
    # Set seed for reproducibility
    set.seed(123)
    selected_ids <- sample(all_dataset_ids, min(num_datasets, length(all_dataset_ids)))
    
    cat("Selected", length(selected_ids), "random datasets\n")
    
  }, error = function(e) {
    stop("Error retrieving dataset list: ", e$message)
  })
  
  # Step 2: Process each selected dataset
  successful_downloads <- 0
  
  for (i in seq_along(selected_ids)) {
    dataset_id <- selected_ids[i]
    cat("\nProcessing dataset", i, "of", length(selected_ids), ":", dataset_id, "\n")
    
    tryCatch({
      # Get dataset metadata
      metadata_url <- paste0("https://opendata.swiss/api/3/action/package_show?id=", dataset_id)
      metadata_response <- GET(metadata_url)
      
      if (status_code(metadata_response) != 200) {
        cat("Skipping dataset - HTTP status:", status_code(metadata_response), "\n")
        next
      }
      
      # Parse dataset info
      dataset_info <- fromJSON(rawToChar(metadata_response$content))$result
      dataset_title <- dataset_info$title
      safe_name <- gsub("[^a-zA-Z0-9]", "_", dataset_title)
      # Truncate if too long
      if (nchar(safe_name) > 50) safe_name <- substr(safe_name, 1, 50)
      
      cat("Dataset title:", dataset_title, "\n")
      
      # Save metadata
      metadata_file <- file.path(metadata_dir, paste0(safe_name, ".json"))
      write_json(dataset_info, metadata_file, pretty = TRUE)
      
      # Check for resources
      if (length(dataset_info$resources) == 0) {
        cat("No resources found for this dataset\n")
        next
      }
      
      cat("Found", length(dataset_info$resources), "resources\n")
      
      # Try to download one resource (preferably CSV, JSON, or XML)
      preferred_formats <- c("csv", "json", "xml", "xlsx", "xls")
      resource_to_download <- NULL
      
      # First try to find a preferred format
      for (format in preferred_formats) {
        for (j in seq_along(dataset_info$resources)) {
          if (!is.null(dataset_info$resources[[j]]$format) && 
              tolower(dataset_info$resources[[j]]$format) == format) {
            resource_to_download <- dataset_info$resources[[j]]
            cat("Found preferred format:", format, "\n")
            break
          }
        }
        if (!is.null(resource_to_download)) break
      }
      
      # If no preferred format, just take the first resource with a URL
      if (is.null(resource_to_download)) {
        for (j in seq_along(dataset_info$resources)) {
          if (!is.null(dataset_info$resources[[j]]$url) && 
              dataset_info$resources[[j]]$url != "") {
            resource_to_download <- dataset_info$resources[[j]]
            cat("Using first available resource\n")
            break
          }
        }
      }
      
      # If still no resource, skip this dataset
      if (is.null(resource_to_download)) {
        cat("No suitable resources found for download\n")
        next
      }
      
      # Determine file extension
      resource_format <- if (!is.null(resource_to_download$format) && resource_to_download$format != "") {
        tolower(resource_to_download$format)
      } else {
        "data"
      }
      
      # Clean up format for filename
      resource_format <- gsub("[^a-zA-Z0-9]", "", resource_format)
      if (resource_format == "") resource_format <- "data"
      
      # Download the resource
      resource_url <- resource_to_download$url
      cat("Downloading from URL:", resource_url, "\n")
      
      output_file <- file.path(raw_dir, paste0(safe_name, ".", resource_format))
      
      # Some URLs might need encoding
      encoded_url <- URLencode(resource_url)
      
      # Try to download with error handling
      download_success <- tryCatch({
        # First try with download.file
        download.file(encoded_url, output_file, mode = "wb", quiet = TRUE)
        TRUE
      }, error = function(e) {
        cat("Error with download.file:", e$message, "\n")
        cat("Trying alternative method...\n")
        
        # Try with httr GET as alternative
        tryCatch({
          response <- GET(encoded_url, write_disk(output_file, overwrite = TRUE), timeout(30))
          status_code(response) == 200
        }, error = function(e2) {
          cat("Error with alternative method:", e2$message, "\n")
          FALSE
        })
      })
      
      # Verify download
      if (download_success && file.exists(output_file) && file.size(output_file) > 0) {
        cat("Successfully downloaded. File size:", file.size(output_file), "bytes\n")
        successful_downloads <- successful_downloads + 1
      } else {
        cat("Download failed or file is empty\n")
        # Clean up failed download
        if (file.exists(output_file)) file.remove(output_file)
      }
      
    }, error = function(e) {
      cat("Error processing dataset:", e$message, "\n")
    })
    
    # Add a small delay between requests
    Sys.sleep(1)
  }
  
  # Summary
  cat("\n--- Download Summary ---\n")
  cat("Attempted to download", length(selected_ids), "datasets\n")
  cat("Successfully downloaded", successful_downloads, "datasets\n")
  cat("Files saved to:", normalizePath(raw_dir), "\n")
  cat("Metadata saved to:", normalizePath(metadata_dir), "\n")
  
  return(successful_downloads)
}

# Run the function
discover_and_download(10)


# Debug function to troubleshoot empty downloads
download_single_dataset <- function() {
  # Create a directory for the download
  download_dir <- "opendata_download"
  if (!dir.exists(download_dir)) {
    dir.create(download_dir)
    cat("Created directory:", download_dir, "\n")
  }
  
  # Direct URL to a known dataset (CSV of Swiss measurement stations)
  download_url <- "https://opendata.swiss/dataset/stations-measurements/resource/9089600a-8337-446d-bc86-25fec9293b9c/download/standardmessstationen.csv"
  
  # File to save the download
  output_file <- file.path(download_dir, "measurement_stations.csv")
  
  cat("Attempting to download from:", download_url, "\n")
  cat("Will save to:", output_file, "\n")
  
  # Try to download the file
  download_result <- try(
    download.file(download_url, output_file, mode = "wb"),
    silent = TRUE
  )
  
  # Check if download was successful
  if (inherits(download_result, "try-error")) {
    cat("Error downloading file:", download_result[1], "\n")
    return(FALSE)
  } else {
    # Check if file exists and has content
    if (file.exists(output_file) && file.size(output_file) > 0) {
      cat("Success! Downloaded file to:", normalizePath(output_file), "\n")
      cat("File size:", file.size(output_file), "bytes\n")
      return(TRUE)
    } else {
      cat("Download appears to have failed. File is empty or doesn't exist.\n")
      return(FALSE)
    }
  }
}

# Run the function
download_single_dataset()


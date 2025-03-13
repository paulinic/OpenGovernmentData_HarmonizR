# Simplified Swiss Open Government Data Harmonization Script
# ------------------------------------------------------

# 1. Load required libraries (installing if needed)
required_packages <- c("httr", "jsonlite", "dplyr", "tidyr", "purrr")

# Install missing packages
for(package in required_packages) {
  if(!require(package, character.only = TRUE, quietly = TRUE)) {
    install.packages(package)
    library(package, character.only = TRUE)
  }
}

# 2. Create a simple directory structure to store our data
main_dir <- "swiss_open_data"
dirs <- c("raw", "processed", "metadata")

# Create main directory and subdirectories
for(dir in dirs) {
  dir_path <- file.path(main_dir, dir)
  if(!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
    cat("Created directory:", dir_path, "\n")
  }
}

# 3. Simple function to download data from opendata.swiss API
download_dataset <- function(dataset_id, format = "json") {
  # Construct the URL to get dataset information
  metadata_url <- paste0("https://opendata.swiss/api/3/action/package_show?id=", dataset_id)
  
  # Get dataset metadata
  response <- httr::GET(metadata_url)
  
  # Check if request was successful
  if(httr::status_code(response) == 200) {
    # Parse the JSON response
    dataset_info <- jsonlite::fromJSON(rawToChar(response$content))
    
    # Extract dataset title for naming files
    dataset_name <- gsub("[^a-zA-Z0-9]", "_", dataset_info$result$title)
    
    # Store metadata 
    metadata_file <- file.path(main_dir, "metadata", paste0(dataset_name, "_info.json"))
    jsonlite::write_json(dataset_info$result, metadata_file, pretty = TRUE)
    cat("Saved metadata to:", metadata_file, "\n")
    
    # Try to find a resource in the requested format
    resources <- dataset_info$result$resources
    desired_resource <- NULL
    
    for(i in 1:length(resources)) {
      if(tolower(resources[[i]]$format) == tolower(format)) {
        desired_resource <- resources[[i]]
        break
      }
    }
    
    # If we found a resource in the desired format, download it
    if(!is.null(desired_resource)) {
      download_url <- desired_resource$url
      data_file <- file.path(main_dir, "raw", paste0(dataset_name, ".", tolower(format)))
      
      # Download the data
      download_response <- httr::GET(download_url)
      
      if(httr::status_code(download_response) == 200) {
        writeBin(download_response$content, data_file)
        cat("Downloaded data to:", data_file, "\n")
        
        return(list(
          success = TRUE,
          dataset_name = dataset_name,
          metadata_path = metadata_file,
          data_path = data_file
        ))
      } else {
        cat("Failed to download data. HTTP status:", httr::status_code(download_response), "\n")
      }
    } else {
      cat("No resource found in", format, "format for this dataset.\n")
    }
  } else {
    cat("Failed to get dataset information. HTTP status:", httr::status_code(response), "\n")
  }
  
  return(list(success = FALSE))
}

# 4. Function to search for datasets
search_datasets <- function(query, rows = 10) {
  # URL encode the query
  encoded_query <- URLencode(query)
  
  # Construct the search URL
  search_url <- paste0("https://opendata.swiss/api/3/action/package_search?q=", 
                       encoded_query, "&rows=", rows)
  
  # Make the request
  response <- httr::GET(search_url)
  
  # Check if request was successful
  if(httr::status_code(response) == 200) {
    # Parse the JSON response
    search_results <- jsonlite::fromJSON(rawToChar(response$content))
    
    # Extract and display the results
    results <- search_results$result$results
    
    if(length(results) > 0) {
      # Create a simple data frame with key information
      result_df <- data.frame(
        title = sapply(results, function(x) x$title),
        id = sapply(results, function(x) x$id),
        organization = sapply(results, function(x) x$organization$title),
        stringsAsFactors = FALSE
      )
      
      return(result_df)
    } else {
      cat("No results found for query:", query, "\n")
      return(NULL)
    }
  } else {
    cat("Search request failed. HTTP status:", httr::status_code(response), "\n")
    return(NULL)
  }
}

# 5. Process downloaded data (example for JSON data)
process_json_data <- function(data_path, output_name = NULL) {
  # Check if file exists
  if(!file.exists(data_path)) {
    cat("File does not exist:", data_path, "\n")
    return(NULL)
  }
  
  # Determine output name if not provided
  if(is.null(output_name)) {
    output_name <- tools::file_path_sans_ext(basename(data_path))
  }
  
  # Read the JSON data
  json_data <- try(jsonlite::read_json(data_path), silent = TRUE)
  
  if(inherits(json_data, "try-error")) {
    cat("Error reading JSON file:", data_path, "\n")
    return(NULL)
  }
  
  # Convert to a data frame if possible
  if(is.list(json_data) && !is.data.frame(json_data)) {
    # Try to convert to data frame
    df <- try({
      # If it's a list of lists, try to simplify to a data frame
      if(is.list(json_data[[1]])) {
        jsonlite::fromJSON(data_path, simplifyDataFrame = TRUE)
      } else {
        as.data.frame(json_data)
      }
    }, silent = TRUE)
    
    if(!inherits(df, "try-error") && is.data.frame(df)) {
      # Save as CSV
      output_path <- file.path(main_dir, "processed", paste0(output_name, ".csv"))
      write.csv(df, output_path, row.names = FALSE)
      cat("Saved processed data to:", output_path, "\n")
      return(df)
    } else {
      cat("Could not convert JSON to data frame automatically.\n")
      return(json_data)
    }
  } else if(is.data.frame(json_data)) {
    # If it's already a data frame, save it
    output_path <- file.path(main_dir, "processed", paste0(output_name, ".csv"))
    write.csv(json_data, output_path, row.names = FALSE)
    cat("Saved processed data to:", output_path, "\n")
    return(json_data)
  } else {
    cat("JSON data is not in a format that can be easily processed.\n")
    return(json_data)
  }
}

# 6. Example usage of the functions
# --------------------------------
cat("\nExample: Searching for datasets about 'zurich'\n")
cat("------------------------------------------\n")
zurich_datasets <- search_datasets("zurich", rows = 5)
print(zurich_datasets)

cat("\nTo download a dataset, use the download_dataset function with the dataset ID:\n")
cat("download_dataset(\"your-dataset-id\")\n\n")
cat("For example: download_dataset(\"", ifelse(!is.null(zurich_datasets) && nrow(zurich_datasets) > 0, zurich_datasets$id[1], "dataset-id"), "\")\n\n")

cat("After downloading, process JSON data with: process_json_data(\"path/to/your/data.json\")\n")

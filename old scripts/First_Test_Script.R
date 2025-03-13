---
title: "R Script Data Harmonization of Swiss Open Government Data"
author: "Nicole Pauli"
date: "`r Sys.Date()`"
output: r_script
---

# Load required libraries and install packages
install.packages("httr")

library(httr2)
library(jsonlite)
library(dplyr)
library(tidyr)
library(tidyverse)
library(purrr)
library(readr)

# SET UP THE TEST DATA REPOSITORY

# Create directory structure
dir.create("test_data_repo", recursive = TRUE)
dir.create("test_data_repo/raw", recursive = TRUE)
dir.create("test_data_repo/harmonized", recursive = TRUE)
dir.create("test_data_repo/metadata", recursive = TRUE)

# Get the data from the opendata.swiss API

url <- "https://opendata.swiss/api/3/action/package_list"
response <- httr::GET(url)

# Document the dataset

create_metadata <- function(dataset_name, source_url, language, 
                            gov_level, domain, known_issues) {
  metadata <- list(
    dataset_name = dataset_name,
    source_url = source_url,
    language = language,
    government_level = gov_level,
    domain = domain,
    known_issues = known_issues,
    download_date = Sys.Date()
  )
  
  write_json(metadata, 
             paste0("test_data_repo/metadata/", dataset_name, "_metadata.json"),
             pretty = TRUE)
}

# Download and store raw data

download_dataset <- function(url, dataset_name, format = "json") {
  response <- GET(url)
  writeBin(content(response, "raw"), 
           paste0("test_data_repo/raw/", dataset_name, ".", format))
  
  # Log the download
  write(paste(Sys.time(), "Downloaded:", dataset_name), 
        "test_data_repo/download_log.txt", append = TRUE)
}

# Create a dataset catalog

create_dataset_catalog <- function() {
  # Read all metadata files
  metadata_files <- list.files("test_data_repo/metadata", 
                               pattern = "*.json", 
                               full.names = TRUE)
  
  # Combine into a single catalog
  catalog <- metadata_files %>%
    map(read_json) %>%
    bind_rows()
  
  write_csv(catalog, "test_data_repo/dataset_catalog.csv")
  return(catalog)
}

# Add data set relationship 

  create_dataset_relationship <- function(dataset1, dataset2, relationship_type, notes) {
    relationship <- list(
      dataset1 = dataset1,
      dataset2 = dataset2,
      relationship_type = relationship_type,  # e.g., "same_domain_different_canton"
      notes = notes
    )
    
    # Append to relationships file
    write_json(relationship, "test_data_repo/relationships.json", 
               append = TRUE, pretty = TRUE)
  }

# TEST WITH A SINGLE DATASET
  
  # Load all necessary packages
  library(httr)
  library(jsonlite)
  library(dplyr)
  library(purrr)
  library(readr)  # Added missing package for write_csv
  
  # Create repository structure with error handling
  for (dir in c("test_data_repo", "test_data_repo/raw", "test_data_repo/harmonized", "test_data_repo/metadata")) {
    tryCatch({
      dir.create(dir, recursive = TRUE)
      cat("Created directory:", dir, "\n")
    }, error = function(e) {
      cat("Error creating directory:", dir, "-", e$message, "\n")
    })
  }
  
  # Fetch datasets with economy tag - with better error handling
  fetch_economy_datasets <- function() {
    tryCatch({
      response <- GET("https://opendata.swiss/api/3/action/package_search?fq=tags:economy")
      
      if (status_code(response) != 200) {
        stop("API request failed with status code: ", status_code(response))
      }
      
      data <- content(response, "parsed")
      
      # Debug output to check structure
      cat("API response structure:", names(data), "\n")
      if ("result" %in% names(data)) {
        cat("Result structure:", names(data$result), "\n")
      }
      
      # Safer extraction with checks
      if (!is.null(data$result) && !is.null(data$result$results)) {
        return(data$result$results)
      } else {
        stop("Expected structure not found in API response")
      }
    }, error = function(e) {
      cat("Error in fetch_economy_datasets:", e$message, "\n")
      return(list())
    })
  }
  
  # Process datasets and create metadata
  process_datasets <- function(datasets, limit = 5) {
    # Take a subset to start with
    datasets <- datasets[1:min(limit, length(datasets))]
    
    dataset_catalog <- data.frame()
    
    for (i in seq_along(datasets)) {
      dataset <- datasets[[i]]
      
      # Extract basic information
      dataset_id <- dataset$id
      title <- dataset$title
      organization <- dataset$organization$title
      
      # Find language from metadata
      language <- "Unknown"
      if (length(dataset$language) > 0) {
        language <- paste(dataset$language, collapse = ", ")
      }
      
      # Identify resources (actual data files)
      resources <- dataset$resources
      
      if (length(resources) > 0) {
        # Take the first resource for simplicity
        resource <- resources[[1]]
        
        resource_url <- resource$url
        format <- resource$format
        
        # Create metadata object
        metadata <- list(
          dataset_id = dataset_id,
          title = title,
          organization = organization,
          language = language,
          source_url = resource_url,
          format = format,
          description = dataset$notes,
          keywords = paste(sapply(dataset$tags, function(x) x$name), collapse = ", "),
          download_date = as.character(Sys.Date())
        )
        
        # Add to catalog
        dataset_catalog <- bind_rows(dataset_catalog, as.data.frame(metadata))
        
        # Save metadata
        safe_name <- gsub("[^a-zA-Z0-9]", "_", title)
        write_json(metadata, 
                   paste0("test_data_repo/metadata/", safe_name, "_metadata.json"),
                   pretty = TRUE)
        
        # Download the resource if it's in a common format
        if (tolower(format) %in% c("csv", "json", "xlsx", "xls")) {
          tryCatch({
            response <- GET(resource_url)
            writeBin(content(response, "raw"), 
                     paste0("test_data_repo/raw/", safe_name, ".", tolower(format)))
            
            cat("Downloaded:", title, "\n")
          }, error = function(e) {
            cat("Failed to download:", title, "-", e$message, "\n")
          })
        }
      }
    }
    
    # Save the catalog
    write_csv(dataset_catalog, "test_data_repo/dataset_catalog.csv")
    return(dataset_catalog)
  }
  
  # Main execution
  economy_datasets <- fetch_economy_datasets()
  cat("Found", length(economy_datasets), "economy-tagged datasets\n")
  catalog <- process_datasets(economy_datasets)
  
  # Document relationships between datasets
  identify_relationships <- function(catalog) {
    relationships <- list()
    
    # Group by organization
    orgs <- unique(catalog$organization)
    
    for (org in orgs) {
      org_datasets <- catalog[catalog$organization == org, ]
      
      # If an org has multiple datasets, they might be related
      if (nrow(org_datasets) > 1) {
        for (i in 1:(nrow(org_datasets)-1)) {
          for (j in (i+1):nrow(org_datasets)) {
            relationship <- list(
              dataset1 = org_datasets$title[i],
              dataset2 = org_datasets$title[j],
              relationship_type = "same_organization",
              notes = paste("Both datasets from", org)
            )
            relationships <- c(relationships, list(relationship))
          }
        }
      }
    }
    
    # Write to file if we found any relationships
    if (length(relationships) > 0) {
      write_json(relationships, "test_data_repo/relationships.json", pretty = TRUE)
    }
    
    return(relationships)
  }
  
  relationships <- identify_relationships(catalog)


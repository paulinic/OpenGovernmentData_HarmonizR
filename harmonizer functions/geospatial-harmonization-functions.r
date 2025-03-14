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

#' Read a GeoJSON file
#'
#' Loads a GeoJSON file into an sf object.
#'
#'
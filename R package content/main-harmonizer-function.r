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
#' # Using an inspection report from the inspector package
#' library(inspector)  # A hypothetical companion package
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

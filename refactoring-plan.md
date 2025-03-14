# HarmonizeR Package Refactoring Plan

## R/utils.R
- Logging functions (`log_info`, `log_warning`, `log_error`)
- Field standardization (`standardize_field_name`)
- Helper utilities

## R/loading.R
- `detect_delimiter`
- `load_files`
- `load_inspection_report`

## R/conversion.R
- `convert_to_datetime`
- `convert_to_numeric`
- Other type conversion utilities

## R/geospatial.R
- `parse_wms_capabilities`
- `load_wms`
- `load_wfs`
- `read_geojson`
- `extract_attributes_from_spatial`
- `create_service_description`

## R/harmonization.R
- `generate_harmonization_plan`
- `harmonize_dataframes`
- `enhance_harmonization_plan`

## R/linking.R
- `identify_key_variables`
- `link_datasets`

## R/main.R
- `process_data` (main workflow)

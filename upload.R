# Libraries --------------------------------------------------------------

library(tidyverse)
library(glue)
library(DBI)

# Database Connection ----------------------------------------------------

con <- dbConnect(
	odbc::odbc(),
	# Create Driver
	Driver = "Oracle in OraClient19Home1",
	# Database HOST/DB_NAME
	DBQ = "10.10.3.8:1521/ACDB",
	# Username
	UID = Sys.getenv("DB_USERNAME"),
	# Password
	pwd = Sys.getenv("DB_PASSWORD")
)

# Import Data ------------------------------------------------------------

val_date <- ymd(readline(prompt = "Input Valuation Date (YYYY-MM-DD):"))
input_dir <- here::here("input", format(val_date, "%B %Y"))
prior_suffix <- format(floor_date(val_date, unit = "month") - days(1), "%m%y")
current_suffix <- format(val_date, "%m%y")

system_dict <- sort(c("Aims", "Insis", "Sirius", "Others"))

# fmt: skip
schema_dict <- c(KE_AIMS_GB_PREM = "Aims", KE_SIRIUS_GB_PREM = "Sirius", KE_EMC_GB_PREM = "EMC",KE_EMC_AFYABYM_PREM = "Afya EMC", KE_INSIS_MED_PREM = "Insis", KE_AIMS_MED_PREM = "Aims Medical", KE_INSIS_MED_CLAIMS = "Insis", KE_BMI_AFYABYM_PREM = "Afya BMI", KE_BMI_GB_PREM = "BMI", KE_OLDAIMS_GB_CLAIMS = "Old Aims", KE_NEWAIMS_GB_CLAIMS = "Aims", KE_SIRIUS_GB_CLAIMS = "Sirius")

menu_selection <- function() {
	system <- (system_dict[menu(system_dict, graphics = TRUE, title = "System")])

	data_type <- c("PREM", "CLAIMS")[menu(
		c("Premium", "Claims"),
		graphics = TRUE,
		title = "Data Type"
	)]

	if (system == "Others") {
		other_systems <- toupper(system_dict[system_dict != "Others"])
		# (?!.*(AIMS|SIRIUS|INSIS)); target pattern
		exclude_pattern <- paste0(
			"(?!.*(",
			paste0(other_systems, collapse = "|"),
			"))"
		)
		pattern <- glue("^KE{exclude_pattern}.*{data_type}$")
	} else {
		pattern <- glue("^KE.*{toupper(system)}.*{data_type}$")
	}

	available_tables <- grep(
		pattern = pattern,
		x = dbListTables(con),
		value = TRUE,
		perl = TRUE
	)

	# Some systems only have one table to speak of
	schema_table <- if (length(available_tables) == 1) {
		available_tables
	} else {
		available_tables[menu(available_tables, graphics = TRUE, title = "Table")]
	}

	system <- schema_dict[[schema_table]]

	out <- c(system = system, data_type = data_type, schema_table = schema_table)

	return(out)
}

read_insis <- function(file_path, data_type) {
	if (data_type == "Claims") {
		col_types <- c(
			"text",
			rep("date", 2),
			rep("text", 6),
			"numeric",
			"text",
			rep("date", 3),
			rep("text", 5),
			rep("numeric", 2),
			"date",
			rep("numeric", 2),
			rep("text", 3),
			rep("date", 2)
		)

		sheet_names <- readxl::excel_sheets(path = file_path)
		out <- map(
			.x = sheet_names,
			~ readxl::read_xlsx(path = file_path, sheet = .x, col_types = col_types)
		) |>
			list_rbind() |>
			rename(START_DATE = INSR_BEGIN, END_DATE = INSR_END) |>
			mutate(across(where(is.POSIXt), ~ as_date(.x)))
	}

	return(out)
}

read_aims <- function(file_path, data_type) {
	if (data_type == "Premium") {
		col_types <- c(
			rep("text", 6),
			"numeric",
			rep("text", 8),
			rep("numeric", 6),
			"text",
			rep("numeric", 20),
			rep("text", 6)
		)

		out <- readxl::read_xlsx(path = file_path, col_types = col_types)

		return(out)
	}
}

read_aims_medical <- function(file_path, data_type) {
	if (data_type == "Premium") {
		col_types <- c(
			rep("text", 4),
			rep("numeric", 2),
			rep("text", 3),
			rep("numeric", 7),
			"text"
		)

		out <- readxl::read_xlsx(path = file_path, col_types = col_types)

		return(out)
	}
}

read_sirius <- function(file_path, data_type) {
	if (data_type == "Claims") {
		col_types <- c(
			rep("text", 3),
			rep("date", 2),
			rep("text", 4),
			rep("numeric", 2),
			"date",
			rep("numeric", 4),
			"text"
		)

		out <- readxl::read_xlsx(path = file_path, col_types = col_types)

		return(out)
	}
}

import_data <- function(system, data_type) {
	file_path <- here::here(
		input_dir,
		glue("{system} {data_type}_{current_suffix}.xlsx")
	)

	out <- switch(
		system,
		Insis = read_insis(file_path = file_path, data_type = data_type),
		Aims = read_aims(file_path = file_path, data_type = data_type),
		Sirius = read_sirius(file_path = file_path, data_type = data_type),
		"Aims Medical" = read_aims_medical(
			file_path = file_path,
			data_type = data_type
		),
		cli::cli_abort(message = "The system {.val {system}} is unhandled")
	)

	return(out)
}

update_table <- function(
	new_data,
	system,
	data_type,
	schema_table,
	overwrite = FALSE
) {
	prior_path <- here::here(
		input_dir,
		glue("{system} {data_type}_{prior_suffix}.parquet")
	)

	if (!fs::file_exists(prior_path)) {
		cli::cli_alert_info(text = "Fetching prior data from {.val {schema_table}}")

		tbl(con, dbplyr::in_schema(schema = "WORKSPACE", table = schema_table)) |>
			collect() |>
			nanoparquet::write_parquet(file = prior_path)

		cli::cli_alert_success(text = "Prior data written to {.path {prior_path}}")
	}

	cli::cli_alert_info(text = "Uploading data to {.val {schema_table}}")

	if (!overwrite) {
		dbAppendTable(
			con = con,
			name = Id(schema = "WORKSPACE", table = schema_table),
			value = new_data
		)
	}

	if (overwrite) {
		dbWriteTable(
			con = con,
			name = Id(schema = "WORKSPACE", table = schema_table),
			value = new_data,
			overwrite = TRUE
		)
	}
	cli::cli_alert_success(
		text = "Data upload to {.val {schema_table}} complete."
	)

	beepr::beep()
}

upload_data <- function(overwrite = FALSE) {
	inputs <- menu_selection()
	system <- inputs[["system"]]
	data_type <- c(PREM = "Premium", CLAIMS = "Claims")[[inputs[["data_type"]]]]
	schema_table <- inputs[["schema_table"]]

	# Import New Data
	new_data <- import_data(system = system, data_type = data_type)

	# Update the table
	update_table(
		new_data = new_data,
		system = system,
		data_type = data_type,
		schema_table = schema_table,
		overwrite = overwrite
	)
}

upload_data(overwrite = FALSE)

dbDisconnect(con)

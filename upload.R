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
lookup <- map(
	.x = readxl::excel_sheets(here::here(input_dir, "Lookup.xlsx")),
	.f = \(sheet_names) {
		{
			readxl::read_xlsx(
				path = here::here(input_dir, "Lookup.xlsx"),
				sheet = sheet_names
			)
		} |>
			janitor::clean_names()
	}
) |>
	set_names(nm = readxl::excel_sheets(here::here(input_dir, "Lookup.xlsx"))) |>
	janitor::clean_names()


system_dict <- sort(c("Aims", "Insis", "Sirius", "Others", "Equity PTD"))

# fmt: skip
schema_dict <- c(KE_AIMS_GB_PREM = "Aims", KE_SIRIUS_GB_PREM = "Sirius", KE_EMC_GB_PREM = "EMC",KE_EMC_AFYABYM_PREM = "Afya EMC", KE_INSIS_MED_PREM = "Insis", KE_AIMS_MED_PREM = "Aims Medical", KE_INSIS_MED_CLAIMS = "Insis", KE_BMI_AFYABYM_PREM = "Afya BMI", KE_BMI_GB_PREM = "BMI", KE_OLDAIMS_GB_CLAIMS = "Old Aims", KE_NEWAIMS_GB_CLAIMS = "Aims", KE_SIRIUS_GB_CLAIMS = "Sirius", EQUITY_PTD_PREM = "Equity PTD")

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
	} else if (system == "Equity PTD") {
		pattern <- "EQUITY_PTD_PREM"
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

# Standardise characters in columns
standardise_names <- function(.data, .column) {
	.data <- .data |>
		mutate(
			# remove special characters
			{{ .column }} := gsub(
				pattern = "[[:punct:]]",
				replacement = '',
				x = {{ .column }}
			),
			# remove brackets
			{{ .column }} := gsub(
				pattern = "[()]",
				replacement = '',
				x = {{ .column }}
			),
			# replace spaces with one space only
			{{ .column }} := gsub(
				pattern = "\\s+",
				replacement = ' ',
				x = {{ .column }}
			)
		)

	return(.data)
}

# Classification function
classify_med <- function(data, insurance_column, agent_column, scheme_column) {
	data <- data |>
		mutate(
			SCHEME_NAME = !!sym(scheme_column),
			AGENCY_NAME = !!sym(agent_column)
		) |>
		standardise_names(.column = SCHEME_NAME) |>
		standardise_names(.column = AGENCY_NAME)

	# Change agent and scheme columns
	agent_column <- "AGENCY_NAME"
	scheme_column <- "SCHEME_NAME"

	out <- tibble(
		# check whether it is retail/corporate/bancassurance
		class = if_else(
			data[[insurance_column]] == "Britam Pandemic Cover",
			"Britam Pandemic Cover",
			if_else(
				# check for bancassurance agents while simulateneously standardising the names
				data[[agent_column]] %in% lookup$agent_names$agent,
				stringi::stri_replace_all_regex(
					str = data[[agent_column]],
					pattern = paste0("^", lookup$agent_names$agent, "$"),
					replacement = lookup$agent_names$cleaned_agent,
					vectorize_all = FALSE
				),
				if_else(
					# anything with milele in the name is a retail policy
					grepl("Milele", data[[insurance_column]], ignore.case = TRUE),
					"Britam Milele",
					# everything else is assumed to be a corporate policy
					"Group Medical"
				)
			)
		)
	) |>
		mutate(
			# work on the corporate schemes; standardise the names
			cleaned_scheme_name = if_else(
				class == "Group Medical",
				if_else(
					data[[scheme_column]] %in% lookup$scheme_remaning$scheme_name,
					stringi::stri_replace_all_regex(
						str = data[[scheme_column]],
						pattern = paste0("^", lookup$scheme_remaning$scheme_name, "$"),
						replacement = lookup$scheme_remaning$cleaned_scheme_name,
						vectorize_all = FALSE
					),
					data[[scheme_column]]
				),
				class
			),
			chann = if_else(
				class %in% lookup$agent_names$cleaned_agent,
				"P&D",
				if_else(
					class == "Britam Milele" | class == "Britam Pandemic Cover",
					"Retail",
					if_else(class == "Group Medical", "Corporate", "Corporate"),
					missing = "Corporate"
				)
			),
			agency_name = if_else(
				class %in% lookup$agent_names$cleaned_agent,
				class,
				data[[agent_column]]
			)
		) |>
		rename(
			CLASS_DESCRIPTION = class,
			SCHEME_NAME = cleaned_scheme_name,
			CHANN = chann,
			AGENCY_NAME = agency_name
		) |>
		select(CHANN, CLASS_DESCRIPTION, AGENCY_NAME, SCHEME_NAME)

	out <- data |> select(-SCHEME_NAME, -AGENCY_NAME) |> bind_cols(out)

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

	if (data_type == "Premium") {
		col_types <- c(
			rep("text", 3),
			rep("numeric", 2),
			"text",
			rep("numeric", 2),
			"text",
			"numeric",
			rep("text", 3),
			"numeric",
			"text",
			rep("numeric", 12)
		)

		out <- readxl::read_xlsx(path = file_path, col_types = col_types) |>
			classify_med(
				insurance_column = "INSURANCE_TYPE",
				agent_column = "AGENCY_NAME_RAW",
				scheme_column = "CLIENT_NAME"
			)
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
	}

	if (data_type == "Claims") {
		col_types <- c(
			rep("text", 6),
			rep("numeric", 3),
			rep("text", 2),
			rep("date", 2),
			"text",
			rep("date", 2),
			"guess",
			rep("date", 2),
			"text",
			"numeric",
			rep("text", 3),
			rep("numeric", 16),
			rep("text", 10),
			rep("numeric", 2),
			"text",
			rep("numeric", 2),
			rep("text", 2)
		)

		out <- readxl::read_xlsx(path = file_path, col_types = col_types)
	}

	return(out)
}

read_old_aims <- function(file_path, data_type) {
	if (data_type == "Claims") {
		col_types <- c(
			rep("text", 6),
			rep("numeric", 3),
			rep("text", 2),
			rep("date", 2),
			"text",
			rep("date", 5),
			"text",
			"numeric",
			rep("text", 3),
			rep("numeric", 11),
			rep("text", 8)
		)

		out <- readxl::read_xlsx(path = file_path, col_types = col_types)
	}

	return(out)
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

read_equity <- function(file_path, data_type) {
	if (data_type == "Premium") {
		col_types <- c(
			"text",
			"numeric",
			rep("text", 3),
			"numeric",
			rep("text", 2),
			rep("numeric", 9)
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
		"Old Aims" = read_old_aims(file_path = file_path, data_type = data_type),
		"Equity PTD" = read_equity(file_path = file_path, data_type = data_type),
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

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/upseRt.R
\name{update_table}
\alias{update_table}
\title{Perform updates on specified table using provided data frame
The records are assumed to exist. The function will fail if they don't exist}
\usage{
update_table(con, df, table, pk_fields, set_fields,
  explicit_type_cast_list = NULL)
}
\arguments{
\item{con}{A DBI connection object}

\item{df}{Dataframe to upsert}

\item{table}{Name of table in database to upsert to}

\item{pk_fields}{Fields in the table that uniquely identify a row}

\item{set_fields}{Field in the table that should be set by the upsert operation}
}
\value{
Return value indicating success or failure of the upsert operation
}
\description{
Perform updates on specified table using provided data frame
The records are assumed to exist. The function will fail if they don't exist
}


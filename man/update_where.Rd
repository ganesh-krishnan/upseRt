% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/upseRt.R
\name{update_where}
\alias{update_where}
\title{Create a WHERE clause for use in a SQL UPDATE statement.}
\usage{
update_where(table, pk_fields, explicit_type_cast_list = NULL)
}
\arguments{
\item{table}{A string naming the target table.}

\item{pk_fields}{A character vector naming the primary key fields.}

\item{postgres_types}{A character vector with values of either NA or the 
desired Postgresql data type e.g. "text", "date". Matched to df by position.
It is assumed that the primary keys are matched to the first set of values in
postgres_types (which could also refer to the value fields).}
}
\value{
A string with a where clause appropriate for a SQL UPDATE statement.
}
\description{
This is used to create a WHERE clause to be used in an UPDATE statement
}
\examples{
update_where("db_table", c("A", "B"), c(NA, "date", NA, "text"))
}


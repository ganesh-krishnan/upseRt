#' Prepend string to each element of character vector
#'
#' @param char_vector A character vector
#' @param string_to_prepend String to prepend
#' @return Character vector with string prepended to each element
prepend_string <- function(char_vector, string_to_prepend) {
    if (length(string_to_prepend)==1) {
        return(paste0(string_to_prepend, char_vector))
    } else {
        warning("String to prepend should be character vector of length 1. Using first element")
        return(paste0(string_to_prepend[1], char_vector))
    }
}

#' Append string to each element of character vector
#'
#' @param char_vector A character vector
#' @param string_to_prepend String to prepend
#' @return Character vector with string appended to each element
append_string <- function(char_vector, string_to_append) {
    if (length(string_to_append)==1) {
        return(paste0(char_vector, string_to_append))
    } else {
        warning("String to append should be character vector of length 1. Using first element")
        return(paste0(char_vector, string_to_append[1]))
    }
}

#' Wrap single quote around string
#'
#' @param char_vector A character vector
#' @return Character vector with each element wrapped in single quotes
quote_string <- function(char_vector) {
    return(prepend_string(char_vector, "'") %>%
           append_string("'"))
}

#' Wrap single quotes around values for character, Date, factor, ordered
#' factor fields and POSIXct date or date/time vectors
#'
#' Useful for creating SQL strings.
#' 
#' @param x A vector
#' @param explicit_type_cast A type cast specification (specify :: if required)
#' @return The same vector with single quotes around values if needed e.g. 
#' to use in a SQL statement. Type casts for most standard types will be automatically
#' performed. If a custom type cast is desired, it can be specified
#' @examples
#' cast_and_quote(CO2$Treatment, "::varchar") # Explicit type cast
cast_and_quote <- function(x, explicit_type_cast = NULL) {

    if (is.null(explicit_type_cast)) {
        quote_list = list(
            'character' = NULL,
            'Date' = '::date',
            'factor' = NULL,
            'orderedfactor' = NULL,
            'POSIXct' = '::timestamp',
            'POSIXctPOSIXt' = '::timestamp')
    } else {
        quote_list <- list(explicit_type_cast)
        names(quote_list) <- class(x)
    }
    
    ret <- x

    if (any(class(x) %in% names(quote_list))) {
        ret <- quote_string(ret)
        matching_class <- class(x)[class(x) %in% names(quote_list)]

        ret <- paste0(ret, quote_list[[matching_class]])
    }

    return(ret)
}

#' Wrap single quotes around data frame columns and cast appropriately
#' 
#' Useful for creating SQL strings.
#' 
#' @param x A vector
#' @param explicit_type_cast A type cast specification (specify :: if required)
#' @return The same vector with single quotes around values if needed e.g. 
#' to use in a SQL statement. Type casts for most standard types will be automatically
#' performed. If a custom type cast is desired, it can be specified
#' @examples
#' cast_and_quote(CO2$Treatment, "::varchar") # Explicit type cast
cast_and_quote_df <- function(df, explicit_type_cast_list = NULL) {
    if (!is.null(explicit_type_cast_list))
        explicit_vars <- names(explicit_type_cast_list)
    else
        explicit_vars <- c()

    other_vars <- setdiff(names(df), explicit_vars)

    if (length(explicit_vars) > 0) {
        df_explicit <- select_(df, .dots = explicit_vars) %>%
            map2(explicit_type_cast_list, ~ cast_and_quote(.x, .y)) %>%
            bind_cols()
    } else
        df_explicit <- df[0,]

    if (length(other_vars) > 0) {
        df_other <- select_(df, .dots = other_vars) %>%
            map(~ cast_and_quote(.x)) %>%
            bind_cols()
    } else
        df_other <- df[0,]
    
    if (nrow(df_explicit) == 0)
        df_relevant <- df_other
    else if (nrow(df_other) == 0)
        df_relevant <- df_explicit
    else if (nrow(df_explicit) == nrow(df_other))
        df_relevant <- bind_cols(df_other, df_explicit)
    else stop("Could not quote and cast dataframe")
}

#' Create a set of key-value pair strings for use in constructing SQL queries.
#'
#' Intended for use in creating WHERE statments as well as UPSERT operations.
#' 
#' @param df A data frame
#' @param fields A string identifying which field in the data frame to be used
#' @return A character vector of key-value pairs
#' @examples
#' key_value_pairs(CO2, "Treatment")
#' key_value_pairs(CO2, "uptake")
key_value_pairs_for_sql <- function(df, fields, explicit_type_cast_list = NULL) {
    df_relevant <- tbl_df(df) %>%
        cast_and_quote_df(explicit_type_cast_list) %>%
        select_(.dots = fields) 


    kv_pairs <- map2(names(df_relevant), df_relevant, ~paste0(.x, " = ", .y))
    names(kv_pairs) <- names(df_relevant)

    kv_pairs <- as.data.frame(kv_pairs)
    return(kv_pairs)
}

#' Create a SQL WHERE clause defining table rows matching a given data frame.
#'
#' Creates a WHERE clause which can be added to a SQL statement such as
#' SELECT or UPDATE in order to define rows in a database as matching those
#' in a given data frame.  Useful for creating custom UPSERT operations. Will
#' wrap single quotes around values for character, Date, factor and ordered
#' factor fields.
#' 
#' @param df A data frame containing primary keys with which to query the db.
#' @param pk_fields A character vector naming the fields which comprise a
#' primary key for selecting matching records in a SQL database table.
#' @return A string with a SQL WHERE clause describing matching records.
#' @examples
#' create_matching_where(CO2, c("Plant", "Type", "Treatment", "conc"))
create_matching_where <- function(df, pk_fields, explicit_type_cast_list = NULL) {

    kv_df <- key_value_pairs_for_sql(df, pk_fields, explicit_type_cast_list)

    row_clauses <- apply(kv_df, 1, function(x) {
        paste0(x, collapse = ' and ')
    })

    all_clauses <- paste("(", row_clauses, ")", sep = "", collapse = " or ")
    paste("WHERE", all_clauses)
}


#' Return sets of primary keys from a db matching primary keys in a data frame.
#' 
#' @param con A connection object.
#' @param df A data frame.
#' @param table A string naming the target table in the database.
#' @param pk_fields A character vector naming the fields which comprise a
#' primary key for selecting matching records in a SQL database table.
#' @return A data frame of matching primary key fields from the database with row counts
get_matching_records <- function(con, df, table, pk_fields, explicit_type_cast_list = NULL) {
  check_where <- create_matching_where(df, pk_fields, explicit_type_cast_list)
  qry_check <- paste("SELECT", 
                     paste(pk_fields, collapse = ", "),
                     ", COUNT(*) as count",
                     "FROM", 
                     table, 
                     check_where,
                     "GROUP BY ",
                     paste(pk_fields, collapse = ", ")
  )
  dbGetQuery(con, qry_check)
}


#' Match records in a data frame with db records by primary keys batch by batch.
#' 
#' Gets around the stack depth limit encountered when passing a very long WHERE
#' clause to SQL. Useful for planning an upsert operation (esp. separating 
#' updates from inserts).
#' 
#' @param con A connection object.
#' @param import A data frame.
#' @param batch_size The number of records to match at once.
#' @param ... Additional arguments to pass to get_matching_records.
#' @return A data frame of row identifier keys found in both the df and the database with
#' the row counts for each combination in the db
matching_records_builder <- function(con, df, table, pk_fields, explicit_type_cast_list = NULL, batch_size = 1000, 
                                     verbose = FALSE) {
  last_row <- nrow(df)
  if (last_row <= batch_size) {
    return(get_matching_records(con, df, table, pk_fields, explicit_type_cast_list))
  }
  
  if(verbose) {print(paste(
    "Retrieving up to", last_row, "matching records", batch_size, "at a time"
  ))}
  matching_records <- get_matching_records(con, df[1:batch_size,], table, pk_fields, explicit_type_cast_list)
  i <- batch_size + 1
  while(i < last_row) {
    next_range <- i:min(i+batch_size-1, last_row)
    if(verbose) {print(c(min(next_range), max(next_range)))}
    matching_records <- rbind(matching_records, get_matching_records(con, df[next_range,], table, pk_fields, explicit_type_cast_list))
    i <- i + batch_size
  }
  matching_records
}


#' Create a WHERE clause for use in a SQL UPDATE statement.
#'
#' This is used to create a WHERE clause to be used in an UPDATE statement
#' 
#' @param table A string naming the target table.
#' @param pk_fields A character vector naming the primary key fields.
#' @param postgres_types A character vector with values of either NA or the 
#' desired Postgresql data type e.g. "text", "date". Matched to df by position.
#' It is assumed that the primary keys are matched to the first set of values in
#' postgres_types (which could also refer to the value fields).
#' @return A string with a where clause appropriate for a SQL UPDATE statement.
#' @examples
#' update_where("db_table", c("A", "B"), c(NA, "date", NA, "text"))
update_where <- function(table, pk_fields, explicit_type_cast_list = NULL) {
  
  explicit_vars <- names(explicit_type_cast_list)
  other_vars <- setdiff(pk_fields, explicit_vars)  
  explicit_pk_fields <- c()

  if (length(explicit_vars) > 0)
      explicit_pk_fields <- map2(explicit_vars, explicit_type_cast_list, ~ paste0(.x, .y))
  newvals_pk_fields <- c(other_vars, explicit_pk_fields)

  orig_pk_fields <- c(other_vars, explicit_vars)

  paste("WHERE",
        paste(paste0(table, ".", orig_pk_fields),
              "=",
              paste0("newvals.", newvals_pk_fields), 
              collapse = " AND "
        )
  )
}

#' Create a set of tuples from rows in a data frame.
#' 
#' Intended for use in creating a SQL update statement. Set postgres_types only
#' if needed to match data type esp. for dates, timestamps.
#' See format at http://www.postgresql.org/docs/current/static/sql-values.html.
#' See also http://www.postgresql.org/docs/9.4/static/datatype.html for types.
#' 
#' @param df A data frame.
#' @param ... Additional arguments esp. postgres_types
#' @return A single string with a comma-separated list of values tuples.
#' @examples write_values_tuples(head(CO2, 10), c(NA, NA, NA, "text", NA))
write_values_tuples <- function(df, explicit_type_cast_list = NULL) {
  df <- cast_and_quote_df(df, explicit_type_cast_list)
  
  tuples <- paste0("(", apply(df, 1, paste0, collapse = ", "), ")")
  paste0(tuples, collapse = ", ")
}

#' Create a SQL UPDATE statement from a data frame.
#'
#' Useful for creating custom UPSERT operations. Will wrap single quotes around 
#' values for character, Date, factor and ordered factor fields.
#' Implements answer to http://stackoverflow.com/questions/18797608.
#' Currently converts all fields to text in the where clause to obviate need
#' to specify types for Postgres.
#' 
#' @param df A data frame containing the primary keys and values for the update.
#' @param table A string naming the target table in the database.
#' @param pk_fields A character vector naming the fields which comprise a
#' primary key for selecting matching records in a SQL database table.
#' @param set_fields A character vector naming the fields to be updated.
#' @param ... Additional arguments esp. postgres_types
#' @return A string comprising a SQL update statement.
#' @examples
#' write_update_statement(CO2, "co2", c("Plant", "Type"), c("conc", "uptake"))
#' write_update_statement(CO2, "co2", c("Type"), c("uptake"), c("TEXT", NA))
write_update_statement <- function(df, table, pk_fields, set_fields, explicit_type_cast_list = NULL) {
  # create pairs of value colums- one for each of the set_fields
  match_list <- paste(paste(set_fields, "=", 
                            paste0("newvals.", set_fields)
  ),
  collapse = ", ")
  
  all_cols <- names(cast_and_quote_df(df[1, c(pk_fields, set_fields)], explicit_type_cast_list))
  
  values <- write_values_tuples(df[, all_cols], explicit_type_cast_list)
  
  combined_col_names <- paste(all_cols, collapse = ", ")
  
  where_type_cast_vars <- intersect(names(explicit_type_cast_list), pk_fields)
  where_list <- update_where(table, pk_fields, explicit_type_cast_list[where_type_cast_vars])
  
  paste0("UPDATE ", table,
         " SET ", match_list,
         " FROM (VALUES ", values,
         ") as newvals(", combined_col_names,
         ") ", where_list)
}

#' Perform updates on specified table using provided data frame
#' The records are assumed to exist. The function will fail if they don't exist
#'
#' @param con A DBI connection object
#' @param df Dataframe to upsert
#' @param table Name of table in database to upsert to
#' @param pk_fields Fields in the table that uniquely identify a row
#' @param set_fields Field in the table that should be set by the upsert operation
#' @return Return value indicating success or failure of the upsert operation
update_table <- function(con, df, table, pk_fields, set_fields, explicit_type_cast_list = NULL) {
    update_statement <- write_update_statement(df, table, pk_fields, set_fields, explicit_type_cast_list)

    tryCatch({
        status <- dbGetQuery(con, update_statement)
    }, error = function(e) {
        stop("Could not update table. Stopping")
    })

    return(status)
}

#' Perform upserts on specified table using provided data frame
#' The records are assumed to exist. If they do not exist, the statement will fail
#'
#' @param con A DBI connection object
#' @param df Dataframe to upsert
#' @param table Name of table in database to upsert to
#' @param pk_fields Fields in the table that uniquely identify a row
#' @param set_fields Field in the table that should be set by the upsert operation
#' @param explicit_type_cast_list A list of kv pairs indicating if any columns need special casting (see examples)
#' @param batch_size Number of rows to perform update on in one operation (too many rows will cause stack overflow)
#' @param type Operation type, one of "upsert", "update_only" or "insert_only"
#' @return Return value indicating success or failure of the upsert operation
#' @examples
#' # With explicit type cast
#' upsert(con, df, table = "staffing_levels", pk_fields = c("zone_id", "date", "local_start_time", "shift_type"),
#'   set_fields = c("num_shoppers"), explicit_type_cast_list = list(local_start_time = "::time"))
#'
#' #Without explicit type cast
#' upsert(con, df, table = "demand_levels", pk_fields = c("zone_id", "date", "forecast_date"),
#'   set_fields = c("demand"))
#' @export
upsert <- function(con, df, table, pk_fields, set_fields, explicit_type_cast_list = list(),
                   batch_size = 1000, type = "upsert") {
    if (!any(type %in% c("upsert", "update_only", "insert_only")))
        stop("Operation type should be upsert, update_only or insert_only")

    if (nrow(df) == 0) return(NULL)
    
    matching_records <- matching_records_builder(con = con,
                                                 df = df,
                                                 explicit_type_cast_list = explicit_type_cast_list,
                                                 batch_size = batch_size,
                                                 verbose = FALSE,
                                                 table = table,
                                                 pk_fields = pk_fields
                                                 )

    if (any(matching_records$count > 1)) stop("Primary key fields do not uniquely identify row. Upsert aborted")
    
    ## create update list
    if (nrow(matching_records) == 0) {
        df_updates <- df[0,]
    } else {
        df_updates <- semi_join(df, matching_records, by = pk_fields)
    }

    ## create insert list
    df_inserts <- anti_join(df, df_updates, by = pk_fields)

    ## insert records not already there
    if (nrow(df_inserts) > 0 & (type == "upsert" | type == "insert_only")) {
        dbWriteTable2_fast_concurrent(con, table, df_inserts, append=TRUE, row.names=FALSE)
    }

    ## update records already there
    if (nrow(df_updates) > 0 & (type == "upsert" | type == "update_only")) {

        last_row <- nrow(df_updates)

        if (last_row <= batch_size) {
            update_table(con, df_updates, table, pk_fields, set_fields, explicit_type_cast_list)
        } else {
            update_table(con, df_updates[1:batch_size,], table, pk_fields, set_fields, explicit_type_cast_list)

            i <- batch_size + 1

            while (i < last_row) {
                next_range <- i:min(i+batch_size-1, last_row)
                update_table(con, df_updates[next_range,], table, pk_fields, set_fields, explicit_type_cast_list)
                i <- i + batch_size
            }
        }
    }

}

#' Write table to postgres database with auto-incrementing primary key
#'
#' @param con A DBI connection object
#' @param df Dataframe to write
#' @param fill.null Should NAs be filled?
#' @param add.id Should ids be added?
#' @param row.names Should row names be written?
#' @param pg.update.seq Should the serial sequencer be updated for postgres tables
#' @return Return value indicating succss or failure
dbWriteTable2_fast_concurrent <- function (con, table.name, df, fill.null = TRUE, add.id = TRUE, 
                                           row.names = FALSE, pg.update.seq = TRUE, ...) 
{
    if (nrow(df)==0) {
        message("No rows in data frame to insert")
        return(TRUE)
    }
    
    sample.row <- dbGetQuery(con, paste("SELECT * FROM", table.name, "LIMIT 1"))
    if( length(sample.row) >0 ){
        fields <- colnames( sample.row )
    }else{
        fields <- dbListFields(con, table.name)
        fields <- fields[!grepl("\\.\\.pg\\.dropped", fields)]    
    }

    if (add.id) {
        if(pg.update.seq){
            table.seq <- paste0("pg_get_serial_sequence('", table.name, "', 'id')")
            if (class(con) == "PostgreSQLConnection") {
                sql <- sprintf("SELECT nextval(%s) AS id FROM generate_series(1, %d)", table.seq, nrow(df))

                res <- tryCatch({
                    dbGetQuery(con, sql)
                }, err=function(e) {
                    stop("Could not insert ID column")
                }) %>%
                    .$id

                df$id <- res
                
            }
            else {
                stop("pg.update.seq=TRUE flag not compatable with database connection type")
            }
        } else {
            last.id.list <- dbGetQuery(con, paste("SELECT id FROM", 
                                                   table.name, "ORDER BY id DESC LIMIT 1"))
            if (length(last.id.list) == 0) 
                n <- 0
            else n <- last.id.list[[1]]
            df$id <- 1:nrow(df) + n
        }
    }
    
    names(df) <- tolower(names(df))
    names(df) <- gsub("\\.", "_", names(df))
    clmn.match <- match(names(df), fields)
    if (any(is.na(clmn.match))) 
        warning(paste("Found '", names(df)[is.na(clmn.match)], 
                      "' not in fields of '", table.name, "' table. Omiting.\n", 
                      sep = ""))
    field.match <- match(fields, names(df))
    if (sum(is.na(field.match)) > 0 & fill.null == TRUE) {
        message("creating NAs/NULLs for for fields of table that are missing in your df")
        nl <- as.list(rep(NA, sum(is.na(field.match))))
        df.nms.orgnl <- names(df)
        df <- cbind(df, nl)
        names(df) <- c(df.nms.orgnl, fields[is.na(field.match)])
    }
    reordered.names <- names(df)[match(fields, names(df))]
    if (any(is.na(reordered.names))) 
        stop("Too many unmatched columns to database column list. Stopping")
    df <- df[, reordered.names]
    r <- dbSendQuery(con, paste("SELECT * FROM", table.name, 
                                "ORDER BY id DESC LIMIT 1"))
    db.col.info <- dbColumnInfo(r)
    rownames(db.col.info) <- db.col.info$name
    null.OK <- caroline::nv(db.col.info, "nullOK")
    reqd.fields <- names(null.OK[!null.OK])
    na.cols <- sapply(df, function(x) any(is.na(x)))
    req.miss <- na.cols[reqd.fields]
    if (any(req.miss)) 
        stop(paste("Didn't load df because required field(s)", 
                   paste(names(req.miss)[req.miss], collapse = ", "), 
                   "contained missing values"))
    db.precisions <- caroline::nv(db.col.info, "precision")
    df.nchars <- sapply(df, function(c) max(nchar(c)))
    prec.reqd <- db.precisions > 0
    too.long <- db.precisions[prec.reqd] < df.nchars[prec.reqd]
    db.sclasses <- caroline::nv(db.col.info, "Sclass")
    df.classes <- sapply(df, class)
    type.mismatches <- names(df.classes)[db.sclasses != df.classes & 
                                         !na.cols]
    dbClearResult(r)
    print(paste("loading", table.name, "table to database"))
    db.write <- dbWriteTable(con, table.name, df, row.names = row.names, 
                             ...)

    if (db.write & add.id) 
        invisible(df$id)
    else return(db.write)
}

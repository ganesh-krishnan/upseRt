**upseRt**

upseRt is an R package that allows you to perform single shot, transparent upserts to Postgres tables.

A fairly typical use case that R developers interfacing with databases typically deal with involves upsertions.
You have a data frame and a corresponding table that you want to write the data frame to. However, some rows in the 
dataframe already exist in the table, while others don't. In other words, what you need is an upsert - UPdate the 
rows that should be updated and inSERT the rows that should be inserted.

This package makes upsertions very easy. You only need to call one function - upseRt. The function will transparently
take care of most details for you. It will type cast most common R types into corresponding Postgres types. It will then
check which rows need updation. It does this via fields that you specify would uniquely identify a row. These fields
are called primary key fields. It will ensure that the fields you specify do uniquely identify a row. If they do not,
it will not proceed with the upseRt. Once it determines pre-existing records, it will update the fields that you specify
and write the new records into the table.

**Features**

* Transparent single shot, upsertions
* Automatic type casting for most common R types into corresponding Postgres types
* Sanity check to ensure that specified primary keys, indeed uniquely identify a row
* Support for auto-incrementing keys in Postgres

**Examples**

*With explicit type casts*
```
upsert(con, df, table = "test_table", 
  pk_fields = c("region", "date", "local_time", "type"),
  set_fields = c("value"), 
  explicit_type_cast_list = list(local_time = "::time"))`
```

*Without explicit type casts*
```
upsert(con, df, table = "test_table2", 
  pk_fields = c("region, "date", "forecast_date"),
  set_fields = c("demand"))
```

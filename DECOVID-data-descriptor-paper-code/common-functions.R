dbGetQuerySchema <- function(db, schema, query){
  set_schema_sql <- paste0("ALTER USER \"",
                           db_user_name_or_group,
                          "\" with DEFAULT_SCHEMA=",
                          schema)
  dbExecute(db, set_schema_sql)
  dbGetQuery(db, query) %>%
    as_tibble %>% # to handle 0-row case
    mutate(schema = schema)
}

# Run a SQL query across multiple schemas, and take the union of the results
dbGetQueryBothTrusts <- function(db, query, schemas = c("uhb", "uclh")){
  results <- lapply(schemas,
                    FUN = function(schema, db, query){
                      dbGetQuerySchema(db, schema, query)
                    },
                    db = db,
                    query = query)
  do.call("rbind", results)
}

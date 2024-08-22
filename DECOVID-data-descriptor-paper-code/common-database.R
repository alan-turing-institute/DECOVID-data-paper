#Enter information for the database
host <- rstudioapi::askForPassword(prompt="Please enter server/host")
database <- rstudioapi::askForPassword(prompt="Please enter database name")

# this is used for changing the default schema
db_user_name_or_group <- rstudioapi::askForPassword(prompt="Please enter database username or group")

db <- DBI::dbConnect(
  odbc::odbc(),
  driver="ODBC Driver 17 for SQL Server",
  Authentication="ActiveDirectoryInteractive",
  server=host,
  database=database
)
print("STATUS: connected to database")

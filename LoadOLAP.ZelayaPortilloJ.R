# Julissa Zelaya Portillo
# CS5200 Spring 2024
# Practicum II
# Create Star/Snowflake Schema

## Libraries

# Package names
packages <- c("RMySQL", "DBI", "RSQLite", "dplyr")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(!installed_packages)) {
  install.packages(packages[!installed_packages])
}

# Packages loading
invisible(lapply(packages, function(pkg) {
  library(pkg, character.only = TRUE)
}))


## Connection to SQLite and MYSQL Databases

# Database connection 
db_host <- "sql5.freemysqlhosting.net"
db_user <- "sql5699023"
db_password <- "MiN2PMaGD1"
db_name <- "sql5699023"
db_port <- 3306

# Establish connection
mysql <- dbConnect(
  RMySQL::MySQL(),
  dbname = db_name,
  host = db_host,
  port = db_port,
  user = db_user,
  password = db_password
)

# Connect to SQLite database
con <- dbConnect(SQLite(), "pharma_sales.db")

## Create Table Structure for MYSQL

# Create TimeDimension table in MYSQL
table_execution_time <- dbExecute(con, "
CREATE TABLE IF NOT EXISTS TimeDimension (
  dateKey DATE PRIMARY KEY,
  year YEAR,
  quarter TINYINT
);
")

# Create product_facts table in MYSQL
table_execution_products <- dbExecute(con, "
CREATE TABLE IF NOT EXISTS product_facts (
  productID INTEGER,
  productName TEXT,
  year YEAR,
  quarter TINYINT,
  territory TEXT,
  totalSalesAmount DECIMAL(10, 2),
  totalUnitsSold INTEGER,
  PRIMARY KEY (productID, year, quarter, territory),
  FOREIGN KEY (productID) REFERENCES Products(productID),
  FOREIGN KEY (year, quarter) REFERENCES TimeDimension(year, quarter)
);
")

# Create  rep_facts table in MySQL
dbExecute(con, "
CREATE TABLE IF NOT EXISTS rep_facts (
  repID INTEGER,
  repName TEXT,
  year YEAR,
  quarter TINYINT,
  totalSales DECIMAL(10, 2),
  averageSales DECIMAL(10, 2),
  PRIMARY KEY (repID, year, quarter),
  FOREIGN KEY (repID) REFERENCES Reps(repID)
);
")


## Extract Data from SQLite Database

# Extract data from all SQLite tables
sales_data <- dbGetQuery(con, "
SELECT 
  s.date, 
  p.productID, 
  p.product, 
  r.repID, 
  r.name AS repName,
  r.territory,
  s.total,
  s.qty AS unitsSold
FROM Sales s
JOIN Products p ON p.productID = s.productID
JOIN Reps r ON r.repID = s.repID
")

# Extract customers and their country
customer_data <- dbGetQuery(con, "
SELECT 
  c.customerID, c.country
FROM Customers c
")

# Add year and quarter for time dimension using base R
sales_data$date <- as.Date(sales_data$date)
sales_data$year <- as.integer(format(sales_data$date, "%Y"))
sales_data$quarter <- as.integer((as.integer(format(sales_data$date, "%m")) - 1) / 3 + 1)

# Prepare TimeDimension data
time_data <- unique(sales_data %>% select(dateKey = date, year, quarter))

# Write to TimeDimension table
dbWriteTable(mysql, "TimeDimension", time_data, append = TRUE, overwrite = FALSE, row.names = FALSE)

# Write to product_facts table
product_facts_data <- sales_data %>%
  group_by(productID, productName = product, year, quarter, territory) %>%
  summarise(totalSalesAmount = sum(total), 
            totalUnitsSold = sum(unitsSold), 
            .groups = 'drop')

dbWriteTable(mysql, "product_facts", product_facts_data, append = TRUE, overwrite = FALSE, row.names = FALSE)

# Write to rep_facts table
rep_facts_data <- sales_data %>%
  group_by(repID, repName, year, quarter) %>%
  summarise(totalSales = sum(total), 
            averageSales = mean(total), 
            .groups = 'drop')

dbWriteTable(mysql, "rep_facts", rep_facts_data, append = TRUE, overwrite = FALSE, row.names = FALSE)

# Close database connections
dbDisconnect(con)
dbDisconnect(mysql)

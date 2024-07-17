# Julissa Zelaya Portillo
# CS5200 Spring 2024
# Practicum II
# Load XML Data

## Libraries

# Package names
packages <- c("RSQLite", "XML", "DBI", "knitr")
# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(!installed_packages)) {
  install.packages(packages[!installed_packages])
}
# Packages loading
invisible(lapply(packages, function(pkg) {
  library(pkg, character.only = TRUE)
}))


# Connect to SQLite database (and create)
con <- dbConnect(SQLite(), "pharma_sales.db")

## Realize the relational schema in SQLite

# Create Products table
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS Products (
    productID INTEGER PRIMARY KEY AUTOINCREMENT,
    product TEXT
  )
")

# Create Reps table
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS Reps (
    repID INTEGER PRIMARY KEY,
    name TEXT,
    territory TEXT,
    commission REAL
  )
")

# Create Customers table
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS Customers (
    customerID INTEGER PRIMARY KEY AUTOINCREMENT,
    customer TEXT,
    country TEXT
  )
")

# Create Sales table
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS Sales (
    saleID INTEGER PRIMARY KEY AUTOINCREMENT,
    txnID INTEGER,
    repID INTEGER,
    customerID INTEGER,
    productID INTEGER,
    date DATE,
    qty INTEGER,
    total INTEGER,
    currency TEXT DEFAULT USD,
    FOREIGN KEY (repID) REFERENCES Reps(repID),
    FOREIGN KEY (customerID) REFERENCES Customers(customerID),
    FOREIGN KEY (productID) REFERENCES Products(productID)
  )
")


## Parse PharmaReps File into Data Frame

# Identify the PharmaReps XML file
find_reps_file <- list.files(path = "txn-xml", pattern = "^pharmaReps.*\\.xml$", full.names = TRUE)

# Check if file is found
if (length(find_reps_file) == 0) {
  stop("SalesRep file not found")
}

# Read and parse the XML file
xml_data <- xmlParse(find_reps_file)

# Extract the rep nodes
rep_nodes <- getNodeSet(xml_data, "//rep")

# Use sapply to extract attributes
df_reps <- data.frame(
  repID = sapply(rep_nodes, function(x) substring(xmlGetAttr(x, "rID"), 2)), # remove first 'r' character
  name = sapply(rep_nodes, function(x) xmlValue(xmlChildren(x)$name)),
  territory = sapply(rep_nodes, function(x) xmlValue(xmlChildren(x)$territory)),
  commission = as.numeric(sapply(rep_nodes, function(x) xmlValue(xmlChildren(x)$commission))),
  stringsAsFactors = FALSE
)


## Parse SalesTxn File into Data Frame

# Identify the SalesTxn XML file
transaction_files <- list.files(path = "txn-xml", pattern = "^pharmaSalesTxn.*\\.xml$", full.names = TRUE)

# Check if any transaction file is found
if (length(transaction_files) == 0) {
  stop("SalesTxn file not found")
}

# Initialize an empty dataframe to store all transactions
all_txns_df <- data.frame()

# Loop over each transaction file
for (file_name in transaction_files) {
  # Read and parse the XML file
  xml_data <- xmlParse(file_name)
  
  # Extract transaction nodes
  txn_nodes <- getNodeSet(xml_data, "//txn")
  
  # Function to extract data from each txn node
  extract_txn_data <- function(txn_node) {
    txn_id <- xmlGetAttr(txn_node, "txnID")
    rep_id <- xmlGetAttr(txn_node, "repID")
    customer <- xmlValue(txn_node[["customer"]])
    country <- xmlValue(txn_node[["country"]])
    sale_node <- txn_node[["sale"]]
    date <- xmlValue(sale_node[["date"]]) # Original date in MM/DD/YYYY format
    
    # Convert date from MM/DD/YYYY to YYYY/MM/DD
    date <- format(as.Date(date, "%m/%d/%Y"), "%Y/%m/%d")
    
    product <- xmlValue(sale_node[["product"]])
    qty <- as.integer(xmlValue(sale_node[["qty"]]))
    total_node <- sale_node[["total"]]
    total <- as.numeric(xmlValue(total_node))
    currency <- xmlGetAttr(total_node, "currency")
    
    # Combine into a data frame
    data.frame(
      txnID = txn_id, repID = rep_id, customer = customer, country = country,
      date = date, product = product, qty = qty, total = total, currency = currency,
      stringsAsFactors = FALSE
    )
  }
  
  # Apply the function to each transaction node and combine results
  df_txns <- do.call(rbind, lapply(txn_nodes, extract_txn_data))
  
  # Append the data frame from the current file to the all transactions data frame
  all_txns_df <- rbind(all_txns_df, df_txns)
}


## Save Data Frame to SQL Database

# Erase existing data
dbExecute(con, "DELETE FROM Reps")
dbExecute(con, "DELETE FROM Customers")
dbExecute(con, "DELETE FROM Products")
dbExecute(con, "DELETE FROM Sales")

# Insert data into Reps table
dbWriteTable(con, "Reps", df_reps, append = TRUE, row.names = FALSE)

# Customer and product tables come from transaction data
# Customers and Products should be unique (lookup table)
customers <- unique(all_txns_df[, c("customer", "country")])
products <- unique(all_txns_df["product"])

# Insert data into Customers and Products tables
dbWriteTable(con, "Customers", customers, append = TRUE, row.names = FALSE)
dbWriteTable(con, "Products", products, append = TRUE, row.names = FALSE)

# Prepare Sales data by linking Customer and Product IDs
customer_ids <- dbGetQuery(con, "SELECT rowid AS customerID, customer FROM Customers")
product_ids <- dbGetQuery(con, "SELECT rowid AS productID, product FROM Products")
all_txns_df <- merge(all_txns_df, customer_ids, by.x = "customer", by.y = "customer")
all_txns_df <- merge(all_txns_df, product_ids, by.x = "product", by.y = "product")
sales_data <- all_txns_df[, c("txnID", "repID", "customerID", "productID", "date", "qty", "total", "currency")]

# Insert Sales data
dbWriteTable(con, "Sales", sales_data, append = TRUE, row.names = FALSE)

# Close the database connection
dbDisconnect(con)

## Create and connect to MYSQL Database

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

# Disconnect to MYSQL database
dbDisconnect(mysql)

## Test Data for Data Insertion to Database

# Connect to the SQLite database
# con <- dbConnect(RSQLite::SQLite(), dbname = "pharma_sales.db")
# 
# Query to check data in the Products table
# products_query <- "SELECT * FROM Products LIMIT 10;"
# products_result <- dbGetQuery(con, products_query)
# print("Products Data:")
# print(products_result)
# 
# # Query to check data in the Reps table
# reps_query <- "SELECT * FROM Reps LIMIT 10;"
# reps_result <- dbGetQuery(con, reps_query)
# print("Reps Data:")
# print(reps_result)
# 
# # Query to check data in the Customers table
# customers_query <- "SELECT * FROM Customers LIMIT 10;"
# customers_result <- dbGetQuery(con, customers_query)
# print("Customers Data:")
# print(customers_result)
# 
# # Query to verify entries in the Sales table
# sales_query <- "
# SELECT Sales.txnID, Sales.date, Reps.name AS RepName, Customers.customer, Products.product, Sales.qty, Sales.total
# FROM Sales
# JOIN Reps ON Sales.repID = Reps.repID
# JOIN Customers ON Sales.customerID = Customers.customerID
# JOIN Products ON Sales.productID = Products.productID
# LIMIT 10;"
# sales_result <- dbGetQuery(con, sales_query)
# print("Sales Data:")
# print(sales_result)
# 
# # Query to check total entries in each table
# count_query <- "
# SELECT 'Products' AS TableName, COUNT(*) AS TotalEntries FROM Products
# UNION ALL
# SELECT 'Reps', COUNT(*) FROM Reps
# UNION ALL
# SELECT 'Customers', COUNT(*) FROM Customers
# UNION ALL
# SELECT 'Sales', COUNT(*) FROM Sales;"
# count_result <- dbGetQuery(con, count_query)
# print("Total Entries in Each Table:")
# print(count_result)
# 
# # Close the database connection
# dbDisconnect(con)

# Boston Crime Analysis
    In this notebook, the Boston crime data is analyzed from data stored in dashDB and the results are written back into dashDB

##Overall, these are the steps involved in analyzing the data
    1. Download the data from the Boston Open Data website as CSV and upload into dashDB 
    2. Read in the table containing Boston Crime statistics from dashDB
    3. Analyze the data using Apache Spark
    4. Store the analyzed results into a new table in dashDB

  Datasets:
      Crime statistics:
      https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports/7cdf-6fgx 
      Police districts:
      https://dataverse.harvard.edu/dataset.xhtml?id=2701204&versionId76395 
     
      


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

### Add the dashDB data source by selecting 'Add Source' on the panel to the right
### Select your dashDB instance

Select the dashDB 'Insert to code' in the box below to retrieve the parameters for dashDB to be replaced below



    port : 
    db : 
    username : 
    ssljdbcurl : 
    host :
    https_url :
    dsn : 
    hostname : 
    jdbcurl : 
    ssldsn : 
    uri : 
    password : 
    



    val crimeData_df = sqlContext.
        load("jdbc", 
        Map( "url" -> "<jdbcurl>:user=<username>;password=<password>;",
        "dbtable" -> "<schema>.CRIME_INCIDENT_REPORTS"))
    
    crimeData_df.count()


Filter out the non-district values from the DataFrame to clean up the data 


    val crimeDataCleaned_df = crimeData_df.select(crimeData_df("REPTDISTRICT")).where(crimeData_df("REPTDISTRICT") !== "NULL").where(crimeData_df("REPTDISTRICT")!=="HTU").orderBy("REPTDISTRICT") 
    crimeDataCleaned_df.count()




    267691



Take an aggregate of total number of crimes by district
Rename the REPTDISTRICT to ID to match the key of District set


    val crimeDataTotals_df = crimeDataCleaned_df.groupBy("REPTDISTRICT").agg(count("REPTDISTRICT")).toDF("ID", "COUNT")
    crimeDataTotals_df.count()




    12



Replace the parameters below with the appropriate dashDB properites

Create the table that will hold the result set


    import java.sql.DriverManager
    val jdbcClassName = "com.ibm.db2.jcc.DB2Driver"
    val jdbcurl="" // enter the hostip from connection settings
    val user="" // put the username from connection settings
    val password="" // put the password from connection settings
    Class.forName(jdbcClassName)
    val connection = DriverManager.getConnection(jdbcurl, user, password)
    val stmt = connection.createStatement()
    println("Execute the statement:\n"+
                        "CREATE TABLE <schema>.BOSTONCRIME(" +
                        "ID VARCHAR(10) ," +
                        "NUM INTEGER)" )
                      
    stmt.executeUpdate("CREATE TABLE <schema>.BOSTONCRIME(" +
                        "ID VARCHAR(10) ," +
                        "NUM INTEGER)")
                         
    stmt.close()
    connection.commit()

    Execute the statement:
    CREATE TABLE DASH013602.BOSTONCRIME(ID VARCHAR(10) ,NUM INTEGER)


Write the result set into the dashDB table to be used downstream
Replace <schema> with proper schema name


    val jdbcurl = ""
    crimeDataTotals_df.insertIntoJDBC(jdbcurl, "<schema>.BOSTONCRIME", false)
    val GetFinalDataCount = sqlContext.jdbc(url = jdbcurl,"<schema>.BOSTONCRIME").show()

    +---+-----+
    | ID|  NUM|
    +---+-----+
    | B2|40640|
    | A1|29653|
    |A15| 5621|
    | A7|12783|
    |D14|19520|
    | C6|20767|
    | B3|24812|
    | D4|37908|
    |E13|15167|
    | E5|12226|
    |E18|13985|
    |C11|34609|
    +---+-----+
    



    

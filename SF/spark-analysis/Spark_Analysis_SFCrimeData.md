
# San Francisco Crime Data Analysis

In this notebook, the SF crime data is analyzed and the results are stored in the Bluemix Object Data Store.

##Overall, these are the steps involved in analyzing the data.

    1.Download the data from the SF open data website and upload the files to the Object Data Store. 
    2.Analyze the data using Apache Spark.
    3.Store the analyzed results to Object Data store.
    

##Data Set

[SF Open Data Website](https://data.sfgov.org/data?category=Public%20Safety)

[SF Open Data Website, Page 2](https://data.sfgov.org/data?category=Public%20Safety)

######Two data sets are used in this notebook. 
    1.SF Crime Data since January 1st, 2003.
    2.SF Police Districts geo co-ordinates data.


##Configuration to Access Object Storage

Below cell defines the function that can be used to set the configuration to access Object Storage


    def set_hadoop_config(credentials):
        prefix = "fs.swift.service." + credentials['name'] 
        hconf = sc._jsc.hadoopConfiguration()
        hconf.set(prefix + ".auth.url", credentials['auth_url']+'/v3/auth/tokens')
        hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
        hconf.set(prefix + ".tenant", credentials['project_id'])
        hconf.set(prefix + ".username", credentials['user_id'])
        hconf.set(prefix + ".password", credentials['password'])
        hconf.setInt(prefix + ".http.port", 8080)
        hconf.set(prefix + ".region", credentials['region'])
        hconf.setBoolean(prefix + ".public", True)

##Adding files to the Object Storage
Add the downloaded files to Object Storage by clicking on **Data Sources** on the right hand side of the page. Then place the cursor in a empty cell and click on the **Insert to code** to get the credentials to access the SF crime data file into the notebook.


    

Do the same thing for the SF police District geo coordinates data file.


    

Insert the appropriate data into the dictionaries. Two dictionaries, one for each file. Not sure, if the **filename** key can have multiple filenames. If it supports multiple filenames, then we can have just one credentials dictionary. 


    credentials = {
        'auth_url': '',
        'project_id': '',
        'region': '',
        'user_id': '',
        'username': '',
        'password': '',
        'filename': '',
        'container': ''
    }
    
    credentials_2 = {
        'auth_url': '',
        'project_id': '',
        'region': '',
        'user_id': '',
        'username': '',
        'password': '',
        'filename': '',
        'container': ''
    }

Now, using the above credentials call the configuration function to set the configuration settings for Object Storage.


    credentials['name'] = 'crimes'
    set_hadoop_config(credentials)
    
    credentials_2['name'] = 'districts'
    set_hadoop_config(credentials_2)

The data files are csv files. So, to work with CSV files, the pyspark_csv module is added to the SparkContext. Similarly, the matplot library is added to draw plots.


    from pyspark.sql import SQLContext
    sqlContext = SQLContext(sc)
    
    sc.addPyFile("https://raw.githubusercontent.com/seahboonsiew/pyspark-csv/master/pyspark_csv.py")
    import pyspark_csv as pycsv
    
    
    %matplotlib inline
    
    import matplotlib.pyplot as plt
    # matplotlib.patches allows us create colored patches, we can use for legends in plots
    import matplotlib.patches as mpatches
    # seaborn also builds on matplotlib and adds graphical features and new plot types

Read the Files from Object Storage


    crimeData = sc.textFile("swift://notebooks." + credentials['name'] + "/SFPD_Incidents_-_from_1_January_2003 (3).csv")
    policeDeptDistrictData = sc.textFile("swift://notebooks." + credentials_2['name'] + "/sfpd_districts (4).csv")

##Analysis

The csv files are converted in Spark DataFrames using the pyspark_csv library.


    crimeData_df = pycsv.csvToDataFrame(sqlContext, crimeData)


    policeDeptDistrictData_df = pycsv.csvToDataFrame(sqlContext,policeDeptDistrictData)

Check the schema to confirm that the files are loaded properly


    crimeData_df.printSchema()


    policeDeptDistrictData_df.printSchema()

Change the column names in SF police District data set to match SF Crime Data columns, as we have to join both the data sets.


    policeDeptDistrictData_newCol_df = policeDeptDistrictData_df.selectExpr("COMPANY as COMPANY","the_geom as the_geom","DISTRICT as PdDistrict","OBJECTID as OBJECTID")
    policeDeptDistrictData_newCol_df.printSchema()

Group the Crime Data based on **District** and store the count of crimes in each district. Then sort the dataframe in descending order of crimes.


    crimeDataByDistrict = crimeData_df.filter(crimeData_df['PdDistrict'] != "null").groupBy("PdDistrict").count()
    crimeDataByDistrictOrder = crimeDataByDistrict.sort("count",ascending=False)
    for line in crimeDataByDistrictOrder.collect():
        print line

Now, join both the dataframes based on the **PdDistrict** column.


    districtCrimeDataWithLongLat = crimeDataByDistrictOrder.join(policeDeptDistrictData_newCol_df,"PdDistrict")

Check the schema of the resultant dataframe


    districtCrimeDataWithLongLat.printSchema()

##Storing the Results in Object Storage

The results are stored as text file in the Object Storage data store. While storing the file, partition value has to be set to 1, so that all the results are stored in a single file. 


    districtCrimeDataWithLongLatJSON = districtCrimeDataWithLongLat.toJSON()


    districtCrimeDataWithLongLatJSON.repartition(1).saveAsTextFile("swift://notebooks." + credentials['name'] + "/SFDistrictCrimesCount.json")

##More Analysis

Extract the year and month value from the **Date** column and store them as new columns.


    crimeData_df = crimeData_df.withColumn('Year',crimeData_df['Date'].substr(0,4))
    crimeData_df = crimeData_df.withColumn('Month',crimeData_df['Date'].substr(6,2))
    crimeData_df.printSchema()

You can view the contents of the new column by running the below code.


    crimeData_df.select('Year').show()


    crimeData_df.select('Month').show()

We can find out how the crime rate is during the Christmas season in each year using the below code. 


    crimeDataCountByHolidaySeasonInEachYear = crimeData_df.filter(crimeData_df['Month']==12).groupBy('Year').count()


    for line in crimeDataCountByHolidaySeasonInEachYear.collect():
        print line

We can also find out how the crime rate is in each month of a year.


    crimeDataCountByMonthFor2015 = crimeData_df.filter(crimeData_df['Year'] == 2015).groupBy('Month').count()
    for line in crimeDataCountByMonthFor2015.collect():
        print line

Crime rate in each year


    crimeDataCountByYear = crimeData_df.groupBy('Year').count()
    for line in crimeDataCountByYear.collect():
        print line

No of crimes based on the type of crime.


    crimeDataCountByCategory = crimeData_df.filter(crimeData_df['Category'] != "null").groupBy("Category").count()
    crimeDataCountByCategory = crimeDataCountByCategory.sort("count",ascending=False)
    for line in crimeDataCountByCategory.collect():
        print line

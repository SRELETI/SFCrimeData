

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


    auth_uri :
    global_account_auth_uri :
    username : Admin_5fb4326f12092f39b26c515816fed2336b7516a8
    password : LG7(]7i#}!*h(Po}
    auth_url : https://identity.open.softlayer.com
    project : object_storage_d2a10b0c_9c47_40e6_99ec_29d1095854dc
    project_id : 761d6775aab242ca991d6360619220e3
    region : dallas
    user_id : 1ea346732c8b413e9d84c8d623acaede
    domain_id : 31372ce3a6c54dca8f5ab9adbf03e249
    domain_name : 929353
    filename : 2007data.csv
    container : notebooks
    tenantId : sf3d-b64afbb80e30d5-9e76a24022cf



      File "<ipython-input-21-2ace772ed7cd>", line 1
        auth_uri :
                 ^
    SyntaxError: invalid syntax




    year = '2007'


    credentials = {
        'auth_url': '',
        'project_id': '',
        'region': '',
        'user_id': '',
        'username': '',
        'password': '',
        'filename': ',
        'container': 'n'
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


    credentials['name'] = 'crimes'
    set_hadoop_config(credentials)

    credentials_2['name'] = 'districts'
    set_hadoop_config(credentials_2)


    from pyspark.sql import SQLContext
    sqlContext = SQLContext(sc)

    sc.addPyFile("https://raw.githubusercontent.com/seahboonsiew/pyspark-csv/master/pyspark_csv.py")
    import pyspark_csv as pycsv


    %matplotlib inline

    import matplotlib.pyplot as plt
    # matplotlib.patches allows us create colored patches, we can use for legends in plots
    import matplotlib.patches as mpatches
    # seaborn also builds on matplotlib and adds graphical features and new plot types


    crimeData = sc.textFile("swift://notebooks." + credentials['name'] + "/" + credentials['filename'])
    policeDeptDistrictData = sc.textFile("swift://notebooks." + credentials_2['name'] +"/" + credentials_2['filename'])


    credentials['filename']




    '2007data.csv'




    crimeData_df = pycsv.csvToDataFrame(sqlContext, crimeData)



    policeDeptDistrictData_df = pycsv.csvToDataFrame(sqlContext,policeDeptDistrictData)


    crimeData_df.printSchema()

    root
     |-- IncidntNum: integer (nullable = true)
     |-- Category: string (nullable = true)
     |-- Descript: string (nullable = true)
     |-- DayOfWeek: timestamp (nullable = true)
     |-- Date: timestamp (nullable = true)
     |-- Time: timestamp (nullable = true)
     |-- PdDistrict: string (nullable = true)
     |-- Resolution: string (nullable = true)
     |-- Address: string (nullable = true)
     |-- X: double (nullable = true)
     |-- Y: double (nullable = true)
     |-- Location: string (nullable = true)
     |-- PdId: integer (nullable = true)




    policeDeptDistrictData_df.printSchema()


    root
     |-- COMPANY: string (nullable = true)
     |-- the_geom: string (nullable = true)
     |-- DISTRICT: string (nullable = true)
     |-- OBJECTID: integer (nullable = true)




    policeDeptDistrictData_newCol_df = policeDeptDistrictData_df.selectExpr("COMPANY as COMPANY","the_geom as the_geom","DISTRICT as PdDistrict","OBJECTID as OBJECTID")
    policeDeptDistrictData_newCol_df.printSchema()

    root
     |-- COMPANY: string (nullable = true)
     |-- the_geom: string (nullable = true)
     |-- PdDistrict: string (nullable = true)
     |-- OBJECTID: integer (nullable = true)




    crimeDataByDistrict = crimeData_df.filter(crimeData_df['PdDistrict'] != "null").groupBy("PdDistrict").count()
    crimeDataByDistrictOrder = crimeDataByDistrict.sort("count",ascending=False)
    for line in crimeDataByDistrictOrder.collect():
        print line

    Row(PdDistrict=u'SOUTHERN', count=24175)
    Row(PdDistrict=u'MISSION', count=18434)
    Row(PdDistrict=u'NORTHERN', count=16430)
    Row(PdDistrict=u'TENDERLOIN', count=14735)
    Row(PdDistrict=u'BAYVIEW', count=13925)
    Row(PdDistrict=u'CENTRAL', count=13335)
    Row(PdDistrict=u'INGLESIDE', count=12797)
    Row(PdDistrict=u'TARAVAL', count=9988)
    Row(PdDistrict=u'PARK', count=7234)
    Row(PdDistrict=u'RICHMOND', count=6586)



    districtCrimeDataWithLongLat = crimeDataByDistrictOrder.join(policeDeptDistrictData_newCol_df,"PdDistrict")


    districtCrimeDataWithLongLat.printSchema()

    root
     |-- PdDistrict: string (nullable = true)
     |-- count: long (nullable = false)
     |-- COMPANY: string (nullable = true)
     |-- the_geom: string (nullable = true)
     |-- OBJECTID: integer (nullable = true)




    districtCrimeDataWithLongLatJSON = districtCrimeDataWithLongLat.toJSON()


    districtCrimeDataWithLongLatJSON.repartition(1).saveAsTextFile("swift://notebooks." + credentials['name'] + "/SFDistrictCrimesCount" + year + ".json")




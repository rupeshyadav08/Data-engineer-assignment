from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year, to_date, broadcast, lit, split, when, max,min, to_timestamp, avg
import findspark

findspark.init()

# create a SparkSession
spark = SparkSession.builder.appName("PlayStoreAppData").getOrCreate()


class SegwiseModel:
    def __init__(self, spark_session):
        """
        Constructor for the RenewableEnergyModel class.
        
        Parameters:
        - spark_session: SparkSession object used for Spark operations.
        """
        self.spark = spark_session

    def read_csv(self, file_path):
        """
            Read a CSV file into a DataFrame using SparkSession.

            Parameters:
            - file_path: The path to the CSV file.

            Returns:
            A DataFrame containing the data from the CSV file.
        """
        return self.spark.read.csv(file_path, header=True, inferSchema=True)
    
    def correction_of_data(self,df):
        """
            Cleaning and costing the data.

            Parameters:
            - df: Dataframe .

            Returns:
            A DataFrame containing the data from the CSV file.
        """
        df = df.withColumnRenamed("_c0", "Index")
        
        #Picking the important columns for the operations
        columns = ['Index', 'appId', 'developer', 'developerId', 'developerWebsite', 'free', 'genreId', 'inAppProductPrice',
       'minInstalls', 'originalPrice', 'price', 'ratings', 'adSupported', 'containsAds', 'reviews',
       'releasedDayYear', 'sale', 'score', 'title', 'releasedDay', 'releasedYear', 'releasedMonth', 'dateUpdated',
       'minprice', 'maxprice']
        
        # Casting column datatype to sutiable datatype
        #Float
        df = df.withColumn("price", col("price").cast("float"))
        df = df.withColumn("score", col("score").cast("float"))
        df = df.withColumn("minprice", col("minprice").cast("float"))
        df = df.withColumn("maxprice", col("maxprice").cast("float"))
        #Int
        df = df.withColumn("free", col("free").cast("int"))
        df = df.withColumn("ratings", col("ratings").cast("int"))
        df = df.withColumn("minInstalls", col("minInstalls").cast("int"))
        df = df.withColumn("adSupported", col("adSupported").cast("int"))
        df = df.withColumn("containsAds", col("containsAds").cast("int"))
        df = df.withColumn("reviews", col("reviews").cast("int"))
        df = df.withColumn("releasedYear", col("releasedYear").cast("int"))


        # Parse ReleasedDayYear column to timestamp and date (assuming format "yyyy-MM-dd")
        df = df.withColumn("dateUpdated", to_timestamp(col("dateUpdated")))

        df =  df.select(columns)
        
        return df
    
    def creating_buckets(df):
        """
            Creating the bucket needed for analysis.

            Parameters:
            - df: Dataframe used for creating buvket.

            Returns:
            A DataFrame containing the data from the CSV file.
        """
        df.write.bucketBy(12, 'releasedYear').saveAsTable('releasedYear_bucket', format='csv')
        df.write.bucketBy(10, 'developerId').saveAsTable('developer_bucket', format='csv')
        df.write.bucketBy(10, 'reviews').saveAsTable('reviews_bucket', format='csv')
        df.write.bucketBy(10, 'price').saveAsTable('price_bucket', format='csv')
        df.write.bucketBy(10, 'genreId').saveAsTable('genre_bucket', format='csv')
        df.write.bucketBy(12, 'minInstalls').saveAsTable('Installation_bucket', format='csv')
    

class SegwiseModelController:
    def __init__(self, spark_session):
        self.model = SegwiseModel(spark_session)
        
    def process_data(self, file_path, time_interval):
        df = self.model.read_csv(file_path)
        df = self.model.correction_of_data(df)
        self.model.creating_buckets(df)
        return df
    
    def start_operation(bucketname):
        price = spark.table(bucketname)


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("SegwiseAnalysis") \
        .getOrCreate()

    # Initialize controller
    controller = SegwiseModelController(spark)

    # Process data Make sure the dataset folder conatins the data
    input_file = "Data/playstore.csv" # change the file name according to device
    df = controller.process_data(input_file) #Pass the time in min 

    price = controller.start_operation("price_bucket")
    #1) Finding the top 10 costliest app in the data and developer developed it and genre it belong
    price.createOrReplaceTempView('price_table')
    app_cost_result = spark.sql("""SELECT appId, developerId, genreId, price
                                FROM price_table
                                ORDER BY price desc
                                LIMIT 10
                                """)
    app_cost_result.show()
    app_cost_result.write.csv("output/price_bucket/app_cost_result.csv", header=True)
    
    
    #2) Costilest app across each genre
    top_costliest_app_genre =spark.sql( """
        SELECT appId,developer,genreId,maxprice
        FROM (
            SELECT
                appId, developer, genreId, maxprice,
                ROW_NUMBER() OVER (PARTITION BY genreId ORDER BY maxprice DESC) AS rank
            FROM
                price_table
        ) ranked
        WHERE rank = 1
        ORDER BY maxprice desc
    """)
    top_costliest_app_genre.show()
    top_costliest_app_genre.write.csv("output/price_bucket/top_costliest_app_genre.csv", header=True)
    
    #Operations on Installation_bucket bucket
    Install = controller.start_operation("Installation_bucket")
    #3) top app across each genre in number of installation 
    Install.createOrReplaceTempView('Installation_table')
    top_apps_install = spark.sql( """
        SELECT appId, developerId, genreId, minInstalls
        FROM (
            SELECT appId, developerId, genreId, minInstalls,
                ROW_NUMBER() OVER (PARTITION BY genreId ORDER BY minInstalls DESC) AS rank
            FROM
                Installation_table
        ) ranked
        WHERE rank <= 1
        ORDER BY minInstalls desc
    """)
    top_apps_install.show()
    top_apps_install.write.csv("output/Installation_bucket/top_apps_install.csv", header=True)
    
    
    #Operations on review bucket
    review = controller.start_operation("reviews_bucket")
    #4) top review free app accross each genreId
    review.createOrReplaceTempView('review_table')
    top_reviewed_free_apps = spark.sql("""
        SELECT appId,developerId,genreId,title,reviews
        FROM (
            SELECT appId, developerId, genreId, title, reviews,
                ROW_NUMBER() OVER (PARTITION BY genreId ORDER BY reviews DESC) AS rank
            FROM
                review_table
            WHERE
                free = 1
        ) ranked
        WHERE rank = 1
        ORDER BY reviews desc
    """)
    top_reviewed_free_apps.show()

    top_reviewed_free_apps.write.csv("output/review_bucket/top_reviewed_free_apps.csv", header=True)
    
    
    #5) top review paid app accross each genreId
    top_reviewed_paid_apps = spark.sql("""
        SELECT appId,developerId,genreId,title,reviews
        FROM (
            SELECT appId, developerId, genreId, title, reviews,
                ROW_NUMBER() OVER (PARTITION BY genreId ORDER BY reviews DESC) AS rank
            FROM
                review_table
            WHERE
                free = 0
        ) ranked
        WHERE rank = 1
        ORDER BY reviews desc
    """)
    top_reviewed_free_apps.show()

    top_reviewed_paid_apps.write.csv("output/review_bucket/top_reviewed_paid_apps.csv", header=True)
    
    #Operations on developer bucket
    developers = controller.start_operation("developer_bucket")
    
    developers.createOrReplaceTempView('developers_table')
    #6) Count of developers with the most apps
    count_most_apps_by_developer =spark.sql("""
        SELECT developerId, COUNT(*) AS app_count 
        FROM developers_table
        GROUP BY developerId
        ORDER BY app_count desc
        limit 10
    """)
    count_most_apps_by_developer.show()
    count_most_apps_by_developer.write.csv("output/developer_bucket/count_most_apps_by_developer.csv", header=True)
    
    # 7) Write SQL query to find the developers with the most minInstalls
    developers_with_most_mininstalls_query = spark.sql("""
        SELECT developerId, SUM(minInstalls) AS total_minInstalls
        FROM developers_table
        GROUP BY developerId
        ORDER BY total_minInstalls DESC
        LIMIT 10
    """)
    developers_with_most_mininstalls_query.show()
    developers_with_most_mininstalls_query.write.csv("output/developer_bucket/developers_with_most_mininstalls_query.csv", header=True)
    
    #8)  developers with the most minInstalls in each genre
    developers_with_most_mininstalls_genrewise =spark.sql("""
        SELECT developerId, genreId, total_minInstalls
        FROM (
            SELECT
                developerId,
                genreId,
                SUM(minInstalls) AS total_minInstalls,
                ROW_NUMBER() OVER (PARTITION BY genreId ORDER BY SUM(minInstalls) DESC) AS rn
            FROM
                developers_table
            GROUP BY
                developerId, genreId
        ) ranked
        WHERE rn = 1
        ORDER BY total_minInstalls desc
    """)
    developers_with_most_mininstalls_genrewise.show()
    developers_with_most_mininstalls_genrewise.write.csv("output/developer_bucket/developers_with_most_mininstalls_genrewise.csv", header=True)

    #Operations on relesed year bucket
    releasedYear = controller.start_operation("releasedYear_bucket")
    #9) Top 10 Years with the most released app 
    releasedYear.createOrReplaceTempView('releasedYear_table')
    most_realsed_year =spark.sql("""
        SELECT releasedYear, COUNT(*) as total_release from releasedYear_table
        GROUP BY releasedYear
        ORDER BY total_release desc
        limit 10
    """)
    most_realsed_year.show()
    most_realsed_year.write.csv("output/releasedYear_bucket/most_realsed_year.csv", header=True)
    
    #10) App relased in each month of years and there rank
    app_year_month = spark.sql("""
             Select *, DENSE_RANK() OVER(PARTITION BY releasedYear ORDER BY month_count DESC) as rank
             FROM(
                 SELECT releasedYear, releasedMonth, COUNT(releasedMonth) as month_count  FROM releasedYear_table
                 WHERE releasedYear BETWEEN 2000 AND 2024 
                 and releasedMonth in ('Jan', 'Feb' ,'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
                 GROUP BY releasedYear, releasedMonth
                 ORDER BY releasedYear, month_count desc
             )

    """)

    app_year_month.show()
    app_year_month.write.csv("output/releasedYear_bucket/app_year_month.csv", header=True)
    
    #11) Most common month to release a app by developers
    app_year_month.createOrReplaceTempView('app_year_month')
    most_common_month = spark.sql(""" 
            SELECT releasedMonth, SUM(month_count) as total_count
            FROM app_year_month
            GROUP BY releasedMonth
    """
    )
    most_common_month.show()
    most_common_month.write.csv("output/releasedYear_bucket/most_common_month.csv", header=True)
    
    # Operations on price bucket
    genre = controller.start_operation("genre_bucket")
    
    #12) Which genre has most paid app
    genre.createOrReplaceTempView('genre_table')
    most_paid_genre = spark.sql(""" 
        SELECT genreId, COUNT(*) AS paid_app_count
        FROM genre_table
        WHERE  free = 0
        GROUP BY genreId
        ORDER BY paid_app_count DESC
    """
    )
    #13) Which genre has most free app
    genre.createOrReplaceTempView('genre_table')
    most_free_genre = spark.sql(""" 
        SELECT genreId, COUNT(*) AS free_app_count
        FROM genre_table
        WHERE  free = 1
        GROUP BY genreId
        ORDER BY free_app_count DESC
    """
    )

    most_paid_genre.show()
    most_free_genre.show()
    joined_result = most_paid_genre.join(most_free_genre, on="genreId", how="inner")
    joined_result.show()
    joined_result.write.csv("output/genreBucket_bucket/joined_result_free_paid.csv", header=True)
    
    #14)How many genre contain in app purchase and also are paid app
    most_paid_genre_inApp =spark.sql("""
        SELECT genreId, COUNT(*) AS paid_app_count
        FROM genre_table
        WHERE free = 0 and inAppProductPrice != 'None'
        GROUP BY genreId
        ORDER BY paid_app_count DESC
    """)
    most_paid_genre_inApp.show()


    #15) How many genre does not contain in app purchase and also are free app

    most_free_inApp =spark.sql("""
        SELECT genreId, COUNT(*) AS free_count
        FROM genre_table
        WHERE free = 1 and inAppProductPrice = 'None'
        GROUP BY genreId
        ORDER BY free_count DESC
    """)
    most_free_inApp.show()
    joined_result = most_paid_genre_inApp.join(most_free_inApp, on="genreId", how="inner")
    joined_result.show()
    joined_result.write.csv("output/genreBucket_bucket/joined_result_purchase.csv", header=True)

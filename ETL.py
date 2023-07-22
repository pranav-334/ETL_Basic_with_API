# import pyspark
# sparkSession = pyspark.sql.SparkSession\
#     .builder \
#     .appName("Python Spark Basic example") \
#     .config("spark.jars", "C:\SQL2019\mssql-jdbc-10.2.1.jre8.jar") \
#     .config("spark.executor.extraClassPath", "C:\SQL2019\mssql-jdbc-10.2.1.jre8.jar") \
#     .config("spark.driver.extraClassPath", "C:\SQL2019\mssql-jdbc-10.2.1.jre8.jar") \
#     .getOrCreate()

import pyspark
'''
Creating a Spark session for any Spark application is mandatory and it only runs if the internet connectivity is there, else it will fail with:
    00:46:47 WARN Utils: Your hostname, DESKTOP-38Q35QF resolves to a loopback address: 127.0.0.1; using 2406:7400:63:72e6:80a2:6e23:8a72:3882 instead (on interface wlan2)

'''
sparkSession = pyspark.sql.SparkSession \
    .builder \
    .appName("Python Spark Basic Example") \
    .config("spark.driver.extraClassPath", "C:\SQL2019\ojdbc8.jar") \
    .getOrCreate()

print("\n---Created a Spark Session---\n")

def extract_movies_to_df():
    '''
        Extracting movies table into a dataframe and returing it
    '''
    movies_df = sparkSession.read \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@localhost:1521:orcl") \
        .option("dbtable", "MOVIES") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("user", "sys as sysdba") \
        .option("password", "Sai@1234") \
        .load()
    return movies_df

def extract_users_to_df():
    '''
        Extracting users table into a dataframe and returing it
    '''
    users_df = sparkSession.read \
        .format("jdbc") \
        .option("url", "jdbc:oracle:thin:@localhost:1521:orcl") \
        .option("dbtable", "USERS") \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("user", "sys as sysdba") \
        .option("password", "Sai@1234") \
        .load()
    return users_df

def transform_avg_ratings(movie_df, users_df):
    '''
        Transformation
    '''
    # Creating a 'avg_rating' dataframe
    avg_rating = users_df.groupBy("movie_id").mean("rating")

    # Joining the tables: movie_df and avg_rating table on id
    df = movies_df.join(avg_rating, movies_df.ID == avg_rating.movie_id)

    df = df.drop("movie_id")
    return df


def load_df_to_db(df):
    '''
        Load the transformed table into another table
    '''
    mode = "overwrite"
    url = "jdbc:oracle:thin:@localhost:1521:orcl"
    properties = {
        "user" : "sys as sysdba",
        "password" : "Sai@1234",
        "driver" : "oracle.jdbc.driver.OracleDriver"
    }
    df.write.jdbc(
        url=url,
        table="avg_ratnigs",
        mode=mode,
        properties=properties
    )


if __name__ == "__main__":
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    ratings_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(ratings_df)
    print("Loaded successfully")
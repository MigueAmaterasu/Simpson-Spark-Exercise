import pandas as pd
import constants.constants as c
from pyspark.sql import SparkSession
from transform.transformations import Transformation as t
import pyspark.sql.functions as f


def main():
    spark = SparkSession.builder.appName(c.APP_NAME).master(c.MODE).getOrCreate()
    # spark.sparkContext.setLogLevel('info')
    simpson_df = spark.read.option(c.HEADER, c.TRUE_STRING).option('delimiter', c.DELIMITER).csv(c.INPUT_PATH)
    # simpson_df.show(601)

    cleaned_df = t.cleaned_df(simpson_df)
    best_season = t.best_season(cleaned_df)
    result_df = spark.createDataFrame([best_season])
    result_df.show()
    best_year = t.best_year(cleaned_df)
    result_df2 = spark.createDataFrame([best_year])
    result_df2.show()
    best_episode = t.best_episode(cleaned_df)
    result_df3 = spark.createDataFrame([best_episode])
    result_df3.show()
    df_with_score = t.df_with_score(cleaned_df)
    top_episodes_df = t.top_episodes_by_season(df_with_score)
    final_df = t.select_columns(top_episodes_df)
    final_df.show()


if __name__ == '__main__':
    main()

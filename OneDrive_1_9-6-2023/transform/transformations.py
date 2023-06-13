import pyspark.sql.functions as f
from pyspark.sql import DataFrame, types
from pyspark.sql.window import Window
import constants.constants as c

import pyspark


class Transformation:

    def __init__(self):
        pass

    @staticmethod
    def cleaned_df(df: DataFrame) -> DataFrame:
        return df.na.drop(subset=[c.RATING, c.VOTES, c.VIEWERS_IN_MILLIONS])

    @staticmethod
    def best_season(df: DataFrame) -> DataFrame:
        season_ratings = df.groupBy(c.SEASON).agg({c.RATING: "sum"}).withColumnRenamed("sum(rating)", "total_rating")
        best_season = season_ratings.orderBy(f.desc("total_rating")).first()
        return best_season

    @staticmethod
    def best_year(df: DataFrame) -> DataFrame:
        year_views = df.groupBy(c.ORIGINAL_AIR_YEAR).agg({c.VIEWERS_IN_MILLIONS: "sum"}).withColumnRenamed(
            "sum(viewers_in_millions)", "total_views")
        best_year = year_views.orderBy(f.desc("total_views")).first()
        return best_year

    @staticmethod
    def best_episode(df: DataFrame) -> DataFrame:
        df_with_score = df.withColumn(c.SCORE, f.col(c.RATING) * f.col(c.VIEWERS_IN_MILLIONS))
        best_episode = df_with_score.orderBy(f.desc(c.SCORE)).select(c.TITLE, c.SCORE).first()
        return best_episode

    @staticmethod
    def top_episodes_by_season(df: DataFrame) -> DataFrame:
        window = Window.partitionBy(c.SEASON).orderBy(f.desc(c.SCORE))
        ranked_df = df.withColumn(c.TOP, f.row_number().over(window))
        filtered_df = ranked_df.filter(f.col(c.TOP) <= 3)
        return filtered_df

    @staticmethod
    def select_columns(df: DataFrame) -> DataFrame:
        return df.select(c.SEASON,
                         c.TITLE,
                         c.NUMBER_IN_SEASON,
                         c.ORIGINAL_AIR_DATE,
                         c.ORIGINAL_AIR_YEAR,
                         c.PRODUCTION_CODE,
                         c.RATING,
                         c.VOTES,
                         c.VIEWERS_IN_MILLIONS,
                         c.SCORE,
                         c.TOP
                         ).orderBy(f.col(c.SEASON),f.col(c.TOP))

    @staticmethod
    def df_with_score(df: DataFrame) -> DataFrame:
        return df.withColumn(c.SCORE, f.col(c.RATING) * f.col(c.VIEWERS_IN_MILLIONS))

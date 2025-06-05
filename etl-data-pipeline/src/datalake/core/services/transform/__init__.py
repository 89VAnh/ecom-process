import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min, max, lag, when, round
from pyspark.sql.window import Window

from datalake.core.session.spark import SparkSession

# PostgreSQL connection properties
db_properties = {
    "url": os.getenv("POSTGRES_URL", "jdbc:postgresql://ecom_db:5432/ecom"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
    "driver": "org.postgresql.Driver",
}

print(f"Using PostgreSQL URL: {db_properties['url']}")


class Transform:
    df: DataFrame

    def __init__(self, spark: SparkSession, df: DataFrame):
        self.spark = spark
        self.df = df

    def build_fact_products(self) -> DataFrame:
        """
        Transform the results DataFrame into a products DataFrame matching the Product interface.

        Returns:
            DataFrame: Transformed DataFrame with columns matching the Product interface.
        """
        try:

            # Read platforms table
            platforms_df = (
                self.spark.read.format("jdbc")
                .option("url", db_properties["url"])
                .option("dbtable", "public.platforms")
                .option("user", db_properties["user"])
                .option("password", db_properties["password"])
                .option("driver", db_properties["driver"])
                .load()
                .select("platform_id", "name")
                .alias("p")
                .cache()
            )

            # Deduplicate results
            results_df = self.df.dropDuplicates(
                ["crawled_at", "platform_id", "title", "price"]
            ).alias("r")

            # Define window for price change calculation
            window_spec = Window.partitionBy("platform_id", "title").orderBy(
                col("crawled_at")
            )

            # Compute price_change_df
            price_change_df = (
                results_df.withColumn("price_numeric", col("price").cast("double"))
                .withColumn(
                    "prev_price", lag(col("price").cast("double"), 1).over(window_spec)
                )
                .withColumn(
                    "priceChange",
                    when(
                        (col("prev_price").isNotNull()) & (col("prev_price") != 0),
                        round(
                            (
                                (col("price_numeric") - col("prev_price"))
                                / col("prev_price")
                                * 100
                            ),
                            2,
                        ),
                    ).otherwise(0.0),
                )
                .orderBy("platform_id", "title", col("crawled_at").desc())
                .alias("pc")
            )

            # Calculate aggregates for lowest and highest prices
            agg_df = (
                results_df.groupBy("platform_id", "title")
                .agg(
                    min("price").alias("lowestPrice"),
                    max("price").alias("highestPrice"),
                    max("crawled_at").alias("latest_crawled_at"),
                )
                .alias("a")
            )

            # Join with aggregated data to get latest records
            latest_results_df = (
                price_change_df.join(
                    agg_df,
                    (col("pc.platform_id") == col("a.platform_id"))
                    & (col("pc.title") == col("a.title"))
                    & (col("pc.crawled_at") == col("a.latest_crawled_at")),
                    "inner",
                )
                .select(
                    col("pc.platform_id").alias("platform_id"),
                    col("pc.title"),
                    col("pc.img"),
                    col("pc.price"),
                    col("pc.rating_score"),
                    col("pc.reviews_number"),
                    col("pc.crawled_at"),
                    col("a.lowestPrice"),
                    col("a.highestPrice"),
                    col("pc.priceChange"),
                    when(col("pc.purchase_count").isNotNull(), col("pc.purchase_count"))
                    .otherwise(0)
                    .alias("purchaseCount"),
                    col("pc.link"),
                )
                .alias("lr")
            )

            # Final join with platforms
            result_df = latest_results_df.join(
                platforms_df, col("lr.platform_id") == col("p.platform_id"), "left"
            ).select(
                col("lr.platform_id").alias("platform_id"),
                col("lr.title").alias("name"),
                col("lr.img").alias("image"),
                when(col("p.name").isNotNull(), col("p.name"))
                .otherwise("Unknown")
                .alias("platform"),
                col("lr.price").alias("currentPrice"),
                col("lr.lowestPrice"),
                col("lr.highestPrice"),
                col("lr.priceChange"),
                col("lr.rating_score").alias("rating"),
                col("lr.reviews_number").alias("reviews"),
                col("lr.purchaseCount").alias("purchaseCount"),
                col("lr.link"),
                col("lr.crawled_at"),
            )

            return result_df
        except Exception as e:
            raise Exception(f"Error in build_fact_products: {str(e)}")

    def build_fact_histories(self) -> DataFrame:
        fact_histories_df = (
            self.df.select(
                col("crawled_at"),
                col("platform_id"),
                col("title"),
                col("price"),
                col("purchase_count"),
            )
            .filter(col("price").isNotNull())
            .fillna(0, subset=["purchase_count"])
            .dropDuplicates(
                ["crawled_at", "platform_id", "title", "price", "purchase_count"]
            )
        )
        return fact_histories_df

    @staticmethod
    def transforms(
        spark: SparkSession, dataframe: DataFrame, transform_func: str
    ) -> DataFrame:
        if transform_func == "":
            return dataframe
        udf = Transform(spark, dataframe)
        print(transform_func)
        func = udf.__getattribute__(transform_func)
        return func()

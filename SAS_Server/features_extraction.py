from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce


def get_spark_session():
    return SparkSession.builder.appName("FEATURE_EXTRACTION").getOrCreate()


def read_csv(spark, path):
    return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)


def save_df(df, path):
    return df.coalesce(1).write.option("header", "true").csv(path)


def columns_analysis(spark, file_name):

    original_file = read_csv(spark, file_name)
    # original_file.select('column').where("column like '%social_security_number%'").show()
    original_file.columns()

    return


def get_features(metadata_report):
    reduced_report = metadata_report[['Full Object Name', 'Object Name', 'PII Count']].orderBy('PII Count', ascending=False)
    for ind_file in reduced_report.rdd.collect():
        print(ind_file['Full Object Name'])


if __name__ == '__main__':

    spark = get_spark_session()
    big_id_report = read_csv(spark, "Objects.csv")
    get_features(big_id_report)
    columns_analysis(spark, '2017_12_clarity_inquiry.csv')
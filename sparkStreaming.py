from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws
from pyspark.sql.types import StructType, StructField, StringType
import os
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyvi import ViTokenizer
# Đặt các gói Spark cần thiết
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Tạo Spark Session
spark = SparkSession.builder \
    .appName("Kafka Spark Streaming Example") \
    .getOrCreate()

# Định nghĩa các tham số Kafka
kafka_bootstrap_servers = '192.168.1.7:9092'
kafka_topic_name = 'demo'

# Định nghĩa schema cho dữ liệu nhận vào
schema = StructType([
    StructField("content", StringType(), True),
    StructField("preprocessed", StringType(), True),
    StructField("NER_label", StringType(), True),
    StructField('tokenize', StringType(), True)
])


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic_name)
    .option("startingOffsets", "earliest")
    .load()
)
# Deserialize the JSON data
df = df.selectExpr("CAST(value AS STRING)")
df = df.withColumn("value", from_json(col("value"), schema))
df = df.select("value.*")
# Print the data to the console
df = df.dropna()
loaded_model = PipelineModel.load("./model/lr_model")

# Áp dụng model đã load để dự đoán
predictions = loaded_model.transform(df)
predictions = predictions.select('content', 'NER_label', 'prediction')
# Lưu dữ liệu vào file CSV
output_path = "./result"  # Đường dẫn đến thư mục lưu file CSV

query = predictions.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", "./result") \
    .start()

query.awaitTermination()

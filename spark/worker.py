import os
import sqlite3
import time
from pathlib import Path

import openai
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import StringType

# Set up OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

# Initialize Spark session
spark = (
    SparkSession.builder.appName("SparkKafkaDocxSummarizer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read stream from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "docx-topic")
    .load()
)

text_df = df.selectExpr("CAST(value AS STRING) as text")


# Define the summarization function using OpenAI
def openai_summary(paragraphs):
    try:
        full_text = "\n".join(paragraphs).strip()
        if not full_text:
            print("Empty paragraphs received")
            return ""

        print(f"Summarizing: \n{full_text}\n")

        response = openai.responses.create(
            model="gpt-4o-mini",
            input=[
                {
                    "role": "system",
                    "content": "You are an assistant that summarizes documents by identifying major topics.",
                },
                {
                    "role": "user",
                    "content": f"Summarize the following document content into main topics:\n{full_text}",
                },
            ],
        )
        return response.output_text
    except Exception as e:
        print(f"Error during summarization: {str(e)}")
        return ""


summary_udf = udf(openai_summary, StringType())

# Aggregate all text and generate summary
grouped_df = text_df.groupBy().agg(collect_list("text").alias("paragraphs"))
summarized_df = grouped_df.withColumn("summary", summary_udf(col("paragraphs")))

# Write the summary to a memory sink for querying
query = (
    summarized_df.select("summary")
    .writeStream.outputMode("complete")
    .format("memory")
    .queryName("summarized_query")
    .start()
)


# Function to write summary to SQLite (file path in a shared volume, e.g., /tmp/shared)
def write_summary_to_db(summary_text):
    if not summary_text:
        return

    db_path = Path("/tmp/shared/summary.db")
    if not db_path.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with open(db_path, "x"):
            pass

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS summary (id INTEGER PRIMARY KEY AUTOINCREMENT, summary_text TEXT)"
    )
    cursor.execute("INSERT INTO summary (summary_text) VALUES (?)", (summary_text,))
    conn.commit()
    conn.close()


# Continuously poll the memory sink and update SQLite
while True:
    try:
        result_df = spark.sql("SELECT * FROM summarized_query")
        if result_df.count() > 0:
            summary = result_df.collect()[0]["summary"]
            write_summary_to_db(summary)
            print("Updated summary in SQLite.")
        else:
            print("No summary available yet.")
    except Exception as e:
        print("Error updating SQLite summary:", e)
    time.sleep(5)

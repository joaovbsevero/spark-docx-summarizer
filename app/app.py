import logging
import os
import sqlite3
import time

import streamlit as st
from docx import Document
from kafka import KafkaProducer

# Set up logging
logging.basicConfig(
    filename="streamlit_app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

st.title("Docx Upload and Topic Summarization with Spark + Kafka")

# Initialize session state variables
if "file_processed" not in st.session_state:
    st.session_state.file_processed = False
if "uploaded_file_data" not in st.session_state:
    st.session_state.uploaded_file_data = None
if "uploaded_file_name" not in st.session_state:
    st.session_state.uploaded_file_name = None

# File uploader
uploaded_file = st.file_uploader(
    "Choose a DOCX file to upload", type="docx", key="uploader"
)

# Process the file only if it hasn't been processed yet.
if uploaded_file is not None and not st.session_state.file_processed:
    file_data = uploaded_file.read()
    st.session_state.uploaded_file_data = file_data
    st.session_state.uploaded_file_name = uploaded_file.name

    file_path = f"temp_uploads/{st.session_state.uploaded_file_name}"
    os.makedirs("temp_uploads", exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(file_data)

    logging.info(f"File uploaded: {st.session_state.uploaded_file_name}")
    st.info("Sending document paragraphs to Kafka...")

    try:
        producer = KafkaProducer(bootstrap_servers="kafka:9092")
        doc = Document(file_path)
        sent_count = 0
        chunk = ""
        for para in doc.paragraphs:
            text = para.text.strip()
            if text:
                chunk += text
            if len(chunk) >= 2048:
                logging.info(f"Sending chunk: {chunk}")
                producer.send("docx-topic", value=chunk.encode("utf-8"))
                time.sleep(0.1)
                sent_count += 1
                chunk = ""
        if chunk:
            logging.info(f"Sending chunk: {chunk}")
            producer.send("docx-topic", value=chunk.encode("utf-8"))
            time.sleep(0.1)
            sent_count += 1

        producer.flush()
        st.success("Document sent to Kafka. Processing...")
        logging.info(f"Sent {sent_count} chunks to Kafka topic 'docx-topic'.")
    except Exception as e:
        st.error("Failed to send data to Kafka.")
        logging.error(f"Kafka error: {str(e)}")

    st.session_state.file_processed = True


# Function to read summary from SQLite
def get_summary_from_db():
    try:
        conn = sqlite3.connect("/tmp/shared/summary.db")
        cursor = conn.cursor()
        cursor.execute("SELECT summary_text FROM summary ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        if row:
            return row[0]
    except Exception as e:
        st.error(f"Error reading summary: {e}")
    return None


# Poll the SQLite database for the summary after processing
if st.session_state.file_processed:
    with st.spinner("Waiting for summary from SQLite..."):
        summary = None
        max_attempts = 60  # e.g., wait up to 120 seconds total
        for attempt in range(max_attempts):
            summary = get_summary_from_db()
            if summary:
                break
            time.sleep(2)
        if summary:
            st.subheader("Summarized Topics")
            st.text(summary)
        else:
            st.error("Summary not available. Please try again later.")

# Reset button to allow uploading another file
if st.session_state.file_processed:
    if st.button("Upload another file"):
        st.session_state.file_processed = False
        st.session_state.uploaded_file_data = None
        st.session_state.uploaded_file_name = None
        st.rerun()

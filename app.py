import os
import re
import json
import google.generativeai as genai
from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

app = Flask(__name__)
CORS(app)

# --- 1. Configure Gemini API ---
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable not set. Please set it in your .env file.")

genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-2.0-flash')

# --- 2. Initialize Spark Session and Load Device Data ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_PARQUET_PATH = os.path.join(BASE_DIR, "data", "processed_parquet")

spark = None
df_device_activity = None

def init_spark_and_load_data():
    global spark, df_device_activity

    spark = SparkSession.builder \
        .appName("ChatbotSparkIntegration") \
        .master("local[*]") \
        .getOrCreate()
    print("Apache Spark Session initialized.")
    print(f"Spark Version: {spark.version}")

    print(f"Attempting to read device activity data from Parquet at: {PROCESSED_PARQUET_PATH}")
    try:
        if not os.path.exists(PROCESSED_PARQUET_PATH):
            raise FileNotFoundError(f"Direktori Parquet tidak ditemukan: {PROCESSED_PARQUET_PATH}")
        
        df_device_activity = spark.read.parquet(PROCESSED_PARQUET_PATH)
        df_device_activity.createOrReplaceTempView("device_activity_logs")
        print("Device Activity DataFrame loaded from Parquet.")
        print("Schema from Parquet:")
        #df_device_activity.printSchema()
        df_device_activity.show(5, truncate=False)
    except Exception as e:
        df_device_activity = None
        print(f"WARNING: Could not load device activity data from Parquet. "
              f"Ensure '{PROCESSED_PARQUET_PATH}' exists and contains valid Parquet files. "
              f"Detail: {e}")
        print("Device activity related queries might not work.")

# --- 3. Function to Retrieve Data from Spark DataFrames ---
def get_data_from_spark(table_name, condition_col=None, condition_val=None, like_match=False, limit=None, order_by_col=None, order_desc=True):
    global spark, df_device_activity

    df = None
    if table_name == "device_activity_logs":
        df = df_device_activity
        if df is None:
            print(f"Error: DataFrame for '{table_name}' is not loaded.")
            return []
    else:
        print(f"Error: Unknown table name '{table_name}' or DataFrame not loaded.")
        return []

    if df is None:
        print(f"Error: DataFrame for '{table_name}' is not initialized.")
        return []

    if condition_col and condition_val:
        if like_match:
            df = df.filter(col(condition_col).like(f"%{condition_val}%"))
        else:
            df = df.filter(col(condition_col) == condition_val) # Pencocokan eksak
            
    if order_by_col and order_by_col in df.columns:
        if order_desc:
            df = df.orderBy(desc(order_by_col))
        else:
            df = df.orderBy(col(order_by_col))

    if limit:
        df = df.limit(limit)

    try:
        return df.collect()
    except Exception as e:
        print(f"Error collecting data from Spark DataFrame: {e}")
        return []

# --- 4. Intent Detection and Entity Extraction ---
def detect_intent_and_extract_entities(user_query):
    query_lower = user_query.lower()
    intent = "unknown"
    entities = {}

    if "status" in query_lower or "cek" in query_lower or "perangkat" in query_lower or "device" in query_lower:
        intent = "check_device_status"
        
        # PRIORITAS 1: Coba ekstrak ID perangkat spesifik
        # Pola ini mencoba menangkap:
        # - ABCNNN (e.g., DEV001, SWI005)
        # - XXX-YYY-NNN (e.g., OLT-BDG-001, ROUTER-SBY-003)
        match_device_id = re.search(r'([a-z]{2,5}\d{2,5}|[a-z]{3,5}-\w{3}-\d{3})', query_lower)
        if match_device_id:
            entities['device_id'] = match_device_id.group(1).upper()
            
        # Coba ekstrak MODEL perangkat umum (e.g., Router, OLT Huawei, Switch Cisco)
        # Ini akan digunakan jika ID spesifik tidak ditemukan atau sebagai informasi tambahan
        # Tambahkan kata kunci yang relevan dengan model perangkat Anda
        match_device_model = re.search(r'(olt|router|server|switch|huawei|cisco|zte|samsung|dev \d+|model \w+)', query_lower)
        if match_device_model:
            # Menggunakan match_device_model.group(0) untuk menangkap seluruh pola yang cocok
            entities['device_model_query'] = match_device_model.group(0) # Nama yang berbeda agar tidak bingung dengan kolom 'device_model' di data
            
    return intent, entities

# --- 5. Function to Retrieve Context from Spark Data ---
def get_context_from_spark_data(intent, entities):
    context = ""
    if intent == "check_device_status":
        device_id_specific = entities.get('device_id')
        device_model_query = entities.get('device_model_query') # Gunakan nama baru untuk entitas model dari query

        # Logika utama: Jika ada ID spesifik, coba cari itu terlebih dahulu
        if device_id_specific:
            print(f"DEBUG: Mencari perangkat dengan ID spesifik: {device_id_specific}")
            result = get_data_from_spark("device_activity_logs", "device_id", device_id_specific, 
                                         like_match=False, limit=1, order_by_col="timestamp", order_desc=True)
            if result:
                row = result[0]
                context = (
                    f"Konteks: Status perangkat {row.device_id} ({row.device_model if hasattr(row, 'device_model') else 'Model Tidak Diketahui'}): "
                    f"Saat ini {row.status} di {row.location if hasattr(row, 'location') else 'Lokasi Tidak Diketahui'}. "
                    f"Terakhir tercatat pada {row.timestamp if hasattr(row, 'timestamp') else 'Waktu Tidak Diketahui'}."
                )
            else:
                # Jika ID spesifik tidak ditemukan, coba cari berdasarkan model yang disebutkan dalam query
                if device_model_query:
                    print(f"DEBUG: ID spesifik '{device_id_specific}' tidak ditemukan. Mencoba mencari berdasarkan model '{device_model_query}'.")
                    # Lakukan pencarian LIKE pada kolom 'device_model'
                    result = get_data_from_spark("device_activity_logs", "device_model", device_model_query,
                                                 like_match=True, limit=5, order_by_col="timestamp", order_desc=True)
                    if result:
                        context_parts = [f"Konteks: Perangkat dengan ID '{device_id_specific}' tidak ditemukan secara eksak, namun berikut beberapa perangkat dengan model serupa ({device_model_query}) atau yang terkait:"]
                        seen_devices = set()
                        for row in result:
                            if row.device_id not in seen_devices:
                                context_parts.append(f"- {row.device_id} ({row.device_model if hasattr(row, 'device_model') else 'Model Tidak Diketahui'}): {row.status} di {row.location if hasattr(row, 'location') else 'Lokasi Tidak Diketahui'}. Terakhir tercatat pada {row.timestamp if hasattr(row, 'timestamp') else 'Waktu Tidak Diketahui'}.")
                                seen_devices.add(row.device_id)
                        context = "\n".join(context_parts)
                    else:
                        context = f"Konteks: Maaf, data status untuk perangkat '{device_id_specific}' atau model '{device_model_query}' tidak ditemukan di log aktivitas."
                else:
                    context = f"Konteks: Maaf, data status untuk perangkat '{device_id_specific}' tidak ditemukan di log aktivitas."

        elif device_model_query: # Hanya ada model umum, cari beberapa perangkat dari model itu
            print(f"DEBUG: Mencari perangkat dengan model umum: {device_model_query}")
            result = get_data_from_spark("device_activity_logs", "device_model", device_model_query,
                                         like_match=True, limit=5, order_by_col="timestamp", order_desc=True)
            if result:
                context_parts = [f"Konteks: Berikut adalah beberapa status terbaru untuk perangkat dengan model '{device_model_query}':"]
                seen_devices = set()
                for row in result:
                    if row.device_id not in seen_devices:
                        context_parts.append(f"- {row.device_id} ({row.device_model if hasattr(row, 'device_model') else 'Model Tidak Diketahui'}): {row.status} di {row.location if hasattr(row, 'location') else 'Lokasi Tidak Diketahui'}. Terakhir tercatat pada {row.timestamp if hasattr(row, 'timestamp') else 'Waktu Tidak Diketahui'}."
                        )
                        seen_devices.add(row.device_id)
                context = "\n".join(context_parts)
            else:
                context = f"Konteks: Tidak ditemukan informasi log aktivitas untuk perangkat dengan model '{device_model_query}'."
        else: # Generic status check without specific device_id or model
            context = "Konteks: Pengguna ingin mengetahui status perangkat secara umum. Informasikan bahwa Anda memerlukan ID perangkat yang spesifik atau model perangkat (misalnya, OLT Huawei, Router Cisco) untuk detail lebih lanjut, atau tanyakan ID perangkat apa yang ingin diketahui statusnya."
    else:
        context = "Konteks: Pertanyaan ini tidak terkait dengan data perangkat yang saya miliki. Saya hanya bisa memberikan informasi mengenai status perangkat."
    return context

# --- 6. Main Function to Interact with Gemini (Streaming) ---
def generate_gemini_response_stream(user_query):
    intent, entities = detect_intent_and_extract_entities(user_query)
    db_context = get_context_from_spark_data(intent, entities)

    llm_role = "Anda adalah customer service yang ramah dan responsif, khusus untuk layanan jaringan dan pemantauan perangkat."
    
    prompt_parts = [
        f"{llm_role}",
        db_context,
        "---",
        f"Pertanyaan Pengguna: {user_query}",
        "---",
        "Berikan jawaban yang ramah, informatif, dan ringkas berdasarkan konteks yang diberikan. "
        "Jika konteks dari data perangkat tidak memberikan informasi yang cukup, katakan bahwa Anda tidak memiliki data spesifik atau butuh informasi lebih lanjut. "
        "Jika pertanyaan tidak terkait dengan perangkat, informasikan bahwa Anda hanya bisa memberikan informasi status perangkat."
    ]

    full_prompt = "\n".join(filter(None, prompt_parts))

    try:
        response_stream = gemini_model.generate_content(full_prompt, stream=True)

        for chunk in response_stream:
            if hasattr(chunk, 'text') and chunk.text:
                yield json.dumps({"text": chunk.text}) + "\n"

    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        yield json.dumps({"error": f"Maaf, ada masalah teknis di server: {str(e)}"}) + "\n"

# --- Flask API Endpoint ---
@app.route('/chat', methods=['POST'])
def chat_endpoint():
    user_message = request.json.get('message')

    if not user_message:
        return jsonify({"error": "Pesan tidak boleh kosong"}), 400

    return Response(stream_with_context(generate_gemini_response_stream(user_message)), mimetype='application/x-ndjson')

# --- Inisialisasi Spark dan Load Data saat aplikasi Flask dimulai ---
with app.app_context():
    init_spark_and_load_data()

if __name__ == '__main__':
    if not os.getenv("GEMINI_API_KEY"):
        print("Peringatan: Variabel lingkungan 'GEMINI_API_KEY' tidak ditemukan. Harap setel di file .env atau environment Anda.")
    
    import atexit
    def stop_spark_on_exit():
        global spark
        if spark:
            spark.stop()
            print("SparkSession stopped gracefully.")
    atexit.register(stop_spark_on_exit)

    app.run(debug=True, port=5000)
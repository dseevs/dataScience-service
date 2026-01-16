from flask import Flask, request, jsonify
from app.service.messageService import MessageService
from kafka import KafkaProducer
import json
import os

# =========================
# Flask App
# =========================
app = Flask(__name__)
app.config.from_pyfile("config.py")

# =========================
# Services
# =========================
messageService = MessageService()

# =========================
# Kafka (lazy initialization)
# =========================
producer = None


def create_kafka_producer():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    app.logger.info(f"Connecting to Kafka at {kafka_servers}")

    return KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )


# =========================
# Routes
# =========================
@app.route("/v1/ds/message", methods=["POST"])
def handle_message():
    global producer

    # 1️⃣ Safe JSON parsing
    data = request.get_json(silent=True)
    if not data or "message" not in data:
        return jsonify({"error": "Missing 'message' field"}), 400

    message = data["message"]

    # 2️⃣ Process message
    result = messageService.process_message(message)
    if result is None:
        return jsonify({"error": "Invalid message format"}), 400

   
    serialized_result = result.model_dump()


    # 3️⃣ Lazy Kafka connection
    if producer is None:
        try:
            producer = create_kafka_producer()
        except Exception as e:
            app.logger.error(f"Kafka connection failed: {e}")
            return jsonify({"error": "Kafka unavailable"}), 503

    # 4️⃣ Send to Kafka safely
    try:
        producer.send("expense_service", serialized_result)
    except Exception as e:
        app.logger.error(f"Kafka send failed: {e}")
        return jsonify({"error": "Failed to publish message"}), 500

    # 5️⃣ Success
    return jsonify(serialized_result), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "UP"}), 200


@app.route("/", methods=["GET"])
def home():
    return "Hello world", 200

# if __name__ == "__main__":
#     app.run(host="localhost", port= 8010 ,debug=True)

# from flask import Flask
# from flask import request, jsonify
# from service.messageService import MessageService
# from kafka import KafkaProducer
# app = Flask(__name__)
# app.config.from_pyfile('config.py')

# messageService = MessageService()
# producer = KafkaProducer(
#     bootstrap_servers=['localhost:9092'],  # NOT localhost
#     api_version=(3, 7, 0),
#     value_serializer=lambda x: x.encode("utf-8")
# )



# @app.route('/v1/ds/message', methods=['POST'])
# def handle_message():
#     message = request.json.get('message')
#     result = messageService.process_message(message)
    
#     serialized_result=result.json()
#     producer.send('processed_messages', value=serialized_result)
    
#     return jsonify(result.model_dump())

# @app.route('/', methods=['GET'])
# def handle_get():
#     return 'Hello world'


# if __name__ == "__main__":
#     app.run(host="localhost", port= 8000 ,debug=True)
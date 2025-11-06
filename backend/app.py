# backend/app.py
import os
import json
import threading
from datetime import datetime
from flask import Flask, jsonify, request
from pymongo import MongoClient
import paho.mqtt.client as mqtt
from flask_cors import CORS

# Config
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "mqdb")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "readings")

MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
MQTT_TOPICS = os.environ.get("MQTT_TOPICS", "sensors/mq135/#")  # comma separated

app = Flask(__name__)
CORS(app)  # allow cross-origin requests for React dev; tighten for prod

# Mongo client (shared)
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# REST endpoints
@app.route("/health")
def health():
    return jsonify({"status":"ok", "time": datetime.utcnow().isoformat()})

@app.route("/latest", methods=["GET"])
def latest():
    # Return last N documents
    limit = int(request.args.get("limit", 100))
    docs = list(collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))
    return jsonify(docs)

@app.route("/messages", methods=["GET"])
def messages():
    """ Same as /latest but clearer naming for React """
    return latest()

# MQTT callbacks
def on_connect(client, userdata, flags, rc):
    print("MQTT connected with rc:", rc)
    topics = [t.strip() for t in MQTT_TOPICS.split(",") if t.strip()]
    for t in topics:
        client.subscribe((t, 0))
        print("Subscribed to:", t)

def on_message(client, userdata, msg):
    try:
        payload_raw = msg.payload.decode("utf-8", errors="ignore")
        # try to parse JSON, otherwise store as string
        try:
            payload = json.loads(payload_raw)
        except Exception:
            payload = {"raw": payload_raw}
        doc = {
            "topic": msg.topic,
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat()
        }
        collection.insert_one(doc)
        print("Inserted doc:", doc)
    except Exception as e:
        print("Error handling message:", e)

def mqtt_thread():
    client = mqtt.Client()
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    # Start MQTT background thread
    t = threading.Thread(target=mqtt_thread, daemon=True)
    t.start()
    # Start Flask app
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

import json
import ssl
import os
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
import paho.mqtt.client as mqtt

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

# 1. Cấu hình Database PostgreSQL (Supabase)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres.hlqvfbtqsuudxbglelmu:[YOUR-PASSWORD]@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Định nghĩa bảng sensor_logs trong SQLAlchemy
class SensorLog(Base):
    __tablename__ = "sensor_logs"

    id = Column(Integer, primary_key=True, index=True)
    node_id = Column(String(50), nullable=False)
    temperature = Column(Float)
    humidity = Column(Float)
    soil_moisture = Column(Float)
    light_lux = Column(Float)
    battery = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(bind=engine)
# 2. Cấu hình MQTT Broker (EMQX Cloud)
MQTT_BROKER = "qe66bb68.ala.eu-central-1.emqxsl.com"
MQTT_PORT = 8883
MQTT_TOPIC = "farm/durian/nodes/+"
MQTT_USER = "BACTUYEN"
MQTT_PASSWORD = "1"

class SensorPayload(BaseModel):
    temperature: float
    humidity: float
    soil_moisture: float
    light_lux: float
    battery: float

def save_to_db(node_id: str, data: SensorPayload):
    """Hàm ghi dữ liệu cảm biến vào Supabase PostgreSQL"""
    db = SessionLocal()
    try:
        log_entry = SensorLog(
            node_id=node_id,
            temperature=data.temperature,
            humidity=data.humidity,
            soil_moisture=data.soil_moisture,
            light_lux=data.light_lux,
            battery=data.battery
        )
        db.add(log_entry)
        db.commit()
        print(f"[Database] Đã lưu dữ liệu thành công cho {node_id}")
    except Exception as e:
        db.rollback()
        print(f"[Database Error] Lỗi lưu dữ liệu: {e}")
    finally:
        db.close()

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[MQTT] Đã kết nối an toàn (TLS) tới EMQX Cloud!")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[MQTT] Lỗi kết nối (rc): {rc}")

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        raw_data = json.loads(payload_str)
        node_id = msg.topic.split("/")[-1]
        
        data = SensorPayload(**raw_data)
        
        # Lưu dữ liệu nhận từ MQTT vào Supabase Database
        save_to_db(node_id, data)
        
    except Exception as e:
        print(f"[MQTT Processing Error] {e}")

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[System] Đang kết nối EMQX Cloud...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"[MQTT Error] {e}")
    
    yield
    
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

app = FastAPI(title="Durian IoT Agriculture Monitoring", lifespan=lifespan)

@app.get("/")
def root():
    return {"status": "online", "message": "Durian IoT Backend running"}

@app.get("/api/v1/sensors/latest")
def get_latest_sensor_data():
    """API lấy 10 bản ghi mới nhất từ Database cho Web Dashboard"""
    db = SessionLocal()
    try:
        logs = db.query(SensorLog).order_by(SensorLog.created_at.desc()).limit(10).all()
        return logs
    finally:
        db.close()
import json
import ssl
import os
import io
import httpx
import pandas as pd
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import paho.mqtt.client as mqtt

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

# Dùng bcrypt & pyjwt thuần cho xác thực tài khoản (Tránh crash passlib trên Render)
import bcrypt
import jwt

# ==========================================
# 1. Cấu hình Database PostgreSQL (Supabase)
# ==========================================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres.hlqvfbtqsuudxbglelmu:bactuyen3122004@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ----------------- SQLAlchemy Models -----------------

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

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), nullable=True)
    hashed_password = Column(String, nullable=False)
    full_name = Column(String(100), nullable=True)
    role = Column(String(20), default="user") # 'admin' hoặc 'user'
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class AlertSetting(Base):
    __tablename__ = "alert_settings"

    id = Column(Integer, primary_key=True, index=True)
    node_id = Column(String(50), unique=True, nullable=False)
    min_temp = Column(Float, default=15.0)
    max_temp = Column(Float, default=38.0)
    min_soil_moisture = Column(Float, default=50.0)
    max_soil_moisture = Column(Float, default=80.0)
    min_humidity = Column(Float, default=40.0)
    max_humidity = Column(Float, default=90.0)
    created_at = Column(DateTime, default=datetime.utcnow)

class DeviceControl(Base):
    __tablename__ = "device_controls"

    id = Column(Integer, primary_key=True, index=True)
    node_id = Column(String(50), unique=True, nullable=False)
    pump_status = Column(Boolean, default=False)
    mode = Column(String(20), default="manual") # 'manual' hoặc 'auto'
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# ==========================================
# 2. Cấu hình MQTT Broker (EMQX Cloud)
# ==========================================
MQTT_BROKER = "qe66bb68.ala.eu-central-1.emqxsl.com"
MQTT_PORT = 8883
MQTT_TOPIC = "farm/durian/nodes/+"
MQTT_USER = "BACTUYEN"
MQTT_PASSWORD = "1"

# ==========================================
# 3. Security / Password / JWT Setup
# ==========================================
SECRET_KEY = os.getenv("JWT_SECRET", "durian_secret_key_2026")
ALGORITHM = "HS256"

def get_password_hash(password: str) -> str:
    """Băm mật khẩu an toàn bằng bcrypt thuần"""
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pwd_bytes, salt).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Xác thực mật khẩu"""
    try:
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except Exception:
        return False

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(hours=24))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# ==========================================
# 4. Pydantic Schemas
# ==========================================
class SensorPayload(BaseModel):
    temperature: float
    humidity: float
    soil_moisture: float
    light_lux: float
    battery: float

class UserCreate(BaseModel):
    username: str
    password: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    role: Optional[str] = "user"

class UserLogin(BaseModel):
    username: str
    password: str

class ThresholdUpdate(BaseModel):
    node_id: str
    min_temp: float = 15.0
    max_temp: float = 38.0
    min_soil_moisture: float = 50.0
    max_soil_moisture: float = 80.0

class PumpControlRequest(BaseModel):
    node_id: str
    action: str # "ON" hoặc "OFF"

class ChatRequest(BaseModel):
    message: str

# ==========================================
# 5. Telegram & Threshold Logic
# ==========================================
def send_telegram_alert(message: str):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    
    if not bot_token or not chat_id:
        return
        
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        httpx.post(url, json=payload, timeout=5.0)
        print("[Telegram Alert] Đã gửi thông báo cảnh báo thành công!")
    except Exception as e:
        print(f"[Telegram Alert Error] Lỗi gửi tin nhắn: {e}")

def check_thresholds_and_alert(db, node_id: str, data: SensorPayload):
    try:
        setting = db.query(AlertSetting).filter(AlertSetting.node_id == node_id).first()
        if not setting:
            return
        
        alerts = []
        if data.soil_moisture < setting.min_soil_moisture:
            alerts.append(f"⚠️ <b>Độ ẩm đất thấp ({data.soil_moisture}% < {setting.min_soil_moisture}%)</b> - Cần tưới nước!")
        elif data.soil_moisture > setting.max_soil_moisture:
            alerts.append(f"🚨 <b>Độ ẩm đất quá cao ({data.soil_moisture}% > {setting.max_soil_moisture}%)</b> - Nguy cơ ngập úng!")
            
        if data.temperature > setting.max_temp:
            alerts.append(f"🔥 <b>Nhiệt độ cao ({data.temperature}°C > {setting.max_temp}°C)</b> - Chú ý che chắn cho cây!")
        elif data.temperature < setting.min_temp:
            alerts.append(f"❄️ <b>Nhiệt độ xuống thấp ({data.temperature}°C < {setting.min_temp}°C)</b>!")

        if alerts:
            msg = f"🔔 <b>CẢNH BÁO TẠI {node_id.upper()}</b>\n" + "\n".join(alerts)
            send_telegram_alert(msg)
    except Exception as e:
        print(f"[Threshold Check Error] {e}")

# ==========================================
# 6. MQTT Processing
# ==========================================
def save_to_db(node_id: str, data: SensorPayload):
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
        
        check_thresholds_and_alert(db, node_id, data)
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

# ==========================================
# 7. FastAPI App Definition & Middleware
# ==========================================
app = FastAPI(title="Durian IoT Agriculture Monitoring", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 8. API ENDPOINTS
# ==========================================

@app.get("/")
def root():
    return {"status": "online", "message": "Durian IoT Backend running with full features"}

# ----------------- A. Cảm biến -----------------
@app.get("/api/v1/sensors/latest")
def get_latest_sensor_data(limit: int = 10):
    db = SessionLocal()
    try:
        logs = db.query(SensorLog).order_by(SensorLog.created_at.desc()).limit(limit).all()
        return logs
    finally:
        db.close()

# ----------------- B. Quản lý Tài khoản (Auth & Users) -----------------
@app.post("/api/v1/auth/login")
def login(req: UserLogin):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == req.username).first()
        if not user or not verify_password(req.password, user.hashed_password):
            raise HTTPException(status_code=401, detail="Tài khoản hoặc mật khẩu không chính xác")
        if not user.is_active:
            raise HTTPException(status_code=403, detail="Tài khoản đã bị khóa")
        
        token = create_access_token({"sub": user.username, "role": user.role})
        return {
            "access_token": token,
            "token_type": "bearer",
            "role": user.role,
            "username": user.username,
            "full_name": user.full_name
        }
    finally:
        db.close()

@app.post("/api/v1/admin/users")
def create_user(req: UserCreate):
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.username == req.username).first()
        if existing:
            raise HTTPException(status_code=400, detail="Tên tài khoản đã tồn tại")
        
        new_user = User(
            username=req.username,
            email=req.email,
            hashed_password=get_password_hash(req.password),
            full_name=req.full_name,
            role=req.role or "user"
        )
        db.add(new_user)
        db.commit()
        return {"status": "success", "message": f"Tạo tài khoản {req.username} thành công"}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/v1/admin/users")
def list_users():
    db = SessionLocal()
    try:
        users = db.query(User).all()
        return [{
            "id": u.id,
            "username": u.username,
            "email": u.email,
            "full_name": u.full_name,
            "role": u.role,
            "is_active": u.is_active,
            "created_at": u.created_at
        } for u in users]
    finally:
        db.close()

# ----------------- C. Cấu hình Ngưỡng Cảnh báo -----------------
@app.get("/api/v1/thresholds")
def get_thresholds():
    db = SessionLocal()
    try:
        return db.query(AlertSetting).all()
    finally:
        db.close()

@app.post("/api/v1/thresholds")
def update_threshold(req: ThresholdUpdate):
    db = SessionLocal()
    try:
        setting = db.query(AlertSetting).filter(AlertSetting.node_id == req.node_id).first()
        if not setting:
            setting = AlertSetting(
                node_id=req.node_id,
                min_temp=req.min_temp,
                max_temp=req.max_temp,
                min_soil_moisture=req.min_soil_moisture,
                max_soil_moisture=req.max_soil_moisture
            )
            db.add(setting)
        else:
            setting.min_temp = req.min_temp
            setting.max_temp = req.max_temp
            setting.min_soil_moisture = req.min_soil_moisture
            setting.max_soil_moisture = req.max_soil_moisture
        db.commit()
        return {"status": "success", "message": f"Cập nhật ngưỡng cho {req.node_id} thành công"}
    finally:
        db.close()

# ----------------- D. Điều khiển Bật/Tắt Máy Bơm Tưới -----------------
@app.post("/api/v1/control/pump")
def control_pump(req: PumpControlRequest):
    db = SessionLocal()
    try:
        action_upper = req.action.upper()
        status_bool = (action_upper == "ON")
        
        ctrl = db.query(DeviceControl).filter(DeviceControl.node_id == req.node_id).first()
        if not ctrl:
            ctrl = DeviceControl(node_id=req.node_id, pump_status=status_bool, mode="manual")
            db.add(ctrl)
        else:
            ctrl.pump_status = status_bool
            ctrl.updated_at = datetime.utcnow()
        db.commit()

        topic = f"farm/durian/control/{req.node_id}"
        payload = json.dumps({"pump": action_upper, "node_id": req.node_id})
        mqtt_client.publish(topic, payload)

        return {"status": "success", "node_id": req.node_id, "pump": action_upper}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/api/v1/control/status")
def get_device_status():
    db = SessionLocal()
    try:
        return db.query(DeviceControl).all()
    finally:
        db.close()

# ----------------- E. Thời tiết (OpenWeatherMap) -----------------
@app.get("/api/v1/weather")
def get_weather(city: str = "Tien Giang"):
    api_key = os.getenv("OPENWEATHER_API_KEY", "")
    if not api_key:
        return {
            "status": "demo",
            "temp": 31.5,
            "humidity": 75,
            "description": "Nắng nhẹ, mây rải rác",
            "rain_1h": 0.0
        }
    
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric&lang=vi"
    try:
        resp = httpx.get(url, timeout=5.0)
        data = resp.json()
        return {
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "description": data["weather"][0]["description"],
            "rain_1h": data.get("rain", {}).get("1h", 0.0)
        }
    except Exception as e:
        return {"error": f"Lỗi lấy thông tin thời tiết: {str(e)}"}

# ----------------- F. Xuất Báo cáo Excel -----------------
@app.get("/api/v1/export/excel")
def export_excel(node_id: Optional[str] = None):
    db = SessionLocal()
    try:
        query = db.query(SensorLog)
        if node_id:
            query = query.filter(SensorLog.node_id == node_id)
            
        logs = query.order_by(SensorLog.created_at.desc()).limit(1000).all()
        
        data = [{
            "ID": log.id,
            "Mã Node": log.node_id,
            "Nhiệt độ (°C)": log.temperature,
            "Độ ẩm KK (%)": log.humidity,
            "Độ ẩm đất (%)": log.soil_moisture,
            "Cường độ ánh sáng (Lux)": log.light_lux,
            "Dung lượng Pin (%)": log.battery,
            "Thời gian ghi nhận": log.created_at.strftime("%Y-%m-%d %H:%M:%S") if log.created_at else ""
        } for log in logs]
        
        df = pd.DataFrame(data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sensor History')
        output.seek(0)
        
        headers = {'Content-Disposition': 'attachment; filename="Bao_Cao_Nong_Nghiep_Sau_Rieng.xlsx"'}
        return StreamingResponse(
            output, 
            headers=headers, 
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi xuất file Excel: {str(e)}")
    finally:
        db.close()

# ----------------- G. Chatbot AI Chẩn đoán Sức khỏe Sầu riêng (Gemini API) -----------------
@app.post("/api/v1/chat")
def chat_with_durian_ai(req: ChatRequest):
    """API Chatbot AI tư vấn sức khỏe cây sầu riêng (Context Injection)"""
    gemini_api_key = os.getenv("GEMINI_API_KEY", "")
    if not gemini_api_key:
        raise HTTPException(
            status_code=500, 
            detail="Chưa cấu hình GEMINI_API_KEY trong biến môi trường"
        )
    
    db = SessionLocal()
    try:
        # 1. Truy vấn dữ liệu cảm biến 6 bản ghi mới nhất từ Supabase
        recent_logs = db.query(SensorLog).order_by(SensorLog.created_at.desc()).limit(6).all()
        sensor_context = []
        for log in recent_logs:
            sensor_context.append(
                f"- Node: {log.node_id} | Nhiệt độ: {log.temperature}°C | "
                f"Độ ẩm đất: {log.soil_moisture}% | Độ ẩm KK: {log.humidity}% | "
                f"Ánh sáng: {log.light_lux} Lux | Thời gian: {log.created_at}"
            )
        sensor_data_str = "\n".join(sensor_context) if sensor_context else "Chưa có dữ liệu cảm biến."

        # 2. Lấy dữ liệu thời tiết ngoài trời
        weather_info = get_weather("Tien Giang")
        weather_str = (
            f"Thời tiết ngoài trời: {weather_info.get('description', 'N/A')}, "
            f"Nhiệt độ: {weather_info.get('temp', 'N/A')}°C, "
            f"Lượng mưa 1h: {weather_info.get('rain_1h', 0)}mm"
        )

        # 3. Tạo System Prompt (Context Injection)
        system_prompt = f"""
Bạn là Chuyên gia Kỹ thuật Nông nghiệp Thông minh chuyên sâu về chăm sóc cây Sầu Riêng.
Dưới đây là DỮ LIỆU THỜI GIAN THỰC từ hệ thống cảm biến trong vườn:

[DỮ LIỆU CẢM BIẾN TRONG VƯỜN]
{sensor_data_str}

[THỜI TIẾT NGOÀI TRỜI]
{weather_str}

[YÊU CẦU DÀNH CHO BẠN]
- Trả lời câu hỏi của nông dân một cách ngắn gọn, súc tích, thân thiện và chuyên nghiệp.
- Dựa trực tiếp vào các thông số cảm biến (đặc biệt là Độ ẩm đất và Nhiệt độ) để đưa ra chẩn đoán chính xác cho từng Node/Gốc cây.
- Tiêu chuẩn sầu riêng: Độ ẩm đất lý tưởng 60% - 70% (dưới 50% là thiếu nước, trên 80% nguy cơ úng rễ); Nhiệt độ thích hợp 24°C - 32°C.
- Đưa ra lời khuyên cụ thể: Có nên bật máy bơm tưới nước lúc này không, hoặc có cần che chắn/bón phân gì không.

Câu hỏi của nông dân: "{req.message}"
"""

        # 4. Gọi Gemini API (Google AI)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={gemini_api_key}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{
                "parts": [{"text": system_prompt}]
            }]
        }

        with httpx.Client(timeout=15.0) as client:
            response = client.post(url, json=payload, headers=headers)
            res_data = response.json()
            
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail=f"Lỗi Gemini API: {res_data}")
                
            reply_text = res_data["candidates"][0]["content"]["parts"][0]["text"]
            return {"reply": reply_text}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi xử lý Chatbot: {str(e)}")
    finally:
        db.close()
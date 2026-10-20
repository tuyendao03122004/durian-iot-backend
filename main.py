import json
import ssl
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel, Field
import paho.mqtt.client as mqtt

# 1. Cấu hình kết nối EMQX Cloud
MQTT_BROKER = "qe66bb68.ala.eu-central-1.emqxsl.com"[cite: 7, 9]
MQTT_PORT = 8883[cite: 7, 9]
MQTT_TOPIC = "farm/durian/nodes/+"[cite: 7, 9]

MQTT_USER = "BACTUYEN"[cite: 7, 9]
MQTT_PASSWORD = "1"[cite: 7, 9]


# 2. Schema kiểm tra kiểu dữ liệu (Data Validation) bằng Pydantic
class SensorPayload(BaseModel):
    temperature: float = Field(..., description="Nhiệt độ không khí từ SHT30 (°C)")
    humidity: float = Field(..., description="Độ ẩm không khí từ SHT30 (%)")
    soil_moisture: float = Field(..., description="Độ ẩm đất (%)")
    light_lux: float = Field(..., description="Cường độ ánh sáng từ BH1750 (Lux)")
    battery: float = Field(..., description="Dung lượng pin (%)")


# 3. Callback khi kết nối thành công MQTT Broker
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("[MQTT] Đã kết nối an toàn (TLS) tới EMQX Cloud!")[cite: 7, 9]
        client.subscribe(MQTT_TOPIC)[cite: 7, 9]
        print(f"[MQTT] Đã subscribe topic: {MQTT_TOPIC}")[cite: 7, 9]
    else:
        print(f"[MQTT] Lỗi kết nối, Mã lỗi (rc): {rc}")[cite: 7, 9]


# 4. Callback khi nhận dữ liệu từ các Node
def on_message(client, userdata, msg):
    try:
        topic = msg.topic[cite: 9]
        payload_str = msg.payload.decode("utf-8")
        raw_data = json.loads(payload_str)

        # Lấy ID của Node từ Topic (ví dụ: farm/durian/nodes/node_1 -> node_1)
        node_id = topic.split("/")[-1][cite: 9]

        # Validate dữ liệu thông qua Pydantic
        data = SensorPayload(**raw_data)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"\n[{timestamp}] [MQTT Received] Trạm: {node_id}")
        print(f" ├─ SHT30 Nhiệt độ: {data.temperature} °C")
        print(f" ├─ SHT30 Độ ẩm KK: {data.humidity} %")
        print(f" ├─ Độ ẩm đất     : {data.soil_moisture} %")
        print(f" ├─ BH1750 Cường độ sáng: {data.light_lux} Lux")
        print(f" └─ Dung lượng Pin: {data.battery} %")

        # TODO: Lưu dữ liệu 'data' vào Cơ sở dữ liệu (PostgreSQL / TimescaleDB)

    except json.JSONDecodeError:
        print(f"[MQTT Error] Payload không phải chuỗi JSON hợp lệ: {msg.payload}")
    except Exception as e:
        print(f"[MQTT Error] Lỗi xử lý dữ liệu từ {msg.topic}: {e}")


# Khởi tạo MQTT Client
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)[cite: 7, 9]
mqtt_client.on_connect = on_connect[cite: 7, 9]
mqtt_client.on_message = on_message[cite: 7, 9]
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)[cite: 7, 9]
mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)[cite: 7, 9]


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[System] Đang kết nối tới EMQX Cloud...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"[MQTT Error] Không thể kết nối: {e}")
    
    yield
    
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

app = FastAPI(title="IoT Agricultural Monitor Gateway", lifespan=lifespan)

@app.get("/")
def read_root():
    return {
        "message": "IoT Agricultural Monitor Gateway Backend đang hoạt động",
        "docs_url": "http://127.0.0.1:8000/docs"
    }
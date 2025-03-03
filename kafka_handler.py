from confluent_kafka import Consumer
from dotenv import load_dotenv
import time
import os
import json
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime, text, func
from sqlalchemy.inspection import inspect
from modbus_mapping import GoodWe
# import queue
import threading
import concurrent.futures
from ecu_1051 import CleanECU1051
from pathlib import Path
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import random
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
DATABASE_URL = os.getenv("DATABASE_URL")

# INFLUXDB_HOST = os.getenv("INFLUXDB_HOST", "localhost")
# INFLUXDB_PORT = int(os.getenv("INFLUXDB_PORT", 8086))
# INFLUXDB_DB = os.getenv("INFLUXDB_DB", "iot_metrics")


engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

kafka_topic_list = []
postgres_schema_list = []

column_cache = {}

def topic_divide(topic):
    parts = topic.split("/")  

    if len(parts) != 5: 
       return None, None, None, None, None

    customer_id,inverter_brand,inverter_devicetype, datalogger_brand, datalogger_sn  = parts

    return customer_id,inverter_brand,inverter_devicetype, datalogger_brand, datalogger_sn 

def list_kafka_topics():
    """獲取 Kafka 中所有的 Topics"""
    global kafka_topic_list
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    metadata = admin_client.list_topics(timeout=10)

    kafka_topic_list = [
        topic for topic in metadata.topics.keys()
        if not topic.startswith("__") and topic.isdigit()
    ]

def list_postgres_schemas():
    """列出 PostgreSQL 中所有數字名稱的 Schemas"""
    global postgres_schema_list
    with engine.connect() as connection:
        schema_query = text("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name ~ '^[0-9]+$'  -- 只選擇全數字的 schema
              AND schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        """)
        postgres_schema_list = [row[0] for row in connection.execute(schema_query)]

def setup_postgres_from_kafka():
    """依據 Kafka Topics 建立 PostgreSQL Schemas 和 Tables"""
    global postgres_schema_list,kafka_topic_list

    print(f"{datetime.now()} setup_postgres_from_kafka kafka_topics:",kafka_topic_list)
    print(f"{datetime.now()} setup_postgres_from_kafka schema_list:",postgres_schema_list)

    for topic in kafka_topic_list or not topic.isdigit():
        if topic.startswith("__"):  # 忽略 Kafka 內建 Topic
            continue

        # 確保 PostgreSQL Schema 存在
        if topic not in postgres_schema_list:
            create_schema(topic)
            create_table(topic, "inverter")
            create_table(topic, "alarm")

            postgres_schema_list.append(topic)

def check_and_update_schema_tables():
    """檢查 PostgreSQL schemas 是否包含 'inverter' 和 'alarm' 表，並更新 column_cache"""
    global column_cache,postgres_schema_list

    with engine.connect() as connection:
        for schema in postgres_schema_list:
            # 查詢該 schema 下的 tables
            table_query = text(f"""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{schema}'
            """)
            existing_tables = {row[0] for row in connection.execute(table_query)}

            # 確保 `inverter` 和 `alarm` 表都存在
            required_tables = ["inverter", "alarm"]
            for table in required_tables:
                if table not in existing_tables:
                    create_table(schema, table)  # 如果表不存在，則建立

            # 初始化 schema 層級
            if schema not in column_cache:
                column_cache[schema] = {}

            # 查詢現有的欄位並更新 column_cache
            for table in required_tables:
                column_query = text(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                """)
                existing_columns = {row[0] for row in connection.execute(column_query)}
                
                # 更新快取
                column_cache[schema][table] = existing_columns

    print(f"{datetime.now()} Schema and table validation completed. column_cache updated ")

def check_and_create_topic(topic_name):
    """如果 Kafka Topic 不存在，則建立新的"""
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})

    # 獲取目前所有 Kafka Topics
    existing_topics = admin_client.list_topics(timeout=10).topics.keys()

    if topic_name not in existing_topics:
        print(f"{datetime.now()} 建立新的 Kafka Topic: {topic_name}")
        new_topic = NewTopic(topic_name, 
                             num_partitions=1, 
                             replication_factor=1)
        admin_client.create_topics([new_topic])
    else:
        print(f"{datetime.now()} Kafka Topic {topic_name} 已存在")


def create_schema(schema_name):
    """檢查 Schema 是否存在，若不存在則創建"""

    global postgres_schema_list

    if schema_name in postgres_schema_list:
        return

    with engine.begin() as connection:

        connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
        connection.commit()
        postgres_schema_list.append(schema_name)

        print(f"{datetime.now()} Schema '{schema_name}' created.")
        

def create_table(schema_name, table_name):
    """創建"""
    metadata = MetaData()
    
    columns = [
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("timestamp", DateTime, nullable=False,server_default=func.now())
    ]
    
    new_table = Table(table_name, metadata, *columns, schema=schema_name)
    metadata.create_all(engine)  

    if schema_name not in column_cache:
        column_cache[schema_name] = {}

    column_cache[schema_name][table_name] = {"id", "timestamp"}

    print(f"{datetime.now()} Table '{table_name}' created in schema '{schema_name}'.")

def create_columns(schema_name, table_name, data):
    """檢查並新增 Kafka 訊息中出現的新欄位"""
    global column_cache

    if schema_name not in column_cache:
        column_cache[schema_name] = {}
        
    # 使用連接取得當前欄位
    if table_name not in column_cache[schema_name]:
        with engine.connect() as connection:
            existing_columns = {row[0] for row in connection.execute(text(f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            """))}
        column_cache[schema_name][table_name] = existing_columns 
    
    existing_columns = column_cache[schema_name][table_name]
    new_columns = [k for k in data.keys() if k not in existing_columns]

    if not new_columns:
        return  # 如果沒有新欄位，直接返回
        
    with engine.connect() as connection:
        for key in new_columns:
            value = data[key]
            if key == "errormessage" or key == "warningcode":  
                column_type = "BIGINT"  # **錯誤碼和警告碼使用 BIGINT**
            elif key.endswith("time"):
                column_type = "TIMESTAMP WITHOUT TIME ZONE"  # **時間類型**
            elif key.endswith("voltage") or key.endswith("current") or key.endswith("power") or key.endswith("energy") or key.endswith("frequency"):
                column_type = "FLOAT"  # **電壓、電流、功率、頻率等**
            elif isinstance(value, bool):
                column_type = "BOOLEAN"  # **布林值**
            elif isinstance(value, int):
                column_type = "INTEGER"  # **整數**
            elif isinstance(value, float):
                column_type = "DOUBLE PRECISION"  # **浮點數**
            else:
                column_type = "CHARACTER VARYING(125)"  # **預設為字串**

            alter_sql = f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN "{key}" {column_type}'
            connection.execute(text(alter_sql))
            print(f"{datetime.now()} Added new column '{key}' to table '{table_name}' in schema '{schema_name}' with type '{column_type}'.")

        # 更新快取，避免下次重複查詢
        column_cache[schema_name][table_name].update(new_columns)

        connection.commit()

def create_index(schema_name, table_name):
    """
    為指定資料表建立複合索引：serialnumber 和 timestamp
    """
    if table_name == "inverter":
        index_name = f"{table_name}_timestamp_serialnumber_idx"
        with engine.connect() as connection:
            # 檢查索引是否存在
            check_index_query = text(f"""
                SELECT 1 FROM pg_indexes 
                WHERE schemaname = '{schema_name}' AND indexname = '{index_name}'
            """)
            result = connection.execute(check_index_query).fetchone()

            if result:
                print(f"{datetime.now()} Index '{index_name}' already exists.")
            else:
                # 建立複合索引
                create_index_query = text(f"""
                    CREATE INDEX "{index_name}" ON "{schema_name}"."{table_name}" ("timestamp", "serialnumber")
                """)
                connection.execute(create_index_query)
                print(f"{datetime.now()} Created index '{index_name}' on '{table_name}' (timestamp,serialnumber).")
    else:
        index_name = f"{table_name}_timestamp_serialnumber_errormessage_idx"
        with engine.connect() as connection:
            # 檢查索引是否存在
            check_index_query = text(f"""
                SELECT 1 FROM pg_indexes 
                WHERE schemaname = '{schema_name}' AND indexname = '{index_name}'
            """)
            result = connection.execute(check_index_query).fetchone()

            if result:
                print(f"{datetime.now()} Index '{index_name}' already exists.")
            else:
                # 建立複合索引
                create_index_query = text(f"""
                    CREATE INDEX "{index_name}" ON "{schema_name}"."{table_name}" ("timestamp", "serialnumber","errormessage")
                """)
                connection.execute(create_index_query)
                print(f"{datetime.now()} Created index '{index_name}' on '{table_name}' (timestamp,serialnumber,errormessage).")

def create_constraints(schema_name, table_name):
    """為指定資料表新增唯一約束 (UNIQUE) 在 collecttime 欄位"""
    with engine.connect() as connection:
        try:
            # 唯一約束：確保 `collecttime` 不重複
            constraint_name = f"{table_name}_collecttime_unique"

            # 檢查約束是否已經存在，避免重複建立
            check_constraint_query = text(f"""
                SELECT conname 
                FROM pg_constraint 
                WHERE conrelid = '{schema_name}.{table_name}'::regclass 
                AND conname = '{constraint_name}'
            """)
            result = connection.execute(check_constraint_query).fetchone()

            if not result:
                connection.execute(text(f"""
                    ALTER TABLE "{schema_name}"."{table_name}" 
                    ADD CONSTRAINT "{constraint_name}" UNIQUE (collecttime)
                """))
                print(f"{datetime.now()} Unique constraint added to '{schema_name}.{table_name}' on collecttime.")
            else:
                print(f"{datetime.now()} Unique constraint already exists on '{schema_name}.{table_name}'.")
        except Exception as e:
            print(f"{datetime.now()} Error adding unique constraint to '{schema_name}.{table_name}': {e}")


def consumer_worker():
    """Kafka 消費者持續監聽 Kafka topic 並將數據寫入資料庫"""
    global kafka_topic_list
    old_kafka_topic_list=[]
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'iot_consumer_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,  # ✅ 自動提交 offset
        'auto.commit.interval.ms': 5000  # ✅ 每 5 秒自動提交
    }
    consumer = Consumer(consumer_config)

    while not kafka_topic_list:
        print(f"{datetime.now()}  No available Kafka topics at startup. Retrying in 60 seconds...")
        list_kafka_topics()  # 重新獲取 Kafka topic 列表
        time.sleep(60)

    consumer.subscribe(kafka_topic_list)
    old_kafka_topic_list = kafka_topic_list

    last_refresh_time = time.time() 

    while True:
        try:
            if time.time() - last_refresh_time >= 3600:
                print(f"{datetime.now()} Refreshing Kafka topic subscriptions...")
                if len(kafka_topic_list)-len(old_kafka_topic_list) > 0:
                    consumer.subscribe(kafka_topic_list)  # 重新訂閱
                    old_kafka_topic_list = kafka_topic_list
                last_refresh_time = time.time()  # 更新上次訂閱時間

            messages = consumer.poll(timeout=10)
            
            if not messages:
                continue

            if messages is None or messages.error():
                print(f"{datetime.now()} Kafka consumer error: {messages.error()}")
                continue

            data = json.loads(messages.value().decode('utf-8'))

            kafka_topic = messages.topic()

            inverter_brand = data.pop("inverter_brand", None)  # Remove "inverter_brand" from data
            inverter_devicetype = data.pop("devicetype", None)  # Remove "inverter_devicetype"

            #write_to_influx_db(kafka_topic, inverter_brand, inverter_devicetype, data)
            write_to_postgresql_db(kafka_topic, inverter_brand, inverter_devicetype, data)
            error_message = random.choice(list(GoodWe(devicetype="GW50KN-MT").ERROR_MESSAGE_MAP.keys())) #建立錯誤假資料
            data["errormessage"] = error_message
            if data["errormessage"] != 0:

                write_to_postgresql_db(kafka_topic, inverter_brand, inverter_devicetype, data)


        except KafkaException as e:
            print(f"{datetime.now()} Kafka error: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"{datetime.now()} Unexpected error in Kafka consumer: {e}")

# 🚀 InfluxDB 資料寫入
def write_to_influx_db(data):
    pass
    try:
        influx_json = [{
            "measurement": "sensor_data",
            "tags": {
                "device": data["serialnumber"],
                "customer": data["customer_id"]
            },
            "time": data["collecttime"],
            "fields": {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
        }]
        #influx_client.write_points(influx_json)
        print(f"{datetime.now()} InfluxDB 寫入成功: {data}")
    except Exception as e:
        print(f"{datetime.now()} InfluxDB 寫入失敗: {e}")

def write_to_postgresql_db(kafka_topic,inverter_brand, inverter_devicetype, data):
    """Function to write data into PostgreSQL."""
    global postgres_schema_list
    global column_cache 

    schema_name = kafka_topic

    if schema_name not in postgres_schema_list:
        create_schema(schema_name)
        create_table(schema_name, "inverter")
        create_table(schema_name, "alarm")

    missing_columns = set(data.keys()) - column_cache[schema_name]["inverter"]
    
    if missing_columns:
        create_columns(schema_name, "inverter", data)  # 創建缺少的欄位
        create_index(schema_name, "inverter")
        create_constraints(schema_name, "inverter")
        column_cache[schema_name]["inverter"].update(missing_columns)  # 更新快取

    if data["errormessage"] == 0:
        with engine.begin() as connection:
            insert_query = text(f"""
                INSERT INTO "{schema_name}"."{"inverter"}" 
                ({', '.join([f'"{k}"' for k in data.keys()])}) 
                VALUES ({', '.join([f':{k}' for k in data.keys()])})
            """)
            connection.execute(insert_query, data)

            print(f"{datetime.now()} Inserted data into {schema_name}.inverter")
        return 

    if inverter_brand == "goodwe"  and data["errormessage"] != 0:
        data = GoodWe(inverter_devicetype).get_error_message(data)
 
    missing_columns = set(data.keys()) - column_cache[schema_name]["alarm"]
    
    if missing_columns:
        create_columns(schema_name, "alarm", data)  # 創建缺少的欄位
        create_index(schema_name, "alarm")
        create_constraints(schema_name, "alarm")
        column_cache[schema_name]["alarm"].update(missing_columns)  # 更新快取

    # Default case to insert into PostgreSQL
    if data["errormessage"] != 0:
        with engine.begin() as connection:
            insert_query = text(f"""
                INSERT INTO "{schema_name}"."{"alarm"}" 
                ({', '.join([f'"{k}"' for k in data.keys()])}) 
                VALUES ({', '.join([f':{k}' for k in data.keys()])})
            """)
            connection.execute(insert_query, data)

            print(f"{datetime.now()} Inserted data into {schema_name}.alarm")        


def start_kafka_consumer():

    consumer_thread = threading.Thread(target=consumer_worker, daemon=True)
    consumer_thread.start()

if __name__ == "__main__":
    
    try:

        list_kafka_topics()
        list_postgres_schemas()

        setup_postgres_from_kafka()
        check_and_update_schema_tables()

        start_kafka_consumer()

        while True:
            time.sleep(1)

    except Exception as e:
        print(f"{datetime.now()} An error occurred: {e}")

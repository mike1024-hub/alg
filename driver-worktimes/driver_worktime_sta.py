#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/20 14:19
# @Author  : wangheng
# @File    : reducer_worktime_sta_test.py
# @Description : 这个函数的主要功能是减速机的扭矩统计图


import sqlite3
import paho.mqtt.client as mqtt
import time
import json
from json import JSONEncoder
import yaml
import numpy as np


# 数据库设置
db_name = "driver_torque.db"
class NumpyEncoder(JSONEncoder):
    def default(self, o):
        # 处理numpy整数类型（包括int64、int32等）
        if isinstance(o, np.integer):
            return int(o)
        # 处理numpy浮点数类型
        elif isinstance(o, np.floating):
            return float(o)
        # 处理numpy数组
        elif isinstance(o, np.ndarray):
            return o.tolist()
        # 其他类型交给默认编码器处理
        return super(NumpyEncoder, self).default(o)
        
# 创建数据库连接和表（如果不存在）
def init_db(driver_num):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    # 建立电机扭矩的数据库
    tableList_motor = []
    for i in range(1, driver_num+1):
        table_name = f"motorTorque_{i}"
        tableList_motor.append(table_name)    # 记录数据库的table名字
        c.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torque REAL,
                timestamp INTEGER
                )
        ''')

    # 建立减速机扭矩的数据库
    tableList_reducer = []
    for i in range(1, driver_num + 1):
        table_temp = f"reducerTorque_{i}"
        tableList_reducer.append(table_temp)  # 记录数据库的table名字
        c.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_temp} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                torque REAL,
                timestamp INTEGER
                )
        ''')
    conn.commit()
    conn.close()

    return tableList_motor, tableList_reducer

# 保存数据到数据库
def save_to_db(dbtables, key, torque, timestamp)->list:
    # global tableList_motor  # 数据库的表名
    # global tableList_reducer

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    # 拆分当前序号
    split_parts = key.split("->")
    cur_name = int(split_parts[-1]) - 1

    table_name = dbtables[cur_name]

    c.execute(f'''
            INSERT INTO {table_name} (torque, timestamp)
            VALUES (?, ?)
        ''', (torque,timestamp))
    conn.commit()
    c.execute(f'''
        SELECT torque FROM {table_name}  WHERE torque > (?)
     ''', (threshold_for_vis,))
    show_data = c.fetchall()
    c.close()
    conn.close()
    torque_all = []
    for row in show_data:
        torque_all.append(row[0])

    return torque_all

def torque_vis(torque_all:list):
    counts,bin_edges=np.histogram(torque_all,bins=6)
    worktime=np.round(counts*data_interval/3600,2)
    total_worktime=np.sum(worktime)
    result = {
        "worktime_statistics": {
            "first": counts[0],  # 直方图第一个值
            "count": len(counts),  # 直方图的数量，个数，默认取6
            "interval": bin_edges[1] - bin_edges[0],  # 直方图的间隔
            "values": counts,
        },
        "total_worktime": total_worktime,
        "current_torque": torque_all[-1]
    }
    return result

def key_exists_in_nested_dict(nested_dict, key_to_check):
    if key_to_check in nested_dict:
        return True
    for key, value in nested_dict.items():
        if isinstance(value, dict):
            if key_exists_in_nested_dict(value, key_to_check):
                return True
    return False

# 返回含指定key值的第一列key值
def find_first_level_key(nested_dict, target_key):
    output_keys = []  # 定义一个空
    for first_level_key, value in nested_dict.items():
        if isinstance(value, dict):  # 检查值是否为字典
            if target_key in value:  # 检查目标键是否在该字典中
                output_keys.append(first_level_key)
    return output_keys

def get_config_from_yaml(yaml_file_path):
    # mqtt设置的默认值
    default_signal_motor = 'motorTorque'
    default_signal_reducer = 'reducerTorque'
    default_driver_num = 16
    default_data_interval = 3
    default_threshold_for_working = 3
    default_threshold_for_vis = 500
    default_gear_ratio_high = 29.545
    default_gear_ratio_low = 55.486
    default_switch_flag = 1      # 默认为低速模式（掘进机低速重载）
    default_id = "driver_worktime1"
    default_host = "127.0.0.1"
    default_port = 1883
    default_keepalive = 6000
    default_sub_topic = "crec/data/realTime/driverGroup"  # 订阅话题，从此话题获取驱动组电机扭矩值
    # default_sub_tbm_toic = "crec/data/realTime/driverBox"
    default_pub_topic_reducer = "crec/computing/realTime/driverGroup/reducerWorktimeStatis"  # 发布话题，
    default_pub_topic_motor = "crec/computing/realTime/driverGroup/motorWorktimeStatis"  # 发布话题，
    default_login_flag = False  # 是否设置登录名和登录密码，如果设置，需要改为“True”，如果不设置，则为“False”
    default_login_name = "admin"  # 登录名设置
    default_password = "admin123"

    try:
        print("开始读取yaml文件....")
        # 读取 YAML 文件
        with open(yaml_file_path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)

        # 初始化 host 和 password 等变量
        signal_motor = default_signal_motor
        signal_reducer = default_signal_reducer
        driver_num = default_driver_num
        data_interval = default_data_interval
        threshold_for_working = default_threshold_for_working
        threshold_for_vis = default_threshold_for_vis
        gear_ratio_high = default_gear_ratio_high
        gear_ratio_low = default_gear_ratio_low
        switch_flag = default_switch_flag
        id = default_id
        host = default_host
        port = default_port
        keepalive = default_keepalive
        sub_topic = default_sub_topic
        pub_topic_reducer = default_pub_topic_reducer
        pub_topic_motor = default_pub_topic_motor
        login_flag = default_login_flag
        login_name = default_login_name
        password = default_password

        if "signal_motor" in data and data["signal_motor"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            signal_motor = data["signal_motor"]

        if "signal_reducer" in data and data["signal_reducer"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            signal_reducer = data["signal_reducer"]

        if "driver_num" in data and data["driver_num"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            driver_num = data["driver_num"]

        if "threshold_for_working" in data and data["threshold_for_working"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            threshold_for_working = data["threshold_for_working"]

        if "threshold_for_vis" in data and data["threshold_for_vis"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            threshold_for_vis = data["threshold_for_vis"]

        if "gear_ratio_high" in data and data["gear_ratio_high"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            gear_ratio_high = data["gear_ratio_high"]

        if "gear_ratio_low" in data and data["gear_ratio_low"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            gear_ratio_low = data["gear_ratio_low"]

        if "switch_flag" in data and data["switch_flag"] not in ["", None]:  # 假设 0, 空字符串, None 为无效值
            switch_flag = data["switch_flag"]

        if "data_interval" in data and data["data_interval"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            data_interval = data["data_interval"]

        data = data["mqtt_config"]
        # 检查 'host' 是否在字典中且不为 0（或其他无效值）
        if "id" in data and data["id"] not in [0, "", None]:  # 假设 0, 空字符串, None 为无效值
            id = data["id"]

        if "host" in data and data["host"] not in [
            0,
            "",
            None,
        ]:  # 假设 0, 空字符串, None 为无效值
            host = data["host"]

        if "port" in data and data["port"] not in [
            0,
            "",
            None,
        ]:  # 假设 0, 空字符串, None 为无效值
            port = data["port"]

        if "keepalive" in data and data["keepalive"] not in [
            0,
            "",
            None,
        ]:  # 假设 0, 空字符串, None 为无效值
            keepalive = data["keepalive"]

        if "sub_topic" in data and data["sub_topic"] not in [
            0,
            "",
            None,
        ]:  # 假设 0, 空字符串, None 为无效值
            sub_topic = data["sub_topic"]

        if "pub_topic_reducer" in data and data["pub_topic_reducer"] not in [
            0,
            "",
            None,
        ]:  # 假设 0, 空字符串, None 为无效值
            pub_topic_reducer = data["pub_topic_reducer"]

        if "pub_topic_motor" in data and data["pub_topic_motor"] not in [
            0,
            "",
            None,
        ]:  # 假设 0, 空字符串, None 为无效值
            pub_topic_motor = data["pub_topic_motor"]

        if "login_flag" in data and data["login_flag"] not in [
            "",
            None,
        ]:  # 假设 0, 空字符串, None 为无效值
            login_flag = data["login_flag"]

        if "login_name" in data and data["login_name"] not in [
            0,
            "",
            None,
        ]:  # 假设 0, 空字符串, None 为无效值
            login_name = data["login_name"]

        # 检查 'password' 是否在字典中且不为 0（或其他无效值）
        if "password" in data and data["password"] not in [0, "", None]:  # 同上
            password = data["password"]

        print("已经完成yaml文件的读取")
        return (
            signal_motor,
            signal_reducer,
            driver_num,
            data_interval,
            threshold_for_working,
            threshold_for_vis,
            gear_ratio_high,
            gear_ratio_low,
            switch_flag,
            id,
            host,
            port,
            keepalive,
            sub_topic,
            pub_topic_reducer,
            pub_topic_motor,
            login_flag,
            login_name,
            password,
        )

    except FileNotFoundError:
        print(f"Error: The file {yaml_file_path} was not found.")
        return (
            default_signal_motor,
            default_signal_reducer,
            default_driver_num,
            default_data_interval,
            default_threshold_for_working,
            default_threshold_for_vis,
            default_gear_ratio_high,
            default_gear_ratio_low,
            default_switch_flag,
            default_id,
            default_host,
            default_port,
            default_keepalive,
            default_sub_topic,
            default_pub_topic_reducer,
            default_pub_topic_motor,
            default_login_flag,
            default_login_name,
            default_password,
        )
    except yaml.YAMLError as exc:
        print(f"Error in parsing YAML file: {exc}")
        return (
            default_signal_motor,
            default_signal_reducer,
            default_driver_num,
            default_data_interval,
            default_threshold_for_working,
            default_threshold_for_vis,
            default_gear_ratio_high,
            default_gear_ratio_low,
            default_switch_flag,
            default_id,
            default_host,
            default_port,
            default_keepalive,
            default_sub_topic,
            default_pub_topic_reducer,
            default_pub_topic_motor,
            default_login_flag,
            default_login_name,
            default_password,
        )


# 当代理响应订阅请求时被调用。
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("连接成功")
        # 确保重新订阅主题
        client.subscribe(sub_topic)
    else:
        print("Connected with result code " + str(rc))
        client.connected_flag = False

# 当客户端断开连接时调用
def on_disconnect(client, userdata, rc):
    print(f"Disconnected with result code {rc}")
    print("尝试重新连接...")


# 当代理响应订阅请求时被调用
def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


# 当使用使用publish()发送的消息已经传输到代理时被调用。
def on_publish(client, obj, mid):
    print("OnPublish, mid: " + str(mid))


# 当收到关于客户订阅的主题的消息时调用。 message是一个描述所有消息参数的MQTTMessage。
def on_message(client, userdata, msg):
    print(msg.topic )
    data = json.loads(msg.payload.decode())
    start_time = time.time()

    motor_torque_keys = find_first_level_key(data, signal_motor)
    if len(motor_torque_keys) > 0:
        print("当前数据中含有电机扭矩信号，开始判断其工作状态，使用设置阈值进行判断")
        if data[motor_torque_keys[0]][signal_motor] > threshold_for_working :
            print("当前驱动组正在工作，开始进行寿命统计")
            motor_json = {}
            reducer_json = {}
            for motor_torque_key in motor_torque_keys:  # 依次对每个电机的参数进行处理
                motorTorque = data[motor_torque_key][signal_motor]    # 电机扭矩
                reducerTorque = motorTorque * gear_ratio       # 减速机扭矩
                timestamp = int(time.time() * 1000)  # 获取当前时间的毫秒级 Unix 时间戳
                motor_long_data = save_to_db(tableList_motor, motor_torque_key, motorTorque, timestamp)
                print("开始进行扭矩区间划分")
                motor_cal_data = torque_vis(motor_long_data)
                motor_json[motor_torque_key] = motor_cal_data

                reducer_long_data = save_to_db(tableList_reducer, motor_torque_key, reducerTorque, timestamp)
                reducer_cal_data = torque_vis(reducer_long_data)
                reducer_json[motor_torque_key] = reducer_cal_data

            motor_json_data = json.dumps(motor_json,cls=NumpyEncoder)
            client.publish(topic=pub_topic_motor, payload=motor_json_data, qos=1)  # 发布消息
            print("已发送电机扭矩寿命统计值")

            reducer_json_data = json.dumps(reducer_json,cls=NumpyEncoder)
            client.publish(topic=pub_topic_reducer, payload=reducer_json_data, qos=1)  # 发布消息
            print("已发送减速机扭矩寿命统计值")

        else:
            print("当前驱动组未在工作")
    else:
        print("当前消息中不包含驱动组扭矩信号.")

    end_time = time.time()
    cal_time = end_time - start_time
    print(f"程序运行花费的时间: {cal_time} 秒")


    
# 当客户端有日志信息时调用
def on_log(client, obj, level, string):
    print("Log:" + string)


if __name__ == "__main__":
    # step1: 读取本地yaml文件进行配置信息的读取
    yaml_file_path = "driver_worktimes_config.yaml"
    (
        signal_motor,
        signal_reducer,
        driver_num,
        data_interval,
        threshold_for_working,
        threshold_for_vis,
        gear_ratio_high,
        gear_ratio_low,
        switch_flag,
        id,
        host,
        port,
        keepalive,
        sub_topic,
        pub_topic_reducer,
        pub_topic_motor,
        login_flag,
        login_name,
        password,
    ) = get_config_from_yaml(yaml_file_path)

    # step2：创建两个本地数据库
    tableList_motor, tableList_reducer = init_db(driver_num)

    # 对当前减速机进行赋值
    if switch_flag > 0 :
        gear_ratio = gear_ratio_low
    else:
        gear_ratio = gear_ratio_high

    
    
    
    # 连接服务器
    try:
        # step3: 对mqtt参数进行配置
    # 实例化
        client = mqtt.Client( client_id= id)  # mqtt.CallbackAPIVersion.VERSION1,
    # 判断是否需要对登录名和密码设置
        if login_flag:
            client.username_pw_set(username=login_name, password=password)
    # 回调函数
        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.on_log = on_log
        client.on_disconnect = on_disconnect
    
    # 设置自动重连延迟
        client.reconnect_delay_set(min_delay=1, max_delay=120)
        connected = False
        while not connected:
            try:
                print(f"尝试连接MQTT服务器 {host}:{port}")
                client.connect(host=host, port=port, keepalive=keepalive)
                connected = True
                print("MQTT服务器连接成功")
            except Exception as e:
                print(f"MQTT服务器连接失败: {e}")
                print("5秒后重新尝试连接...")
                time.sleep(5)  # 等待5秒后重试
    # 启动循环
        client.loop_forever()    
    except Exception as e:
        print(f"连接失败: {e}")
    
    

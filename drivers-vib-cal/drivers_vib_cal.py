#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/18 14:50
# @Author  : wangheng
# @File    : drivers_vib_cal.py
# @Description : 这个函数的主要功能是接收mqtt的驱动组电机、减速机以及主驱动箱的振动信号，然后直方图统计后发出统计结果
# 2025.01.09修改   ———修改问题：驱动组电机、减速机、及主驱动箱的信号并不是一组一组的发出的，而是单个单个的发出信号。
#                 ————修改方法，将程序新增检测话题中的振动信号数量，从而依次处理

#2025.02.15修改   ———— 增加log日志打印功能，程序运行情况在本地log文件夹中显示，排查拟合曲线显示范围与图形不对照的原因

import logging
import os
import traceback
from logging.handlers import TimedRotatingFileHandler
import sqlite3
import numpy as np
import paho.mqtt.client as mqtt
import time
import json
import yaml
import math
from scipy.stats import gaussian_kde
import pandas as pd
from dataclasses import dataclass

# 抑制特定的弃用警告
import warnings
warnings.filterwarnings("ignore", category=FutureWarning, message="'DataFrame.swapaxes' is deprecated")

# 配置日志文件夹
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 配置日志格式
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)

# 创建一个TimedRotatingFileHandler，每天轮换一次日志文件
log_file = os.path.join(log_dir, "codeWork.log")
file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=2)
file_handler.setFormatter(logging.Formatter(log_format))

# 将handler添加到logger
logger = logging.getLogger()
logger.addHandler(file_handler)

@dataclass
class Config:
    """配置参数数据类"""
    work_status_enabled: bool
    signal_motor: str
    motor_num: int
    motor_hist_num: int
    signal_reducer: str
    reducer_num: int
    reducer_hist_num: int
    signal_driver_box: str
    driver_box_points: int
    driver_hist_num: int
    main_driver_key: str
    cutter_haed_torque_key: str
    cutter_head_speed_key: str
    total_thrust_key: str
    thrust_speed_key: str
    id: str
    host: str
    port: int
    keepalive: int
    sub_motor_topic: str
    sub_reducer_topic: str
    sub_driver_box_topic:str
    sub_tbm_topic: str
    pub_topic_motor: str
    pub_topic_reducer: str
    pub_topic_driver_box: str
    pub_topic_alarm: str
    login_flag: bool
    login_name: str
    password: str

# 默认配置字典
DEFAULT_CONFIG = {
    'work_status_enabled': True,
    'motor_vib_config': {
        'signal_motor': '02',
        'motor_num': 16,
        'motor_hist_num': 30
    },
    'tunneling_status_config':{
        'main_driver_key': '01',
        'cutter_haed_torque_key': '0401',
        'cutter_head_speed_key': '0501',
        'total_thrust_key': '0201',
        'thrust_speed_key': '0202',
    },
    'reducer_vib_config': {
        'signal_reducer': '02',
        'reducer_num': 16,
        'reducer_hist_num': 30
    },
    'driver_box_vib_config': {
        'signal_driver_box': '02',
        'driver_box_points': 4,
        'driver_hist_num': 30
    },
    'mqtt_config': {
        'id': 'motor_vib_cal1',
        'host': '127.0.0.1',
        'port': 1883,
        'keepalive': 6000,
        'sub_tbm_topic': 'crec/data/realTime/002010',
        'sub_driver_box_topic': 'crec/data/realTime/002004',
        'sub_motor_topic': 'crec/data/realTime/002001',
        'sub_reducer_topic': 'crec/data/realTime/002002',
        'pub_topic_motor': 'crec/computing/realTime/driverGroup/motorVibStatistics',
        'pub_topic_reducer': 'crec/computing/realTime/driverGroup/reducerVibStatistics',
        'pub_topic_driver_box': 'crec/computing/realTime/driverBox/VibStatistics',
        'pub_topic_alarm': 'crec/alarm/post',
        'login_flag': False,
        'login_name': 'admin',
        'password': 'admin123'
    }
}

def _flatten_config(config_dict):
    """将嵌套的配置字典展平为单层字典"""
    flattened = {}
    for key, value in config_dict.items():
        if isinstance(value, dict):
            flattened.update(_flatten_config(value))
        else:
            flattened[key] = value
    return flattened

def _is_valid_value(value):
    """检查值是否有效（非0、非空字符串、非None）"""
    return value not in [0, "", None]

def _merge_config(default, custom):
    """递归合并默认配置和自定义配置"""
    if isinstance(default, dict) and isinstance(custom, dict):
        merged = default.copy()
        for key, value in custom.items():
            if key in merged:
                curr_v=_merge_config(merged[key], value)
                merged[key] = curr_v
            else:
                merged[key] = value
        return merged
    return custom# if _is_valid_value(custom) else default

def get_config_from_yaml(yaml_file_path) -> Config:
    """从YAML文件加载配置，返回Config对象"""
    try:
        logger.info("开始读取yaml文件....")
        with open(yaml_file_path, 'r', encoding='utf-8') as file:
            custom_config = yaml.safe_load(file) or {}

        # 合并配置
        merged_config = _merge_config(DEFAULT_CONFIG, custom_config)
        # 展平配置字典
        flattened_config = _flatten_config(merged_config)

        # 类型验证示例（关键参数）
        if not isinstance(flattened_config['port'], int):
            logger.warning(f"端口号 {flattened_config['port']} 不是整数，使用默认值 {DEFAULT_CONFIG['mqtt_config']['port']}")
            flattened_config['port'] = DEFAULT_CONFIG['mqtt_config']['port']

        return Config(**flattened_config)

    except FileNotFoundError:
        logger.error(f"配置文件 {yaml_file_path} 未找到，使用默认配置")
    except yaml.YAMLError as exc:
        logger.error(f"YAML文件解析错误: {exc}", exc_info=True)
    except KeyError as exc:
        logger.error(f"配置文件缺少必要键: {exc}", exc_info=True)
    except Exception as exc:
        logger.error(f"配置加载失败: {exc}", exc_info=True)

    # 所有异常情况下返回默认配置
    default_flattened = _flatten_config(DEFAULT_CONFIG)
    return Config(**default_flattened)

def clean_old_logs():
    """清理超过两天的日志文件"""
    now = time.time()
    for filename in os.listdir(log_dir):
        file_path = os.path.join(log_dir, filename)
        if os.path.isfile(file_path) and filename.endswith(".log"):
            file_age = now - os.path.getmtime(file_path)
            if file_age > 1 * 86400:  # 2天 = 2 * 86400秒
                os.remove(file_path)
                logger.info(f"Deleted old log file: {filename}")

# 表格中掘进状态的数据较少，不适用于使用正态分布来判断掘进状态，使用掘进参数简单判断
def Simple_judgment(data_rotation_speed, data_torque, data_thrust_speed, data_force):
    """
    利用当前掘进机的掘进参数 判断当前掘进的状态
    :param data_rotation_speed: 刀盘转速
    :param data_torque:  刀盘扭矩
    :param data_thrust_speed:  推进速度
    :param data_force:  推进力
    :return:  返回设备的掘进状态：0→停机段，1→空转段，2→上升段，3→稳定段，4→下降段
    """
    data_rotation_speed = float(data_rotation_speed)
    data_torque = float(data_torque)
    data_thrust_speed = float(data_thrust_speed)
    data_force = float(data_force)
    if (data_rotation_speed > 0) and (
            data_torque > 0
    ):  # 最开始是刀盘开始转动，空推段，用刀盘扭矩和刀盘转速来判断
        if (data_thrust_speed > 0) and (
                data_force > 10
        ):  # 判断是否开始推进，推进后达到稳定段，这里先判断是否达到上升段
            flag = 3  # 稳定段数据
        else:
            flag = 1  # 空转段
    else:
        flag = 0  # 停机段
    return flag

# 数据库设置
db_name = "driver_vib_feature.db"
# 创建数据库连接和表（如果不存在）
def init_db(motor_num, reducer_num, driver_box_points):
    # 依次对数据库进行处理，有多少种类的振动信号，就建多少个表供数据查询
    vib_tables = [motor_num, reducer_num, driver_box_points]
    name_tables = ["motorVib", "reducerVib", "driverBox"]
    tables_list = []
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    for num in range(0, len(vib_tables)):
        tables_single = []
        for name in range(1, vib_tables[num]+1):
            cur_name = f"{name_tables[num]}_{name}"
            tables_single.append(cur_name)    # 记录数据库的table名字
            c.execute(f'''
            CREATE TABLE IF NOT EXISTS {cur_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER,
                    rotational_speed  REAL,
                    torque REAL,
                    forward_speed REAL,
                    force REAL,
                    work_flag REAL,
                    peak_to_peak REAL,
                    rms REAL,
                    kurtosis REAL,
                    directfreq REAL
                    )
            ''')
        tables_list.append(tables_single)
    conn.commit()
    conn.close()
    return tables_list
# 接下来是箱型图滤波函数
# 定义替代函数
def replace_value(x, val_low, val_up, data_ser):
    if x < val_low.iloc[0] or x > val_up.iloc[0]:
        return data_ser.mean()  # 使用均值替代
    else:
        return x  # 保持原值


def box_plot_outliers(data_ser, box_scale):
    """
    利用箱线图去除异常值
    :param data_ser: 接收 pandas.Series 数据格式
    :param box_scale: 箱线图尺度，默认用 box_plot（scale=3）进行清洗
    :return:
    """
    iqr = box_scale * (data_ser.quantile(0.75) - data_ser.quantile(0.25))
    # 下阈值
    val_low = data_ser.quantile(0.25) - iqr * 1.5
    # 上阈值
    val_up = data_ser.quantile(0.75) + iqr * 1.5
    # 异常值
    outlier = data_ser[(data_ser < val_low) | (data_ser > val_up)]
    # # 正常值
    mid_value = data_ser[0].values
    mid_mean = sum(mid_value) / len(mid_value)
    mid_value = mid_value.tolist()
    normal_value = []
    for i in range(0, len(mid_value)):
        if mid_value[i] <= val_low.iloc[0] or mid_value[i] >= val_up.iloc[0]:
            normal_value.append(mid_mean)  # 使用均值替代
        elif mid_value[i] > val_low.iloc[0] and mid_value[i] < val_up.iloc[0]:
            normal_value.append(mid_value[i])

    # print("output data ：", type(normal_value))
    # print("111111")
    return outlier, normal_value, (val_low, val_up)


def KRM(data):
    motor_df = pd.DataFrame(data)
    batch_size = 20  # 最优效果是 20
    df_batches = np.array_split(motor_df, batch_size)
    # 对每个批次进行处理

    normal_value_batches = []
    for batch in df_batches:
        # 在这里进行批次的处理
        outlier, normal_value, processed_data = box_plot_outliers(batch, 1.5)  # 根据需要的处理操作进行修改
        normal_value_batches.extend(normal_value)
    # print("处理前后数据长度分别为：", len(data), len(normal_value_batches))
    return normal_value_batches


def nextpow2(x):
    if x == 0:
        return 0
    else:
        return int(np.ceil(np.log2(x)))


def Do_fft(sig, Fs):  # 输入信号和采样频率
    xlen = len(sig)
    sig = sig - sig.mean()
    NFFT = 2 ** nextpow2(xlen)
    yf = np.fft.fft(sig, NFFT) / xlen * 2
    yf = abs(yf[0: int(NFFT / 2 + 1)])
    f = Fs / 2 * np.linspace(0, 1, int(NFFT / 2 + 1))
    f = f[:]
    return f, yf
    # 频域离散值的序号


def fre_feature_cal(data, fs):
    # fre_amlitude = []
    data = np.array(data)
    Fs = fs
    f, y = Do_fft(data, Fs)
    p_temp = math.sqrt(sum(y ** 2))  # 通频幅值
    return p_temp


"""振动信号的时域特征提取"""


# 振动数据 时域特征提取，共14个指标，分为有量纲参数和无量纲参数 两种
# 有量纲参数 1~9            无量纲参数 10-15
def get_time_domain_features(data):
    """data为一维振动信号"""
    data = np.array(data)
    # x_rms = 0
    # absXbar = 0
    # x_r = 0
    # S = 0
    K = 0
    # k = 0
    x_rms = 0
    # fea = []
    # len_ = len(data.iloc[0, :])
    len_ = len(data)
    mean_ = data.mean(axis=0)  # 1.均值
    # var_ = data.var(axis=0)  # 2.方差
    std_ = data.std(axis=0)  # 3.标准差
    max_ = data.max(axis=0)  # 4.最大值
    min_ = data.min(axis=0)  # 5.最小值
    # print("最大值，最小值：", max_, min_)
    x_p = max(abs(max_), abs(min_))  # 6.峰值
    for i in range(len_):
        x_rms += data[i] ** 2
        # absXbar += abs(data[i])
        # x_r += math.sqrt(abs(data[i]))
        # S += (data[i] - mean_) ** 3
        K += (data[i] - mean_) ** 4
    x_rms = math.sqrt(x_rms / len_)  # 7.均方根值，有效值
    # absXbar = absXbar / len_  # 8.绝对平均值
    # x_r = (x_r / len_) ** 2  # 9.方根幅值
    # W = x_r # 10.波形指标
    # C = x_p / x_rms  # 11.峰值指标
    # I = x_r  # 12.脉冲指标
    # L = x_p / x_r  # 13.裕度指标
    # S = S / ((len_ - 1) * std_ ** 3)  # 14.偏斜度
    K = K / ((len_ - 1) * std_ ** 4)  # 15.峭度

    #      均值    方差   标准差  最大值  最小值  峰值 均方根值  绝对平均值 方根幅值  波形指标 峰值指标 脉冲指标 裕度指标 偏斜度  峭度
    fea = [
        x_p,
        x_rms,
        K,
    ]  # 更改后的指标顺序
    # fea = [mean_, absXbar, var_, std_, x_r, x_rms, x_p, max_, min_, W, C, I, L, S, K]
    return fea

# 保存数据到数据库
def save_to_db(cur_index,device_key, key, timestamp, tbm_cuter_speed, tbm_cutterheadTorque, tbm_speed, tbm_thrust, work_flag,
                       x_p, x_rms, x_k, fre_feature):
    # global dbtables  # 数据库的表名

    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    table_name ='tbl_'+str(cur_index)+'_'+device_key+'_'+key  #dbtables[cur_index][cur_name]
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    table_exists = c.fetchone()
    if not table_exists:
        c.execute(f'''
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                timestamp INTEGER,
                rotational_speed REAL,
                torque REAL,
                forward_speed REAL,
                force REAL,
                work_flag INTEGER,
                peak_to_peak REAL,
                rms REAL,
                kurtosis REAL,
                directfreq REAL
            )
        ''')
        conn.commit()
    c.execute(f'''
            INSERT INTO {table_name} (timestamp, rotational_speed, torque, forward_speed, force,work_flag, peak_to_peak,rms,  kurtosis, directfreq)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, tbm_cuter_speed, tbm_cutterheadTorque, tbm_speed, tbm_thrust, work_flag, x_p, x_rms, x_k, fre_feature))
    conn.commit()
    c.execute(f'''
        SELECT peak_to_peak, rms,  kurtosis, directfreq FROM {table_name} ORDER BY id DESC LIMIT 2000
     ''')
    show_data = c.fetchall()
    c.close()
    conn.close()
    peak_to_peak_list = []
    rms_list = []
    kurtosis_list = []
    directfreq_list = []
    for row in show_data:
        peak_to_peak_list.append(row[0])
        rms_list.append(row[1])
        kurtosis_list.append(row[2])
        directfreq_list.append(row[3])

    output_data = [peak_to_peak_list, rms_list, kurtosis_list, directfreq_list]
    return output_data

def vib_cal(long_data, num_config):
    '''
    计算振动信号特征值
    :param long_data:
    :param x_p:
    :param x_rms:
    :param x_k:
    :param fre_feature:
    :return: output_json
    '''
    json_data = {}

    data_name = ["peak_to_peak", "rms", "kurt", "direct_freq"]
    #按照顺序进行核密度的估计
    for i in range(0, len(long_data)):
        data = long_data[i]
        # 进行滤波 #进行 离群点去除
        if len(data) > 50:
            fixed_array = KRM(data)
        else:
            fixed_array = data
        fea_name = data_name[i]

        # 对振动直方图的数量进行固定，取默认值为40
        if len(fixed_array) > num_config :
            hist_num = num_config
        else:
            hist_num = len(fixed_array)


        
        counts,bins_edges = np.histogram(fixed_array, bins=hist_num, density=True)
        # n 为每个直方图的高度大小，共有40个值，对应n中41组数中的40个直方图大小。
        # bins 为直方图的每个区间边界，共有41个值，如[0,1,2,3,4。。。]，实际上是[0, 1), [1, 2), [2, 3)，，，等，实际上包含40个区间，左边界闭合
        
        counts = counts.tolist()
        bins_edges = bins_edges.tolist()

        if len(bins_edges) > 1 :
            hist_interval = bins_edges[1] - bins_edges[0]
        else:
            hist_interval = 1.0

        # 计算核密度估计
        if len(fixed_array) > 500 :
            kde = gaussian_kde(fixed_array, bw_method='scott')  # 使用Scott的方法选择带宽
            x_kde = np.linspace(min(fixed_array), max(fixed_array), 1000)  # 生成用于绘制KDE的x值
            y_kde = kde(x_kde)  # 计算KDE在x_kde处的值
            fit_num = 1000

            # 将ndarray转换为list
            x_kde = x_kde.tolist()
            y_kde = y_kde.tolist()
        elif len(fixed_array) < 5 :
            x_kde = list(range(0, len(fixed_array)))
            y_kde = fixed_array
            fit_num = len(fixed_array)

        else:
            kde = gaussian_kde(fixed_array, bw_method='scott')  # 使用Scott的方法选择带宽
            x_kde = np.linspace(min(fixed_array), max(fixed_array), len(fixed_array))  # 生成用于绘制KDE的x值
            y_kde = kde(x_kde)  # 计算KDE在x_kde处的值
            fit_num = len(fixed_array)

            # 将ndarray转换为list
            x_kde = x_kde.tolist()
            y_kde = y_kde.tolist()

        if len(x_kde) > 1 :
            fit_interval = x_kde[1] - x_kde[0]
        else:
            fit_interval = 1.0

        json_data[fea_name] = {
            "vib_statis": {
                "first" : bins_edges[0],
                "count" : hist_num,
                "interval" : hist_interval,
                "values" : counts,
            },
            "fit_distri": {
                "first": x_kde[0],
                "count": fit_num,
                "interval" : fit_interval,
                "values" : y_kde,
            },
            "cur_value": fixed_array[0]
        }
    return json_data

# 返回含指定key值的第一列key值
def find_first_level_key(nested_dict, target_key):
    output_keys = []  # 定义一个空
    for first_level_key, value in nested_dict.items():
        if isinstance(value, dict):  # 检查值是否为字典
            if target_key in value:  # 检查目标键是否在该字典中
                output_keys.append(first_level_key)
    return output_keys





# 当代理响应订阅请求时被调用。
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("连接成功")
        logger.info("mqtt已经连接成功")
        # 连接成功后重新订阅主题
        subscribe_topics(client)
    else:
        logger.error(f"连接失败，返回码: {rc}")
    print("Connected with result code " + str(rc))


# 当代理响应订阅请求时被调用
def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


# 当使用使用publish()发送的消息已经传输到代理时被调用。
def on_publish(client, obj, mid):
    print("OnPublish, mid: " + str(mid))

def handle_vib_data(pub_topic:str,index:int,data:dict[str,dict],signal_key:str,hist_num:int):
    if not start_flag:
        logger.info(f"{pub_topic}不满足启动条件")
        return
    process_data={}
    for device_key,device_value in data.items():
        filter_dicts={k:v for k,v in device_value.items() if k.startswith(signal_key)}
        if(filter_dicts):
            process_data[device_key]=filter_dicts
    if not process_data:
        logger.info(f"{pub_topic}当前没有需要处理的振动数据")
        return
    device_name=''
    if index==2:
        device_name='主驱动箱'
    elif index==1:
        device_name='减速机'
    elif index==0:
        device_name='电机'
    result_json = {}
    #result_json["vib_feature_num"] = 4
    for item_key,item_value in process_data.items():
        result_json[item_key]={}
        for s_key,signal_value in item_value.items():
            freq=signal_value["freq"]
            wave=signal_value["timeData"]
            timestamp = int(time.time() * 1000)  # 获取当前时间的毫秒级 Unix 时间戳
            vib_data = wave - np.mean(wave)    # 归一化处理
            vib_features = get_time_domain_features(vib_data)  # 提取时域特征
            fre_feature = fre_feature_cal(vib_data, freq)     # 提取频域特征
            x_p = vib_features[0]  # 峰峰值
            x_rms = vib_features[1]  # 有效值
            x_k = vib_features[2]  # 峭度值
            long_data = save_to_db(index,item_key, s_key, timestamp, tbm_cuter_speed, tbm_cutterheadTorque, tbm_speed, tbm_thrust, work_flag,


                                   x_p, x_rms, x_k, fre_feature)
            output_json = vib_cal(long_data, hist_num)
            result_json[item_key][s_key] = output_json
    json_str=json.dumps(result_json)        
    client.publish(topic=pub_topic, payload=json_str, qos=1)  # 发布消息
    logger.info(f"{device_name}振动数据统计结果已发送，发送的主题为{pub_topic}")        
    
# 当收到关于客户订阅的主题的消息时调用。 message是一个描述所有消息参数的MQTTMessage。
def on_message(client, userdata, msg):
    global work_flag    #声明全局变量--只有需要改变全局变量的时候才定义global
    global start_flag
    global tbm_cuter_speed
    global tbm_cutterheadTorque
    global tbm_speed
    global tbm_thrust
    topic = msg.topic
    # print(msg.topic)
    logger.info(f"接收到的话题为{topic}")
    data = json.loads(msg.payload.decode())

    start_time = time.time()
    
    # 获取当前掘进参数，判断电机工作状态
    if topic == config.sub_tbm_topic :
        logger.info(f"接收到的有驱动箱数据，数据为{data}")
        word_param_data = data[config.main_driver_key]
        if (config.cutter_haed_torque_key in word_param_data) and (config.cutter_head_speed_key in word_param_data) and (config.thrust_speed_key in word_param_data) and (config.total_thrust_key in word_param_data):
            tbm_cuter_speed = word_param_data[config.cutter_head_speed_key]
            tbm_cutterheadTorque = word_param_data[config.cutter_haed_torque_key]
            tbm_speed = word_param_data[config.thrust_speed_key]
            tbm_thrust = word_param_data[config.total_thrust_key]
            logger.info(f"开始进行掘进工况判断，掘进参数为{tbm_cuter_speed}、{tbm_cutterheadTorque}、{tbm_speed}、{tbm_thrust}")
            # 通过掘进参数判断工作状态 返回设备的掘进状态：0→停机段，1→空转段，2→上升段，3→稳定段，4→下降段
            # 只有flag=3的时候才会进行振动数据的判断。
            work_flag = Simple_judgment(tbm_cuter_speed, tbm_cutterheadTorque, tbm_speed, tbm_thrust)
            logger.info(f"当前TBM工作状态：{work_flag}")

            if config.work_status_enabled > 0 :
                logger.info("当前掘进工作状态判断条件开启")
                if work_flag == 3:
                    logger.info("当前为稳定段,开始计算")
                    start_flag = True
                else:
                    logger.info("当前为非稳定段，不进行计算")
                    start_flag = False
            else:
                logger.info("当前掘进工作状态判断条件关闭，直接进行振动信号计算")
                start_flag = True
    elif topic==config.sub_driver_box_topic:
        handle_vib_data(config.pub_topic_driver_box,2,data,config.signal_driver_box,config.driver_hist_num)
    elif topic==config.sub_motor_topic:    
        handle_vib_data(config.pub_topic_motor,0,data,config.signal_motor,config.motor_hist_num)
    elif topic==config.sub_reducer_topic:
        handle_vib_data(config.pub_topic_reducer,1,data,config.signal_reducer,config.reducer_hist_num)
        
    else:
        logger.info("no data that can be processed received")
    end_time = time.time()
    cal_time = end_time - start_time
    logger.info(f"程序运行花费的时间: {cal_time} 秒")
    # 清理旧日志
    clean_old_logs()

# 当客户端有日志信息时调用
def on_log(client, obj, level, string):
    print("Log:" + string)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"MQTT连接意外断开，返回码: {rc}")
        logger.info("尝试重新连接...")
    else:
        logger.info("MQTT连接正常断开")

def subscribe_topics(client):
    """订阅所有需要的主题"""
    try:
        client.subscribe(config.sub_motor_topic)
        client.subscribe(config.sub_reducer_topic)
        client.subscribe(config.sub_driver_box_topic)
        client.subscribe(config.sub_tbm_topic)
        logger.info("订阅所有主题成功")
        return True
    except Exception as e:
        logger.error(f"订阅主题失败: {e}")
        return False

if __name__ == "__main__":
    # step1: 读取本地yaml文件进行配置信息的读取
    yaml_file_path = "./drivers_vib_cal_config.yaml"
    logger.info(f"程序配置文件为：{yaml_file_path}")
    config = get_config_from_yaml(yaml_file_path)
    
    # step2：创建一个本地数据库
    #dbtables = init_db(config.motor_num, config.reducer_num, config.driver_box_points)
    # 全局变量
    tbm_cuter_speed = 0
    tbm_cutterheadTorque = 0
    tbm_speed = 0
    tbm_thrust = 0
    # 创建一个工作状态表示掘进机当前的工作状态
    work_flag = 0
    if config.work_status_enabled > 0 :
        print("当前掘进工作状态判断条件开启")
        start_flag = False
    else:
        print("当前掘进工作状态判断条件关闭")
        start_flag = True

    # step3: 对mqtt参数进行配置
    try:
        # 实例化
        client = mqtt.Client( client_id=config.id)  # mqtt.CallbackAPIVersion.VERSION1,
        # 判断是否需要对登录名和密码设置
        if config.login_flag:
            client.username_pw_set(username=config.login_name, password=config.password)
        # 回调函数
        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_message = on_message
        client.on_log = on_log
        client.on_disconnect = on_disconnect
        client.reconnect_delay_set(min_delay=1, max_delay=120)
        connected = False
        while not connected:
            try:
                logger.info(f"尝试连接MQTT服务器 {config.host}:{config.port}")
                client.connect(host=config.host, port=config.port, keepalive=config.keepalive)
                connected = True
                logger.info("MQTT服务器连接成功")
            except Exception as e:
                logger.error(f"MQTT服务器连接失败: {e}")
                logger.info("5秒后重新尝试连接...")
                time.sleep(5)  # 等待5秒后重试
        client.loop_forever()
    except Exception as e:
        # 捕获异常并记录到日志
        logger.error(f"Program crashed with error: {e}")
        logger.error(traceback.format_exc())
    finally:
        # 清理旧日志
        clean_old_logs()

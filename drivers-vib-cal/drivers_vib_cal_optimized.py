#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/18 14:50
# @Author  : wangheng
# @File    : drivers_vib_cal_optimized.py
# @Description : 优化后的驱动组振动信号处理配置模块

import logging
import os
import yaml
from dataclasses import dataclass
from copy import deepcopy

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    signal_driver_box: list
    driver_box_points: int
    driver_hist_num: int
    id: str
    host: str
    port: int
    keepalive: int
    sub_vib_topic: str
    sub_tbm_topic: str  # 修复拼写错误: sub_tbm_toic -> sub_tbm_topic
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
        'signal_motor': 'motorAcc',
        'motor_num': 16,
        'motor_hist_num': 30
    },
    'reducer_vib_config': {
        'signal_reducer': 'reducerAcc',
        'reducer_num': 16,
        'reducer_hist_num': 30
    },
    'driver_box_vib_config': {
        'signal_driver_box': ['acc->1', 'acc->2', 'acc->3', 'acc->4'],
        'driver_box_points': 4,
        'driver_hist_num': 30
    },
    'mqtt_config': {
        'id': 'motor_vib_cal1',
        'host': '127.0.0.1',
        'port': 1883,
        'keepalive': 6000,
        'sub_vib_topic': 'crec/data/realTime/driverGroup',
        'sub_tbm_topic': 'crec/data/realTime/driverBox',  # 修复拼写错误
        'pub_topic_motor': 'crec/computing/realTime/driverGroup/motorVibStatistics',
        'pub_topic_reducer': 'crec/computing/realTime/driverGroup/reducerVibStatistics',
        'pub_topic_driver_box': 'crec/computing/realTime/driverBox/VibStatistics',
        'pub_topic_alarm': 'crec/alarm/post',
        'login_flag': False,
        'login_name': 'admin',
        'password': 'admin123'
    }
}

def _is_valid_value(value):
    """检查值是否有效（非0、非空字符串、非None）"""
    return value not in [0, "", None]

def _merge_config(default, custom):
    """递归合并默认配置和自定义配置"""
    if isinstance(default, dict) and isinstance(custom, dict):
        merged = deepcopy(default)
        for key, value in custom.items():
            if key in merged:
                merged[key] = _merge_config(merged[key], value)
            else:
                merged[key] = value
        return merged
    return custom if _is_valid_value(custom) else default

def get_config_from_yaml(yaml_file_path) -> Config:
    """从YAML文件加载配置，返回Config对象"""
    try:
        logger.info("开始读取yaml文件....")
        with open(yaml_file_path, 'r', encoding='utf-8') as file:
            custom_config = yaml.safe_load(file) or {}

        # 合并配置
        merged_config = _merge_config(DEFAULT_CONFIG, custom_config)

        # 提取配置参数并进行类型验证
        motor_config = merged_config['motor_vib_config']
        reducer_config = merged_config['reducer_vib_config']
        driver_box_config = merged_config['driver_box_vib_config']
        mqtt_config = merged_config['mqtt_config']

        # 类型验证示例（关键参数）
        if not isinstance(mqtt_config['port'], int):
            logger.warning(f"端口号 {mqtt_config['port']} 不是整数，使用默认值 {DEFAULT_CONFIG['mqtt_config']['port']}")
            mqtt_config['port'] = DEFAULT_CONFIG['mqtt_config']['port']

        return Config(
            work_status_enabled=merged_config['work_status_enabled'],
            signal_motor=motor_config['signal_motor'],
            motor_num=motor_config['motor_num'],
            motor_hist_num=motor_config['motor_hist_num'],
            signal_reducer=reducer_config['signal_reducer'],
            reducer_num=reducer_config['reducer_num'],
            reducer_hist_num=reducer_config['reducer_hist_num'],
            signal_driver_box=driver_box_config['signal_driver_box'],
            driver_box_points=driver_box_config['driver_box_points'],
            driver_hist_num=driver_box_config['driver_hist_num'],
            id=mqtt_config['id'],
            host=mqtt_config['host'],
            port=mqtt_config['port'],
            keepalive=mqtt_config['keepalive'],
            sub_vib_topic=mqtt_config['sub_vib_topic'],
            sub_tbm_topic=mqtt_config['sub_tbm_topic'],  # 使用修复后的变量名
            pub_topic_motor=mqtt_config['pub_topic_motor'],
            pub_topic_reducer=mqtt_config['pub_topic_reducer'],
            pub_topic_driver_box=mqtt_config['pub_topic_driver_box'],
            pub_topic_alarm=mqtt_config['pub_topic_alarm'],
            login_flag=mqtt_config['login_flag'],
            login_name=mqtt_config['login_name'],
            password=mqtt_config['password']
        )

    except FileNotFoundError:
        logger.error(f"配置文件 {yaml_file_path} 未找到，使用默认配置")
    except yaml.YAMLError as exc:
        logger.error(f"YAML文件解析错误: {exc}", exc_info=True)
    except KeyError as exc:
        logger.error(f"配置文件缺少必要键: {exc}", exc_info=True)
    except Exception as exc:
        logger.error(f"配置加载失败: {exc}", exc_info=True)

    # 所有异常情况下返回默认配置
    default = DEFAULT_CONFIG
    return Config(
        work_status_enabled=default['work_status_enabled'],
        signal_motor=default['motor_vib_config']['signal_motor'],
        motor_num=default['motor_vib_config']['motor_num'],
        motor_hist_num=default['motor_vib_config']['motor_hist_num'],
        signal_reducer=default['reducer_vib_config']['signal_reducer'],
        reducer_num=default['reducer_vib_config']['reducer_num'],
        reducer_hist_num=default['reducer_vib_config']['reducer_hist_num'],
        signal_driver_box=default['driver_box_vib_config']['signal_driver_box'],
        driver_box_points=default['driver_box_vib_config']['driver_box_points'],
        driver_hist_num=default['driver_box_vib_config']['driver_hist_num'],
        id=default['mqtt_config']['id'],
        host=default['mqtt_config']['host'],
        port=default['mqtt_config']['port'],
        keepalive=default['mqtt_config']['keepalive'],
        sub_vib_topic=default['mqtt_config']['sub_vib_topic'],
        sub_tbm_topic=default['mqtt_config']['sub_tbm_topic'],
        pub_topic_motor=default['mqtt_config']['pub_topic_motor'],
        pub_topic_reducer=default['mqtt_config']['pub_topic_reducer'],
        pub_topic_driver_box=default['mqtt_config']['pub_topic_driver_box'],
        pub_topic_alarm=default['mqtt_config']['pub_topic_alarm'],
        login_flag=default['mqtt_config']['login_flag'],
        login_name=default['mqtt_config']['login_name'],
        password=default['mqtt_config']['password']
    )
# -*- coding: utf-8 -*-
"""
本模块负责数据库的设置和会话管理。
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config import Settings

# --- 数据库连接配置 ---
# 从配置中加载设置
settings = Settings()

# --- SQLAlchemy 引擎 ---
# 使用从配置中获取的 db_url 创建 SQLAlchemy 引擎。
# `connect_args={"check_same_thread": False}` 是 SQLite 特有的配置，
# 允许多个线程共享同一个连接，这对于 FastAPI 的运行方式是必需的。
engine = create_engine(
    settings.db_url, connect_args={"check_same_thread": False}
)

# --- 数据库会话 ---
# 创建一个 SessionLocal 类，它是数据库会话的工厂。
# autocommit=False 和 autoflush=False 确保事务控制是手动的。
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- 数据模型基类 ---
# 创建一个 Base 类，我们之后创建的所有 ORM 模型都将继承这个类。
Base = declarative_base()

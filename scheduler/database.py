from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# -*- coding: utf-8 -*-
"""
本模块负责数据库的设置和会话管理。
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- 数据库连接配置 ---
# 定义 SQLite 数据库文件的路径。使用 /tmp 目录以确保在不同环境中都具有写入权限。
SQLALCHEMY_DATABASE_URL = "sqlite:////tmp/scheduler.db"

# --- SQLAlchemy 引擎 ---
# 创建 SQLAlchemy 引擎。
# `connect_args={"check_same_thread": False}` 是 SQLite 特有的配置，
# 允许多个线程共享同一个连接，这对于 FastAPI 的运行方式是必需的。
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

# --- 数据库会话 ---
# 创建一个 SessionLocal 类，它是数据库会话的工厂。
# autocommit=False 和 autoflush=False 确保事务控制是手动的。
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- 数据模型基类 ---
# 创建一个 Base 类，我们之后创建的所有 ORM 模型都将继承这个类。
Base = declarative_base()

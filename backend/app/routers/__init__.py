"""业务路由汇总：增量归并专用路由先注册，固定子路径优先于通用资源路由。"""
from __future__ import annotations

from app.routers import delta_merge, resource

ROUTERS = [delta_merge, resource]

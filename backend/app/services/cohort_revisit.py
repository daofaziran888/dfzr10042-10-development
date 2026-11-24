"""同期群回访批次领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class CohortRevisitService(BaseModuleService):
    MODULE = "cohort_revisit"
    ENTITY = "回访批次"
    OPERATION_TERM = "导入"
    LIST_FIELDS = ["批次编号", "所属群组", "来源文件", "回访行数", "拒绝行数", "重复行数", "解析失败", "确认时间"]
    REQUIRED_FIELDS = ["批次编号", "所属群组", "来源文件"]
    STATUS_ORDER = ["待确认", "已确认", "部分解析失败", "已作废"]
    ACTION_RULES = {"确认批次": "已确认", "标记解析失败": "部分解析失败", "作废批次": "已作废"}
    NEGATIVE_ACTIONS = ["作废批次"]

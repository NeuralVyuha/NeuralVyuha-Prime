"""
Workflow Engine Models — Pydantic schemas for workflow definitions and executions.
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from enum import Enum


class StepType(str, Enum):
    ENRICH = "enrich"
    NOTIFY = "notify"
    WAIT = "wait"
    CONDITION = "condition"
    CREATE_CASE = "create_case"
    HTTP = "http"


class WorkflowStep(BaseModel):
    name: str
    type: StepType
    config: Dict[str, Any] = {}
    on_failure: str = "continue"  # "continue", "abort", "retry"


class WorkflowTrigger(BaseModel):
    type: str = "kafka"  # "kafka", "manual", "schedule"
    topic: Optional[str] = "workflow.trigger.v1"
    cel_condition: Optional[str] = None  # CEL guard before execution


class WorkflowDefinition(BaseModel):
    workflow_id: Optional[str] = None
    name: str
    description: str = ""
    tenant_id: str = "dev-tenant"
    trigger: WorkflowTrigger = WorkflowTrigger()
    steps: List[WorkflowStep] = []
    enabled: bool = True


class WorkflowExecution(BaseModel):
    execution_id: str
    workflow_id: str
    tenant_id: str
    status: str = "RUNNING"  # RUNNING, COMPLETED, FAILED, ABORTED
    current_step: int = 0
    total_steps: int = 0
    started_at: int = 0
    finished_at: Optional[int] = None
    trigger_event: Dict[str, Any] = {}
    step_results: List[Dict[str, Any]] = []
    error: Optional[str] = None

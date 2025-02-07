"""
This module defines constants used throughout the project, including Pydantic models for data validation,
Spark schemas for DataFrame constructions, and file paths for data storage and results output.
"""

from datetime import datetime

from pydantic import BaseModel
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    TimestampType,
)


# Pydantic Schemas
class PharmacySchema(BaseModel):
    """Schema for pharmacy data entries."""

    chain: str
    npi: str


class ClaimSchema(BaseModel):
    """Schema for Claims data entries."""

    id: str
    npi: str
    ndc: str
    price: float
    quantity: float
    timestamp: datetime


class RevertSchema(BaseModel):
    """Schema for Reverts data entries."""

    id: str
    claim_id: str
    timestamp: datetime


# Spark Schemas
PHARMACY_SCHEMA = StructType(
    [StructField("chain", StringType(), True), StructField("npi", StringType(), True)]
)

CLAIMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("npi", StringType(), True),
        StructField("ndc", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("quantity", FloatType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)


# Paths
DATA_DIR = "./data"
PHARMACY_DIR = f"{DATA_DIR}/pharmacies"
CLAIMS_DIR = f"{DATA_DIR}/claims"
REVERTS_DIR = f"{DATA_DIR}/reverts"

TASK_1_2_OUTPUT = "task_1_2/result.json"
TASK_3_OUTPUT = "task_3/"
TASK_4_OUTPUT = "task_4/"

INCORRECT_DATA_OUTPUT = "task_1_2/incorrect_data.json"

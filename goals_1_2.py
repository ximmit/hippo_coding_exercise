"""
This module is designed for processing and validating pharmacy, claims, and revert data files.
It reads data from specified directories, validates it against predefined Pydantic schemas,
and calculates various metrics for claims and reverts. Outputs are saved in JSON format.
"""

import os
import json
import csv
from typing import Optional, List
from pydantic import ValidationError

from constants import (
    PHARMACY_DIR,
    CLAIMS_DIR,
    REVERTS_DIR,
    TASK_1_2_OUTPUT,
    PharmacySchema,
    ClaimSchema,
    RevertSchema,
    INCORRECT_DATA_OUTPUT,
)


def read_json_or_csv(file_path: str) -> Optional[List]:
    """Read and return data from a JSON or CSV file."""
    result = None
    try:
        if file_path.endswith(".json"):
            with open(file_path, "r", encoding="utf-8") as f:
                result = json.load(f)
        elif file_path.endswith(".csv"):
            with open(file_path, "r", encoding="utf-8") as f:
                result = list(csv.DictReader(f))
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {file_path}")
    except csv.Error:
        print(f"Error reading CSV from {file_path}")
    return result


# Function to load and validate the data
def load_json_files(directory: str, schema):
    """Load, validate, and return data from all files in a directory against a specified schema. Goal 1"""
    data = []
    incorrect_rows = []
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        try:
            records = read_json_or_csv(filepath)
            for record in records:
                try:
                    validated_data = schema(**record)
                    data.append(validated_data.model_dump())
                except ValidationError as e:
                    print(f"Schema validation error in {filename}: {e}")
                    incorrect_rows.append(
                        {"file": filename, "data": record, "error": str(e)}
                    )
        except json.JSONDecodeError as e:
            print(f"Error reading {filename}: {e}")

    return data, incorrect_rows


def save_to_json(output_file: str, results: List) -> None:
    """Save data to a JSON file with formatting."""
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4, default=str)
        print(f"Output saved to {output_file}")
    except IOError as e:
        print(f"Error opening/writing to {output_file}: {e}")


def calculate_metrics(claims: List, reverts: List) -> List:
    """Calculate metrics based on claims and reverts data. Goal 2"""
    metrics = {}

    # Process claims
    for claim in claims:
        key = (claim["npi"], claim["ndc"])
        if key not in metrics:
            metrics[key] = {
                "fills": 0,
                "reverted": 0,
                "total_price": 0.0,
                "avg_price": 0.0,
            }
        metrics[key]["fills"] += 1
        metrics[key]["total_price"] += claim["price"]

    # Process reverts
    reverted_claims = {revert["claim_id"]: True for revert in reverts}
    for claim in claims:
        if claim["id"] in reverted_claims:
            key = (claim["npi"], claim["ndc"])
            if key in metrics:
                metrics[key]["reverted"] += 1

    # Calculate average price
    for key, data in metrics.items():
        if data["fills"] > 0:
            data["avg_price"] = data["total_price"] / data["fills"]

    # Format the results
    formatted_results = []
    for (npi, ndc), data in metrics.items():
        formatted_results.append(
            {
                "npi": npi,
                "ndc": ndc,
                "fills": data["fills"],
                "reverted": data["reverted"],
                "avg_price": round(data["avg_price"], 2),
                "total_price": round(data["total_price"], 5),
            }
        )

    return formatted_results


if __name__ == "__main__":
    # Read JSONs
    pharmacy_data, pharmacy_data_incorrect = load_json_files(
        PHARMACY_DIR, PharmacySchema
    )
    claims_data, claims_data_incorrect = load_json_files(CLAIMS_DIR, ClaimSchema)
    reverts_data, reverts_data_incorrect = load_json_files(REVERTS_DIR, RevertSchema)

    # Collect incorrect data
    incorrect_data = (
        pharmacy_data_incorrect + claims_data_incorrect + reverts_data_incorrect
    )

    # Calculate metrics
    metrics_results = calculate_metrics(claims_data, reverts_data)

    # Save the data
    save_to_json(TASK_1_2_OUTPUT, metrics_results)
    save_to_json(INCORRECT_DATA_OUTPUT, incorrect_data)

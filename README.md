# Data Vault Validation Tool

## Overview

This tool validates data integrity across a Data Vault implementation in Snowflake by comparing source tables with hub, satellite, and business view tables. It identifies records that failed to migrate properly between layers, detects data representation differences, and provides detailed reports for troubleshooting data lineage issues.

## Features

- **Complete Data Flow Validation**: Traces data from source tables through hubs, satellites, and finally to business views
- **Smart Difference Detection**: Distinguishes between actual missing records and data representation differences
- **Adaptive Bizview Validation**: Understands business rules and expected filtering in different bizview types 
- **Context-Aware Reporting**: Provides detailed explanations of findings with suggested resolution paths
- **False Positive Reduction**: Uses multiple validation techniques to eliminate false positives
- **Performance Optimized**: Handles large data volumes efficiently with adaptive sampling and query optimization

## Prerequisites

- Snowflake account with appropriate access permissions
- Python 3.8+
- Required packages:
  - `snowflake-snowpark-python>=1.5.0`
  - `pandas>=1.3.5`

## Setup

1. Install required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure your Snowflake connection parameters in your environment or script.

3. Customize the `table_configs` list in the script to match your Data Vault architecture.

## Configuration

The script is driven by a `table_configs` list containing dictionaries that define the mapping between your source tables and Data Vault structures. For each entity, you'll configure:

```python
{
    "source_table": "SOURCE_DB.SOURCE_SCHEMA.PRODUCTS",          # Source table
    "hub_table": "DATAVAULT_DB.RAWVAULT.H_PRODUCT",              # Hub table
    "cur_satellite_table": "DATAVAULT_DB.RAWVAULT.S_PRODUCT_LROC_INFO",  # Current satellite
    "bizview_table": "BIZVIEW_DB.BIZVIEWS.DIM_PRODUCT",          # Business view
    "source_key": "PRODUCT_ID",                                  # Primary key in source
    "hub_key": "PRODUCT_HID",                                    # Business key in hub
    "satellite_hash_key": "HK_H_PRODUCT",                        # Hash key in satellite
    "bizview_key": "PRODUCT_KEY",                                # Business key in bizview
    "deleted_column": "IS_DELETED",                              # Deleted flag column
    "columns_to_compare": [                                      # Columns to validate
        "NAME", "DESCRIPTION", "SKU", "STATUS", ...
    ],
    "custom_except_query": """                                   # Custom SQL for detailed validation
    SELECT PRODUCT_ID, NAME, DESCRIPTION, ...
    FROM SOURCE_DB.SOURCE_SCHEMA.PRODUCTS
    EXCEPT
    SELECT HP.PRODUCT_HID, NAME, DESCRIPTION, ...
    FROM DATAVAULT_DB.RAWVAULT.H_PRODUCT HP
    JOIN DATAVAULT_DB.RAWVAULT.S_PRODUCT_LROC_INFO SP ON HP.HK_H_PRODUCT = SP.HK_H_PRODUCT
    """
}
```

### Link Table Configuration

For link entities (representing relationships), the configuration needs additional parameters:

```python
{
    "source_table": "SOURCE_DB.SOURCE_SCHEMA.ORDER_ITEMS",
    "hub_tables": ["DATAVAULT_DB.RAWVAULT.H_ORDER", "DATAVAULT_DB.RAWVAULT.H_PRODUCT"],
    "link_table": "DATAVAULT_DB.RAWVAULT.L_ORDER_PRODUCT",
    "cur_satellite_table": "DATAVAULT_DB.RAWVAULT.S_ORDER_PRODUCT_LROC_INFO",
    "bizview_table": "BIZVIEW_DB.BIZVIEWS.FACT_ORDER_ITEM",
    "source_key": "ORDER_ITEM_ID",
    "hub_keys": ["ORDER_ID", "PRODUCT_ID"],
    "link_hash_keys": ["HK_H_ORDER", "HK_H_PRODUCT"],
    "satellite_hash_key": "HK_L_ORDER_PRODUCT",
    "bizview_key": "ORDER_ITEM_ID",
    "deleted_column": "IS_DELETED",
    ...
}
```

## Usage

### As Python Script

```python
# Example usage
from snowflake.snowpark import Session
from data_vault_validator import main

# Create a Snowflake session
connection_parameters = {
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "role": "your_role",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
}

session = Session.builder.configs(connection_parameters).create()

# Run the validation
results = main(session)

# Display results
results.show()

# Optionally save results to a Snowflake table
results.write.save_as_table("DATA_VAULT_VALIDATION_RESULTS", mode="overwrite")
```

### As Snowflake Stored Procedure

```sql
CREATE OR REPLACE PROCEDURE RUN_DATA_VAULT_VALIDATION()
RETURNS TABLE (
    TABLE_NAME STRING,
    SOURCE_TABLE STRING,
    HUB_TABLE STRING,
    SATELLITE_TABLE STRING,
    BIZVIEW_TABLE STRING,
    SOURCE_COUNT NUMBER,
    SOURCE_COUNT_NONDELETED NUMBER,
    HUB_COUNT NUMBER,
    SATELLITE_COUNT NUMBER,
    BIZVIEW_COUNT NUMBER,
    SOURCE_TO_VAULT_DIFFERENCES NUMBER,
    DATA_REPRESENTATION_DIFFERENCES NUMBER, 
    BIZVIEW_MISSING_RECORDS NUMBER,
    DELETED_RECORDS NUMBER,
    VALIDATION_MESSAGE STRING,
    DATA_DISCREPANCY_DETAILS VARIANT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'data_vault_validator.main'
AS
$$
# Paste the entire script here
$$;

-- Execute the procedure:
CALL RUN_DATA_VAULT_VALIDATION();
```

## Output Format

The script produces a DataFrame with the following columns:

| Column                          | Description                                      |
|---------------------------------|--------------------------------------------------|
| TABLE_NAME                      | The entity name                                  |
| SOURCE_TABLE                    | Full path to source table                        |
| HUB_TABLE                       | Full path to hub table                           |
| SATELLITE_TABLE                 | Full path to satellite table                     |
| BIZVIEW_TABLE                   | Full path to business view                       |
| SOURCE_COUNT                    | Total count of records in source                 |
| SOURCE_COUNT_NONDELETED         | Count of non-deleted records in source           |
| HUB_COUNT                       | Count of records in hub table                    |
| SATELLITE_COUNT                 | Count of records in satellite table              |
| BIZVIEW_COUNT                   | Count of records in business view                |
| SOURCE_TO_VAULT_DIFFERENCES     | Records truly missing between source and vault   |
| DATA_REPRESENTATION_DIFFERENCES | Records with different data representation       |
| BIZVIEW_MISSING_RECORDS         | Records missing in bizview beyond thresholds     |
| DELETED_RECORDS                 | Count of properly deleted records                |
| VALIDATION_MESSAGE              | Detailed explanation of validation findings      |
| DATA_DISCREPANCY_DETAILS        | JSON with detailed samples and explanations      |

## Understanding the Results

### Source to Vault Validation

The tool examines two types of data discrepancies between source and vault:

1. **True Missing Records**: Records that exist in the source but are completely absent from the vault (both hub and satellite)
2. **Data Representation Differences**: Records that exist in both source and vault but with differences in how data is represented (timestamps, formatting, null handling, etc.)

### Bizview Validation

For business views, the tool applies business-aware validation that understands:

- **Dimension views** typically have a 1:1 relationship with hubs
- **Fact views** often have intentional filtering based on business rules
- **Current/Active views** are expected to have fewer records by design

The tool applies adaptive thresholds based on the view type to reduce false positives.

## Advanced Features

### Key-only Validation

When schema differences are suspected, the tool can perform key-only validation to check if business keys are present even when attribute formats differ.

### Adaptive Thresholds

The tool applies different validation thresholds based on:
- Table type (dimension vs. fact)
- Data volume (adjusts percentage thresholds for large datasets)
- Expected business filtering patterns

### Detailed Discrepancy Analysis

The `DATA_DISCREPANCY_DETAILS` column contains JSON with:
- Sample records with discrepancies
- Detailed explanation of findings
- Key metrics and comparisons
- Recommendations for resolution

## Best Practices

- Run the validation after each ETL load to catch issues early
- Review the `VALIDATION_MESSAGE` field for context on findings
- Focus on `SOURCE_TO_VAULT_DIFFERENCES` as the primary measure of data loss
- For bizviews, consider if differences are due to intentional business filtering
- Customize `custom_except_query` for entity-specific comparison logic

## Troubleshooting Common Issues

- **High Data Representation Differences**: Often indicates timestamp precision differences, case sensitivity, or whitespace handling variations
- **Empty Bizviews**: Check for business rule filters being applied incorrectly
- **Performance Issues**: For very large tables, consider adding time-based filtering to the custom queries
- **False Positives**: Refine custom queries to handle known variations in data representation

## Extending the Tool

The tool can be extended to support:
- Historical satellite validation
- Real-time monitoring and alerting
- Cross-environment comparisons (dev/test/prod)
- Time-series analysis of data quality metrics

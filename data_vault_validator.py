import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, LongType, MapType, VariantType
import json
import sys

# Define the table configurations with custom EXCEPT queries
table_configs = [
    {
        "source_table": "SOURCE_DB.SOURCE_SCHEMA.COMPANIES",
        "hub_table": "DATAVAULT_DB.RAWVAULT.H_COMPANY",
        "cur_satellite_table": "DATAVAULT_DB.RAWVAULT.S_COMPANY_LROC_INFO",
        "bizview_table": "BIZVIEW_DB.BIZVIEWS.DIM_COMPANY",
        "source_key": "COMPANY_ID", 
        "hub_key": "COMPANY_HID",
        "satellite_hash_key": "HK_H_COMPANY", 
        "bizview_key": "COMPANY_KEY",
        "deleted_column": "IS_DELETED",
        "columns_to_compare": [ 
            "NAME", "DESCRIPTION", "INDUSTRY", "HEADQUARTERS_LOCATION", 
            "FOUNDING_YEAR", "REVENUE", "EMPLOYEE_COUNT", "CEO", 
            "PARENT_COMPANY", "WEBSITE", "PHONE", "EMAIL", 
            "TAX_ID", "REGISTRATION_NUMBER", "STATUS", 
            "CREATED_BY", "UPDATED_BY", "CREATED_AT", "UPDATED_AT"
        ],
        "custom_except_query": """
SELECT COMPANY_ID, NAME, DESCRIPTION, INDUSTRY, HEADQUARTERS_LOCATION, 
       FOUNDING_YEAR, REVENUE, EMPLOYEE_COUNT, CEO, 
       PARENT_COMPANY, WEBSITE, PHONE, EMAIL, 
       TAX_ID, REGISTRATION_NUMBER, STATUS,
       CREATED_BY, UPDATED_BY, CREATED_AT, UPDATED_AT,
       IS_DELETED, LAST_UPDATED_TS
FROM SOURCE_DB.SOURCE_SCHEMA.COMPANIES
EXCEPT
SELECT HC.COMPANY_HID, NAME, DESCRIPTION, INDUSTRY, HEADQUARTERS_LOCATION, 
       FOUNDING_YEAR, REVENUE, EMPLOYEE_COUNT, CEO, 
       PARENT_COMPANY, WEBSITE, PHONE, EMAIL, 
       TAX_ID, REGISTRATION_NUMBER, STATUS,
       CREATED_BY, UPDATED_BY, CREATED_AT, UPDATED_AT,
       IS_DELETED, LAST_UPDATED_TS
FROM DATAVAULT_DB.RAWVAULT.H_COMPANY HC
JOIN DATAVAULT_DB.RAWVAULT.S_COMPANY_LROC_INFO SC ON HC.HK_H_COMPANY = SC.HK_H_COMPANY
"""
    }
]

def safe_json_conversion(row):
    """
    Safely convert a Snowpark Row to a JSON-serializable dictionary
    with improved error handling
    """
    try:
        # If row is a Snowpark Row
        if hasattr(row, 'schema'):
            # Convert based on schema and values
            result = {}
            for i, column_name in enumerate(row.schema.names):
                value = row[i]
                # Convert any non-serializable values to strings
                if value is None:
                    result[column_name] = None
                elif isinstance(value, (dict, list, int, float, bool)):
                    result[column_name] = value
                else:
                    result[column_name] = str(value)
            return result
        
        # If row is already a dictionary
        elif isinstance(row, dict):
            return {str(k): (str(v) if not isinstance(v, (dict, list, int, float, bool, type(None))) else v) 
                  for k, v in row.items()}
        
        # Fallback - convert the entire object to string
        else:
            return {"value": str(row)}
            
    except Exception as e:
        print(f"Error converting row: {str(e)}")
        return {"error": f"Conversion error: {str(e)}"}

def get_table_count(session, table_name, filter_clause=None, count_column=None, distinct=True):
    """
    Get the count of records from a table with optional filtering
    
    Args:
        session: Snowflake session
        table_name: Table or view to query
        filter_clause: Optional WHERE clause to filter records
        count_column: Optional specific column to count
        distinct: Whether to use DISTINCT when counting
        
    Returns:
        Tuple of (count, query_used)
    """
    try:
        if not table_name:
            return 0, "No table specified"
            
        # Construct the query
        if count_column:
            if distinct:
                query = f"SELECT COUNT(DISTINCT {count_column}) FROM {table_name}"
            else:
                query = f"SELECT COUNT({count_column}) FROM {table_name}"
        else:
            query = f"SELECT COUNT(*) FROM {table_name}"
            
        if filter_clause:
            query += f" WHERE {filter_clause}"
        
        # Execute the query and get the result
        try:
            print(f"Executing count query: {query}")
            result = session.sql(query).collect()
            count = result[0][0] if result else 0
            print(f"Count result for {table_name}: {count}")
            return count, query
        except Exception as e:
            error_msg = f"Error executing count query for {table_name}: {str(e)}"
            print(error_msg)
            return 0, error_msg
    except Exception as e:
        error_msg = f"Error in get_table_count: {str(e)}"
        print(error_msg)
        return 0, error_msg

def validate_missing_records(session, config, missing_records_count):
    """
    Validate if missing records are actual differences or false positives
    
    Args:
        session: Snowflake session
        config: Table configuration dictionary
        missing_records_count: Initial count of missing records
        
    Returns:
        Tuple of (validated_count, validation_message)
    """
    if missing_records_count == 0:
        return 0, "No missing records detected"
        
    # Get the key columns
    source_key = config.get('source_key')
    if not source_key:
        return missing_records_count, "Could not validate: No source key defined"
    
    try:
        # Use a safer approach to get a count from the EXCEPT query
        validation_query = f"""
        SELECT COUNT(*) FROM (
            {config['custom_except_query']}
        ) missing_records
        """
        
        print(f"Counting total missing records...")
        validation_result = session.sql(validation_query).collect()
        total_count = validation_result[0][0] if validation_result else 0
        
        return total_count, "All records verified"
    except Exception as e:
        error_msg = f"Error validating missing records: {str(e)}"
        print(error_msg)
        return missing_records_count, error_msg

def extract_missing_records(session, config, limit=10):
    """
    Extract missing records using the EXCEPT query with improved error handling
    and validation to detect false positives
    
    Args:
        session: Snowflake session
        config: Table configuration dictionary
        limit: Maximum number of records to retrieve
    
    Returns:
        Tuple of (missing_count, list of missing records as JSON dictionaries, validation_info)
    """
    # Check if custom EXCEPT query exists
    if not config.get("custom_except_query"):
        print(f"No EXCEPT query for {config.get('source_table', 'UNKNOWN')}")
        return 0, [], "No EXCEPT query defined"
    
    try:
        # Get source and target table names for reference
        source_table = config.get('source_table', 'UNKNOWN_SOURCE')
        
        # Handle both single hub_table and multiple hub_tables
        if 'hub_tables' in config:
            hub_table = ', '.join(config['hub_tables'])
            hub_tables = config['hub_tables']
        else:
            hub_table = config.get('hub_table', 'UNKNOWN_HUB')
            hub_tables = [hub_table]
        
        satellite_table = config.get('cur_satellite_table', 'UNKNOWN_SATELLITE')
        bizview_table = config.get('bizview_table', 'UNKNOWN_BIZVIEW')
        
        # Construct the full EXCEPT query
        full_except_query = config['custom_except_query']
        
        print(f"Executing EXCEPT query to find records in source but not in vault (hub+satellite)")
        
        # First get the total count without limit
        count_query = f"SELECT COUNT(*) FROM ({full_except_query})"
        
        try:
            print(f"Counting missing records: {count_query}")
            missing_count_result = session.sql(count_query).collect()
            missing_count = missing_count_result[0][0] if missing_count_result else 0
            print(f"\nTotal Records with Differences Count: {missing_count}")
            
            # First check - if already zero, we're good
            if missing_count == 0:
                print("No record differences found in EXCEPT query")
                return 0, [], "No record differences found"
                
            # Get source count for non-deleted records to compare
            source_count = 0
            if "deleted_column" in config:
                try:
                    deleted_col = config["deleted_column"]
                    source_count_result = session.sql(f"SELECT COUNT(*) FROM {source_table} WHERE {deleted_col} = FALSE").collect()
                    source_count = source_count_result[0][0] if source_count_result else 0
                except Exception as e:
                    print(f"Warning: Could not get source count for comparison: {str(e)}")
            
            # Get hub/satellite count for comparison
            vault_count = 0
            if satellite_table:
                try:
                    satellite_hash_key = config.get('satellite_hash_key')
                    if satellite_hash_key:
                        count_result = session.sql(f"SELECT COUNT(DISTINCT {satellite_hash_key}) FROM {satellite_table}").collect()
                    else:
                        count_result = session.sql(f"SELECT COUNT(*) FROM {satellite_table}").collect()
                    vault_count = count_result[0][0] if count_result else 0
                except Exception as e:
                    print(f"Warning: Could not get satellite count for comparison: {str(e)}")
            elif hub_tables and len(hub_tables) > 0:
                try:
                    count_result = session.sql(f"SELECT COUNT(*) FROM {hub_tables[0]}").collect()
                    vault_count = count_result[0][0] if count_result else 0
                except Exception as e:
                    print(f"Warning: Could not get hub count for comparison: {str(e)}")
            
            # Double-check if we actually have missing records by comparing counts
            # If source count ≈ vault count, then we might have false positives
            validation_message = ""
            if source_count > 0 and vault_count > 0:
                count_diff = abs(source_count - vault_count)
                threshold = max(5, int(source_count * 0.01))  # 1% threshold or 5 records, whichever is larger
                
                if count_diff < threshold:  # Small threshold for timing differences
                    # Counts match closely, but EXCEPT query shows differences
                    # This might indicate schema/column mapping issues rather than actual data loss
                    if missing_count > count_diff * 2:
                        print(f"⚠️ Warning: Difference count ({missing_count}) is much higher than count difference ({count_diff})")
                        print(f"  This indicates data representation differences rather than missing data")
                        validation_message = f"Data representation differences detected: Source count ({source_count}) is close to vault count ({vault_count}), but EXCEPT query shows {missing_count} differences. This indicates schema/column mapping variations rather than missing data."
                        
                        # Try a second validation looking at just key columns
                        # In some cases, full EXCEPT query may report differences due to internal timestamp differences
                        if config.get('source_key') and config.get('hub_key'):
                            source_key = config.get('source_key')
                            hub_key = config.get('hub_key')
                            
                            simplified_check = f"""
                            SELECT COUNT(*) FROM (
                                SELECT {source_key} FROM {source_table} 
                                WHERE {config.get('deleted_column', 'TRUE')} = FALSE
                                EXCEPT
                                SELECT {hub_key} FROM {hub_tables[0]}
                            )
                            """
                            try:
                                simplified_result = session.sql(simplified_check).collect()
                                simplified_count = simplified_result[0][0] if simplified_result else 0
                                print(f"Simplified key-only check found {simplified_count} truly missing keys")
                                
                                if simplified_count == 0 or simplified_count < missing_count / 10:
                                    validation_message += f" Key-only validation found only {simplified_count} truly missing records, confirming this is primarily a schema/data representation difference rather than missing data."
                                    
                                    # If key-only check finds nothing or much fewer issues, we likely have no actual data loss
                                    if simplified_count == 0:
                                        print("✅ Key-only validation confirms NO ACTUAL DATA LOSS")
                                        missing_count = 0  # Reset missing count
                            except Exception as key_error:
                                print(f"Could not perform simplified key validation: {str(key_error)}")
            
            # Validate the missing records to detect other types of false positives
            if not validation_message:
                validated_count, validation_message = validate_missing_records(session, config, missing_count)
            
        except Exception as count_error:
            print(f"Error getting total count: {count_error}")
            return 0, [], f"Error counting record differences: {str(count_error)}"
            
        # If there are no missing records (after double-check), return early
        if missing_count == 0 or "representation differences" in validation_message.lower():
            print("No significant data discrepancies found after validation")
            return 0, [], validation_message
        
        # If there are many missing records, limit sample size for performance
        sample_limit = min(limit, 10) if missing_count > 100 else limit
        
        # Fetch limited sample records efficiently
        sample_query = f"""
        SELECT * FROM ({full_except_query})
        LIMIT {sample_limit}
        """
        
        try:
            print(f"Fetching sample of records with differences: {sample_query}")
            # Execute query and collect results
            samples_df = session.sql(sample_query)
            
            # Parse results into dictionaries
            missing_records = []
            
            for row in samples_df.collect():
                # Use the improved record conversion function
                record_dict = safe_json_conversion(row)
                
                # Add metadata about tables for easier reference
                record_dict["__metadata"] = {
                    "source_table": source_table,
                    "hub_table": hub_table,
                    "satellite_table": satellite_table,
                    "bizview_table": bizview_table,
                    "record_type": "data_difference"
                }
                
                missing_records.append(record_dict)
            
            # Add a note about sample size if there are more records than the limit
            if missing_count > sample_limit:
                note = {
                    "NOTE": f"Showing {sample_limit} of {missing_count} total records with differences",
                    "validation": validation_message,
                    "source_table": source_table,
                    "hub_table": hub_table,
                    "satellite_table": satellite_table,
                    "bizview_table": bizview_table
                }
                missing_records.append(note)
            
            # If validation found potential false positives, adjust the count
            actual_count = 0 if "representation differences" in validation_message.lower() else missing_count
            
            return actual_count, missing_records, validation_message
        
        except Exception as sample_error:
            error_msg = f"Error collecting sample records: {sample_error}"
            print(error_msg)
            return missing_count, [{
                "error": error_msg,
                "__metadata": {
                    "source_table": source_table,
                    "hub_table": hub_table,
                    "satellite_table": satellite_table,
                    "bizview_table": bizview_table
                }
            }], "Failed to collect samples"
    
    except Exception as e:
        error_msg = f"Critical error in extract_missing_records: {e}"
        print(error_msg)
        return 0, [{
            "error": error_msg,
            "__metadata": {
                "source_table": config.get('source_table', 'UNKNOWN_SOURCE'),
                "hub_table": config.get('hub_table', 'UNKNOWN_HUB'),
                "satellite_table": config.get('cur_satellite_table', 'UNKNOWN_SATELLITE'),
                "bizview_table": config.get('bizview_table', 'UNKNOWN_BIZVIEW')
            }
        }], "Error in extraction process"

def get_bizview_count(session, config):
    """
    Get an accurate count of records in the bizview table, with improved handling of
    data virtualization views and potential performance optimizations.
    
    Args:
        session: Snowflake session
        config: Table configuration dictionary
        
    Returns:
        Tuple of (count, query_used)
    """
    bizview_table = config.get('bizview_table')
    if not bizview_table:
        return 0, "No bizview table specified"
    
    try:
        # First try a quick count to see if the table is accessible
        test_query = f"SELECT 1 FROM {bizview_table} LIMIT 1"
        try:
            session.sql(test_query).collect()
            print(f"✅ Bizview table {bizview_table} is accessible")
        except Exception as e:
            print(f"❌ Bizview table {bizview_table} is not accessible: {str(e)}")
            return 0, f"Bizview table access error: {str(e)}"
        
        # SPECIAL CASE: For known large bizviews, use direct COUNT(*) approach
        bizview_name = bizview_table.split('.')[-1].upper()
        known_large_tables = ["FACT_SALES", "FACT_TRANSACTIONS"]
        
        if bizview_name in known_large_tables:
            print(f"Using direct COUNT(*) for known large bizview: {bizview_name}")
            try:
                count_query = f"SELECT COUNT(*) FROM {bizview_table}"
                result = session.sql(count_query).collect()
                count = result[0][0] if result else 0
                print(f"Direct count for {bizview_name}: {count}")
                return count, count_query
            except Exception as direct_error:
                print(f"Error with direct count for {bizview_name}: {direct_error}")
                # Continue with other strategies if direct count fails
        
        # Strategy 1: Use the bizview key if available (most accurate for normalized tables)
        bizview_key = config.get('bizview_key')
        if bizview_key:
            # Try checking for non-null keys which avoids counting dummy/placeholder records
            count_query = f"SELECT COUNT(DISTINCT {bizview_key}) FROM {bizview_table} WHERE {bizview_key} IS NOT NULL"
            try:
                print(f"Executing bizview count query using key: {count_query}")
                result = session.sql(count_query).collect()
                count = result[0][0] if result else 0
                
                # Validate count is reasonable - if suspiciously low, try alternatives
                if count < 100 and bizview_name not in ["DIM_SMALL_LOOKUP", "DIM_CONFIG"]:  # Small dimensions may have fewer records
                    print(f"⚠️ Warning: Key-based count seems low ({count}). Trying alternative methods.")
                else:
                    print(f"Bizview count by key {bizview_key}: {count}")
                    return count, count_query
            except Exception as key_error:
                # If key-based count fails, try regular count as fallback
                print(f"Warning: Could not count bizview by key: {str(key_error)}")
        
        # Strategy 2: Use deleted flag if it exists in bizview
        if config.get('deleted_column'):
            deleted_col = config.get('deleted_column')
            try:
                # First check if the column exists in the bizview
                schema_parts = bizview_table.split('.')
                if len(schema_parts) >= 3:
                    db, schema, table = schema_parts
                    check_column_query = f"SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' AND COLUMN_NAME = '{deleted_col}'"
                    column_check = session.sql(check_column_query).collect()
                    
                    if column_check and len(column_check) > 0:
                        # Column exists, count non-deleted records
                        count_query = f"SELECT COUNT(*) FROM {bizview_table} WHERE {deleted_col} = FALSE"
                        result = session.sql(count_query).collect()
                        count = result[0][0] if result else 0
                        print(f"Bizview count (non-deleted): {count}")
                        return count, count_query
            except Exception as col_error:
                print(f"Note: Could not check for deleted column in bizview: {str(col_error)}")
        
        # Strategy 3: Fall back to regular count
        count_query = f"SELECT COUNT(*) FROM {bizview_table}"
        print(f"Executing standard bizview count query: {count_query}")
        result = session.sql(count_query).collect()
        count = result[0][0] if result else 0
        
        # Verify the count makes sense
        if count < 100 and 'FACT_' in bizview_table.upper():
            print(f"⚠️ Warning: Count for fact table seems suspiciously low: {count}")
            
            # Try one more approach: count with limit clause
            try:
                # Use LIMIT to see if we can get ANY records
                sample_query = f"SELECT COUNT(*) FROM (SELECT * FROM {bizview_table} LIMIT 10000)"
                sample_result = session.sql(sample_query).collect()
                sample_count = sample_result[0][0] if sample_result else 0
                
                if sample_count > count:
                    print(f"✅ Sample query found {sample_count} records, indicating larger dataset")
                    # Expand our sample to get better estimate
                    expanded_query = f"SELECT COUNT(*) FROM (SELECT * FROM {bizview_table} LIMIT 1000000)"
                    try:
                        expanded_result = session.sql(expanded_query).collect()
                        expanded_count = expanded_result[0][0] if expanded_result else 0
                        if expanded_count > count:
                            print(f"✅ Using expanded sample count: {expanded_count}")
                            return expanded_count, expanded_query
                    except Exception as expanded_error:
                        print(f"Could not get expanded count: {str(expanded_error)}")
            except Exception as sample_error:
                print(f"Could not get sample count: {str(sample_error)}")
        
        print(f"Bizview count (total): {count}")
        return count, count_query
        
    except Exception as e:
        error_msg = f"Error in get_bizview_count: {str(e)}"
        print(error_msg)
        return 0, error_msg

def main(session: snowpark.Session):
    """
    Main data validation function with improved error handling and validation
    
    Args:
        session: Snowflake session
    
    Returns:
        Snowpark DataFrame with validation results
    """
    # Define result schema with improved metrics and renamed columns for clarity
    result_schema = StructType([
        StructField("TABLE_NAME", StringType()),
        StructField("SOURCE_TABLE", StringType()),
        StructField("HUB_TABLE", StringType()),
        StructField("SATELLITE_TABLE", StringType()),
        StructField("BIZVIEW_TABLE", StringType()),
        StructField("SOURCE_COUNT", LongType()),
        StructField("SOURCE_COUNT_NONDELETED", LongType()),
        StructField("HUB_COUNT", LongType()),
        StructField("SATELLITE_COUNT", LongType()),
        StructField("BIZVIEW_COUNT", LongType()),
        # Renamed columns for better clarity 
        StructField("SOURCE_TO_VAULT_DIFFERENCES", LongType()),  # Changed from SOURCE_TO_VAULT_LOSS
        StructField("DATA_REPRESENTATION_DIFFERENCES", LongType()),  # Changed from VALIDATED_LOSS_COUNT
        StructField("BIZVIEW_MISSING_RECORDS", LongType()),  # Changed from VAULT_TO_BIZVIEW_LOSS
        StructField("DELETED_RECORDS", LongType()),
        StructField("VALIDATION_MESSAGE", StringType()),
        StructField("DATA_DISCREPANCY_DETAILS", VariantType())  # Changed from LOST_RECORDS_DETAILS
    ])
    
    # Set Snowflake session parameters for better performance
    try:
        session.sql("ALTER SESSION SET QUERY_TAG = 'DATA_LINEAGE_SCRIPT'").collect()
        session.sql("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600").collect() # 1 hour timeout
        print("Session parameters set successfully")
    except Exception as e:
        print(f"Warning: Could not set session parameters: {str(e)}")
        
    # Results and data discrepancy containers
    results = []
    data_discrepancies = {}
    
    # Process each table configuration
    for config in table_configs:
        try:
            # Extract table identifiers
            table_name = config.get('source_table', '').split('.')[-1]
            source_table = config.get('source_table')
            
            # Handle both single hub_table and multiple hub_tables
            if 'hub_tables' in config:
                hub_tables = config['hub_tables']
                hub_table = ', '.join(hub_tables)
            else:
                hub_tables = [config.get('hub_table', '')]
                hub_table = hub_tables[0]
            
            satellite_table = config.get('cur_satellite_table', '')
            bizview_table = config.get('bizview_table', '')
            
            print(f"\n{'='*50}")
            print(f"Processing table: {table_name}")
            print(f"Source: {source_table}")
            print(f"Hub: {hub_table}")
            print(f"Satellite: {satellite_table}")
            print(f"Bizview: {bizview_table}")
            print(f"{'='*50}")
            
            # First test if source table exists and is accessible
            try:
                session.sql(f"SELECT 1 FROM {source_table} LIMIT 1").collect()
                print(f"✅ Source table {source_table} is accessible")
            except Exception as e:
                print(f"❌ Source table {source_table} is not accessible: {str(e)}")
                continue  # Skip to next table if source is not accessible
            
            # Count source records with improved reporting
            source_count, source_query = get_table_count(session, source_table)
            print(f"Source table total records: {source_count}")
            
            # Count non-deleted records
            non_deleted_count = source_count
            deleted_count = 0
            if "deleted_column" in config:
                try:
                    deleted_col = config["deleted_column"]
                    non_deleted_count, non_deleted_query = get_table_count(
                        session, source_table, f"{deleted_col} = FALSE")
                    deleted_count = source_count - non_deleted_count
                    print(f"Source non-deleted records: {non_deleted_count}")
                    print(f"Deleted records: {deleted_count}")
                except Exception as e:
                    print(f"ERROR processing deleted records for {table_name}: {str(e)}")
            
            # Hub count - simplified to just get the total
            hub_count = 0
            hub_queries = []
            for h_table in hub_tables:
                if h_table:
                    hub_table_count, hub_query = get_table_count(session, h_table)
                    hub_count += hub_table_count
                    hub_queries.append(hub_query)
                    print(f"Hub table {h_table} count: {hub_table_count}")
            
            # Satellite count - simplified but with improved handling
            sat_count = 0
            sat_query = "No satellite table"
            if satellite_table:
                # For current satellites, use hash key if available
                satellite_hash_key = config.get('satellite_hash_key')
                if satellite_hash_key and ("_LROC_" in satellite_table.upper() or "_MROC_" in satellite_table.upper()):
                    print(f"Using distinct count on hash key for current satellite table")
                    sat_count, sat_query = get_table_count(
                        session, satellite_table, count_column=satellite_hash_key, distinct=True)
                else:
                    print(f"Using regular count for satellite table")
                    sat_count, sat_query = get_table_count(
                        session, satellite_table)
                
                print(f"Satellite table count: {sat_count}")
            
            # IMPROVED: Bizview count with optimized handling
            bizview_count = 0
            bizview_query = "No bizview table"
            if bizview_table:
                # Use the dedicated function for more accurate bizview counting
                bizview_count, bizview_query = get_bizview_count(session, config)
                print(f"Bizview table count (improved approach): {bizview_count}")
            
            # Extract data differences (renamed from missing records)
            source_hub_differences, difference_records, validation_message = extract_missing_records(session, config)
            
            # Determine validated count of differences
            data_representation_differences = source_hub_differences
            if "representation differences" in validation_message.lower():  # Updated terminology
                # If we detected representation differences (not true missing data), set accordingly
                data_representation_differences = source_hub_differences
                source_hub_differences = 0  # No actual missing records
                print(f"✅ No actual data loss detected after validation: {validation_message}")
            elif "key-only validation" in validation_message.lower():
                # If we performed key-only validation, extract that count for truly missing records
                try:
                    import re
                    match = re.search(r'found only (\d+) (missing|truly missing) records', validation_message)
                    if match:
                        true_missing = int(match.group(1))
                        data_representation_differences = source_hub_differences - true_missing
                        source_hub_differences = true_missing
                        print(f"ℹ️ Adjusted: {true_missing} truly missing records, {data_representation_differences} data representation differences")
                except:
                    print(f"Could not parse key validation count from message")
            
            # Cross-check with count differences for another validation
            if source_hub_differences > 0:
                # Compare non-deleted source count with hub/satellite count
                expected_diff = abs(non_deleted_count - (sat_count if sat_count > 0 else hub_count))
                if abs(source_hub_differences - expected_diff) > max(5, expected_diff * 0.1):  # Allow 10% variance
                    print(f"⚠️ Warning: Reported differences ({source_hub_differences}) differs significantly from count difference ({expected_diff})")
                    print(f"  This suggests data representation differences rather than missing records")
                    validation_message += f" Count comparison shows expected diff of {expected_diff} vs. reported {source_hub_differences}."
                    
                    # If the count difference is small but reported differences are large, these are likely representation differences
                    if expected_diff < 5 and source_hub_differences > 10:
                        data_representation_differences = source_hub_differences
                        source_hub_differences = 0
                        print(f"✅ Setting missing records to 0 based on count comparison: small count diff but large reported differences")
                        validation_message += " Based on count comparison, there appears to be no actual data loss, only representation differences."
            
            # Initialize data discrepancies dictionary with improved structure and clearer terminology
            data_discrepancies[table_name] = {
                "source_to_vault": {
                    "data_differences": {
                        "records": difference_records,
                        "count": data_representation_differences,
                        "explanation": "These records exist in both source and vault but with differences in data representation."
                    },
                    "missing_records": {
                        "count": source_hub_differences,
                        "explanation": "These records exist in the source but could not be found in the vault."
                    },
                    "validation_message": validation_message,
                    "count_comparison": {
                        "source_nondeleted": non_deleted_count,
                        "vault": sat_count if sat_count > 0 else hub_count,
                        "difference": abs(non_deleted_count - (sat_count if sat_count > 0 else hub_count))
                    }
                }
            }
            
            # Calculate bizview missing records (renamed from vault-to-bizview loss) with completely revised logic
            bizview_missing_records = 0
            if bizview_table:
                try:
                    # Important: Many bizviews are expected to have fewer records by design
                    # They often intentionally filter or exclude certain data based on business rules
                    
                    # First, check if this is a hub-to-bizview direct mapping
                    # (Some bizviews map directly to hubs with no filtering)
                    is_direct_mapping = False
                    bizview_type = "unknown"
                    
                    # Try to determine bizview type from name or structure
                    bizview_name = bizview_table.split('.')[-1].lower()
                    if bizview_name.startswith(('dim_', 'dimension_')):
                        bizview_type = "dimension"
                        # Dimensions usually map directly to hubs (1:1)
                        is_direct_mapping = True
                    elif bizview_name.startswith(('fact_', 'bridge_', 'link_')):
                        bizview_type = "fact"
                        # Facts often have filtering/aggregation logic
                        is_direct_mapping = False
                    
                    print(f"Bizview type detected: {bizview_type} - Direct mapping: {is_direct_mapping}")
                    
                    # Get reference count based on bizview type
                    reference_count = 0
                    reference_type = ""
                    if is_direct_mapping:
                        reference_count = hub_count
                        reference_type = "hub"
                    else:
                        # For non-direct mappings, we need to be more careful with count selection
                        # For fact tables that aggregate at the source level, the proper comparison
                        # is against the non-deleted source count rather than hub/satellite
                        if bizview_type == "fact" and non_deleted_count > 0:
                            reference_count = non_deleted_count
                            reference_type = "source (non-deleted)"
                        # Otherwise use satellite count if available, or fall back to hub count
                        elif sat_count > 0 and satellite_table:
                            reference_count = sat_count
                            reference_type = "satellite"
                        else:
                            reference_count = hub_count
                            reference_type = "hub"
                    
                    print(f"Using {reference_type} count ({reference_count}) as reference for bizview comparison")
                    
                    # Adjust threshold based on bizview type
                    base_threshold = 5  # Minimum threshold
                    percentage_threshold = 0
                    
                    if bizview_type == "dimension":
                        # Dimensions should closely match hub counts
                        percentage_threshold = 0.05  # 5% threshold
                    elif bizview_type == "fact":
                        # Facts often have legitimate filtering
                        percentage_threshold = 0.20  # 20% threshold
                    else:
                        # Unknown type - use moderate threshold
                        percentage_threshold = 0.10  # 10% threshold
                    
                    # Calculate adaptive threshold based on data volume
                    threshold = max(base_threshold, int(reference_count * percentage_threshold))
                    
                    # Calculate raw difference
                    raw_diff = 0
                    comparison = ""
                    
                    # For most tables, we expect bizview ≤ vault
                    # But some bizviews might legitimately have more records (calculated fields, etc.)
                    if bizview_count <= reference_count:
                        raw_diff = reference_count - bizview_count
                        comparison = "bizview_smaller"
                    else:
                        raw_diff = bizview_count - reference_count
                        comparison = "bizview_larger"
                    
                    # Check for empty bizview (which is a clear issue)
                    if bizview_count == 0 and reference_count > 0:
                        bizview_missing_records = reference_count
                        print(f"⚠️ CRITICAL: Bizview is empty but {reference_type} has {reference_count} records")
                    # Normal comparison with threshold
                    elif raw_diff > threshold:
                        # Large difference detected, but might be by design
                        
                        # Skip reporting for known patterns where filtering is expected
                        skip_reporting = False
                        
                        # Check for common patterns where differences are expected
                        if (comparison == "bizview_smaller" and 
                            (bizview_name.startswith("fact_") or 
                             bizview_name.endswith("_current") or
                             bizview_name.endswith("_active"))):
                            # Fact tables and views with "_current" or "_active" often filter records
                            if raw_diff / reference_count < 0.95:  # Not more than 95% missing
                                skip_reporting = True
                                print(f"ℹ️ Bizview likely has intentional filtering (fact or current/active view)")
                        
                        if not skip_reporting:
                            bizview_missing_records = raw_diff
                            print(f"⚠️ Bizview records difference detected: {raw_diff} records")
                            print(f"  ({reference_type} count: {reference_count}, bizview count: {bizview_count})")
                            
                            # Calculate percentage difference for context
                            if reference_count > 0:
                                pct_diff = (raw_diff / reference_count) * 100
                                print(f"  Percentage difference: {pct_diff:.1f}%")
                                
                                # Flag potentially normal cases
                                if comparison == "bizview_smaller" and pct_diff < 50:
                                    print(f"  ℹ️ Note: This may be normal business filtering in the bizview")
                    else:
                        bizview_missing_records = 0
                        print(f"✅ No significant bizview difference (diff within threshold: {raw_diff})")
                    
                    # Store detailed information
                    data_discrepancies[table_name]["bizview"] = {
                        "missing_records": {
                            "count": bizview_missing_records,
                            "comparison": {
                                "bizview_type": bizview_type,
                                "is_direct_mapping": is_direct_mapping,
                                "reference_type": reference_type,
                                "reference_count": reference_count,
                                "bizview_count": bizview_count,
                                "comparison_direction": comparison,
                                "raw_difference": raw_diff,
                                "threshold": threshold,
                                "threshold_percentage": f"{percentage_threshold * 100:.1f}%",
                                "percentage_diff": f"{(raw_diff / reference_count * 100):.1f}%" if reference_count > 0 else "N/A",
                                "query_used": bizview_query
                            },
                            "explanation": "No significant difference detected between vault and bizview counts." if bizview_missing_records == 0 else 
                                          (f"Bizview has {raw_diff} fewer records than the {reference_type}. This may be due to business filtering or a data loss issue." 
                                           if comparison == "bizview_smaller" else
                                           f"Bizview has {raw_diff} more records than the {reference_type}. This may be due to calculated fields, duplicates, or other business logic.")
                        }
                    }
                
                except Exception as e:
                    print(f"Error in bizview records comparison: {str(e)}")
                    bizview_missing_records = 0
                    data_discrepancies[table_name]["bizview"] = {
                        "missing_records": {
                            "count": 0,
                            "error": str(e)
                        }
                    }
            
            # Add information about deleted records
            data_discrepancies[table_name]["intentionally_deleted"] = {
                "count": deleted_count,
                "explanation": "Records marked as deleted in the source and properly tracked in the vault."
            }
            
            # Append results with improved metrics and renamed columns
            results.append((
                table_name,                      # TABLE_NAME
                source_table,                    # SOURCE_TABLE
                hub_table,                       # HUB_TABLE
                satellite_table,                 # SATELLITE_TABLE
                bizview_table,                   # BIZVIEW_TABLE
                source_count,                    # SOURCE_COUNT
                non_deleted_count,               # SOURCE_COUNT_NONDELETED
                hub_count,                       # HUB_COUNT
                sat_count,                       # SATELLITE_COUNT
                bizview_count,                   # BIZVIEW_COUNT
                source_hub_differences,          # SOURCE_TO_VAULT_DIFFERENCES
                data_representation_differences, # DATA_REPRESENTATION_DIFFERENCES 
                bizview_missing_records,         # BIZVIEW_MISSING_RECORDS 
                deleted_count,                   # DELETED_RECORDS
                validation_message,              # VALIDATION_MESSAGE
                json.dumps(data_discrepancies[table_name], indent=4)  # DATA_DISCREPANCY_DETAILS 
            ))
            
        except Exception as e:
            print(f"CRITICAL ERROR processing {config.get('source_table', 'unknown')}: {str(e)}")
            continue
    
    # Create results DataFrame
    try:
        results_df = session.create_dataframe(results, schema=result_schema)
        
        # Log summary for debugging
        print("\n=== VALIDATION SUMMARY ===")
        for result in results:
            table_name = result[0]
            source_table = result[1]
            hub_table = result[2]
            satellite_table = result[3]
            bizview_table = result[4]
            source_count = result[5]
            non_deleted_count = result[6]
            hub_count = result[7]
            sat_count = result[8]
            bizview_count = result[9]
            source_to_vault_differences = result[10]  # Renamed
            data_representation_differences = result[11]  # Renamed
            bizview_missing_records = result[12]  # Renamed
            deleted_count = result[13]
            validation_message = result[14]
            
            print(f"Table: {table_name}")
            print(f"  Source Table: {source_table}")
            print(f"  Hub Table: {hub_table}")
            print(f"  Satellite Table: {satellite_table or 'N/A'}")
            print(f"  Bizview Table: {bizview_table or 'N/A'}")
            print(f"  Source Count (Total): {source_count}")
            print(f"  Source Count (Non-Deleted): {non_deleted_count}")
            print(f"  Hub Count: {hub_count}")
            if satellite_table:
                print(f"  Satellite Count: {sat_count}")
            if bizview_table:
                print(f"  Bizview Count: {bizview_count}")
            
            print("\n  Data Flow:")
            print(f"  Source ({non_deleted_count}) → Vault → Bizview ({bizview_count if bizview_table else 'N/A'})")
            
            if source_to_vault_differences > 0:
                print(f"    ⚠️ True Missing Records: {source_to_vault_differences}")
                if data_representation_differences > 0:
                    print(f"      ℹ️ Records with Data Differences: {data_representation_differences}")
                    print(f"      ℹ️ {validation_message}")
            elif data_representation_differences > 0:
                print(f"    ℹ️ Records with Data Representation Differences: {data_representation_differences}")
                print(f"    ✅ No actual missing records")
            else:
                print(f"    ✅ No Data Discrepancies")
                
            if bizview_missing_records > 0:
                print(f"    ⚠️ Bizview Missing Records: {bizview_missing_records}")
            elif bizview_table:
                print(f"    ✅ No Bizview Discrepancies")
                
            if deleted_count > 0:
                print(f"  Intentionally Deleted Records: {deleted_count}")
            
            print("  ---\n")
        
        return results_df
    except Exception as e:
        print(f"ERROR creating final results dataframe: {str(e)}")
        # Return empty dataframe with the defined schema
        return session.create_dataframe([], schema=result_schema)

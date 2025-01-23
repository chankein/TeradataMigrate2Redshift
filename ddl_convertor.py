import re
import argparse

def read_ddl_from_file(file_path):
    """
    Read a Teradata DDL file.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()

def write_ddl_to_file(file_path, ddl):
    """
    Write the Redshift DDL to a file.
    """
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(ddl)

def convert_teradata_to_redshift(ddl, row_count):
    """
    Convert Teradata DDL to Redshift DDL.
    """
    # Extract the table name and convert it to lowercase
    # CREATE TABLE, CREATE MULTISET TABLE, CREATE SET TABLE
    table_name_match = re.search(r"CREATE (?:MULTISET |SET )?TABLE (\w+\.\w+)", ddl, re.IGNORECASE)
    if table_name_match:
        schema_table = table_name_match.group(1).split('.')
        schema_name = schema_table[0].lower()  # Convert schema name to lowercase
        table_name = schema_table[1].lower()   # Convert table name to lowercase
    else:
        raise ValueError("Table name not found in DDL")

    # Replace schema name with a bash variable
    redshift_table_name = f"{schema_name}.{table_name}"

    # Identify the column definition section
    column_section_match = re.search(r"\(\s*([\s\S]+?)\s*\)\s*(?:PRIMARY|UNIQUE|WITH|NO)", ddl, re.IGNORECASE)
    if column_section_match:
        column_section = column_section_match.group(1)
    else:
        raise ValueError("Column definitions not found in DDL")

    # Extract column definitions
    column_pattern = re.compile(r"\s*(\w+)\s+([\w\(\)]+)\s*(CHARACTER SET \w+)?\s*(NOT CASESPECIFIC)?\s*(DEFAULT [^,]+)?\s*(COMPRESS)?\s*(NOT NULL)?", re.IGNORECASE)
    columns = column_pattern.findall(column_section)

    # Specific column conversion rules (highest priority)
    # Condition: Apply the following rules to specific columns first
    specific_columns = {
        "LOG_DATE": ("log_date", "DATE"),  # log_date should be DATE. Do not change character encoding.
        "SUMMARY_DATE": ("summary_date", "DATE"),  # summary_date should be DATE. Do not change character encoding. Recommended change.
        "GUID": ("guid", "CHAR(26)"),  # guid should be CHAR(26). Do not change character encoding.
        "STORE_ID": ("seller_id", "VARCHAR(30)"),  # store_id should be renamed to seller_id and set as VARCHAR(30). Do not change character encoding.
        "SELLER_ID": ("seller_id", "VARCHAR(30)"),  # seller_id should be VARCHAR(30). Do not change character encoding.
        "Y_ID_HEX": ("y_id_hex", "VARCHAR(18)"),  # y_id_hex should be VARCHAR(18). Do not change character encoding.
        "ORDER_ID": ("order_id", "VARCHAR(50)"),  # order_id should be VARCHAR(50). Do not change character encoding.
        "ORDER_DATE": ("order_date", "DATE"),  # order_date should be DATE. Do not change character encoding.
        "ITEM_CODE": ("srid", "VARCHAR(99)"),  # item_code should be renamed to srid and set as VARCHAR(99). Do not change character encoding.
        "LOG_MONTH": ("log_month", "INTEGER"),  # log_month should be INTEGER. Do not change character encoding.
        "HASH_ID_HEX": ("hash_id_hex", "VARCHAR(18)"),  # hash_id_hex should be VARCHAR(18). Do not change character encoding.
        "REPORT_DATE": ("report_date", "DATE"),  # report_date should be DATE. Do not change character encoding. Recommended change.
        "CAMPAIGN_ID": ("campaign_id", "INTEGER"),  # campaign_id should be INTEGER. Do not change character encoding.
        "PRODUCT_CATEGORY_ID": ("product_category_id", "INTEGER"),  # product_category_id should be INTEGER. Do not change character encoding.
        "SUMMARY_MONTH": ("summary_month", "INTEGER"),  # summary_month should be INTEGER. Do not change character encoding. Recommended change.
        "COUPON_ID": ("coupon_id", "VARCHAR(64)"),  # coupon_id should be VARCHAR(64). Do not change character encoding.
        "VIEW_ID": ("view_id", "BIGINT"),  # view_id should be BIGINT. Do not change character encoding.
        "MK_CAMPAIGN_ID": ("mk_campaign_id", "VARCHAR(16)"),  # mk_campaign_id should be VARCHAR(16). Do not change character encoding.
        "MONTH_ID": ("month_id", "INTEGER"),  # month_id should be INTEGER. Do not change character encoding. Recommended change.
        "LIST_ID": ("list_id", "INTEGER"),  # list_id should be INTEGER. Do not change character encoding. Prohibited.
        "EVENT_ID": ("event_id", "INTEGER"),  # event_id should be INTEGER. Do not change character encoding. Recommended change.
        "STORE_ACCOUNT": ("seller_id", "VARCHAR(30)"),  # store_account should be renamed to seller_id and set as VARCHAR(30). Do not change character encoding.
        "CREATE_DATE": ("create_date", "DATE"),  # create_date should be DATE. Do not change character encoding.
        "CATALOG_ID": ("catalog_id", "VARCHAR(10)"),  # catalog_id should be VARCHAR(10). Do not change character encoding.
        "SERV_DATE": ("serv_date", "DATE"),  # serv_date should be DATE. Do not change character encoding. Recommended change.
        "GENRE_CATEGORY_ID": ("genre_category_id", "INTEGER"),  # genre_category_id should be INTEGER. Do not change character encoding.
        "JAN_CODE": ("jan_code", "VARCHAR(13)"),  # jan_code should be VARCHAR(13). Do not change character encoding.
        "GIFT_CARD_ID": ("gift_card_id", "VARCHAR(20)"),  # gift_card_id should be VARCHAR(20). Do not change character encoding.
        "SAPP_ID": ("sapp_id", "VARCHAR(20)"),  # sapp_id should be VARCHAR(20). Do not change character encoding.
        "SELL_ID": ("sell_id", "VARCHAR(8)"),  # sell_id should be VARCHAR(8). Do not change character encoding.
        "SETTLE_ID": ("settle_id", "VARCHAR(7)"),  # settle_id should be VARCHAR(7). Do not change character encoding.
        "SELLER_EVENT_ID": ("seller_event_id", "INTEGER"),  # seller_event_id should be INTEGER. Do not change character encoding.
        "ARTICLE_ID": ("article_id", "DECIMAL(20, 0)"),  # article_id should be DECIMAL(20, 0). Do not change character encoding.
        "HASH_ID": ("hash_id", "VARCHAR(104)"),  # hash_id should be VARCHAR(104). Do not change character encoding. Prohibited.
        "PROMOTION_ID": ("promotion_id", "VARCHAR(10)"),  # promotion_id should be VARCHAR(10). Do not change character encoding.
        "TIME_SALE_COUPON_CAMPAIGN_ID": ("time_sale_coupon_campaign_id", "VARCHAR(10)"),  # time_sale_coupon_campaign_id should be VARCHAR(10). Do not change character encoding.
        "BASKET_ID": ("basket_id", "VARCHAR(36)"),  # basket_id should be VARCHAR(36). Do not change character encoding.
        "SRID": ("srid", "VARCHAR(99)"),  # srid should be VARCHAR(99). Do not change character encoding.
        "SELLER_MANAGED_ITEM_ID": ("seller_managed_item_id", "VARCHAR(99)"),  # seller_managed_item_id should be VARCHAR(99). Do not change character encoding.
        "YSRID": ("ysrid", "VARCHAR(130)"),  # ysrid should be VARCHAR(130). Do not change character encoding.
        "YSRID_LIST": ("ysrid_list", "VARCHAR(130)"),  # ysrid_list should be VARCHAR(130). Do not change character encoding. Prohibited.
        "SKUID": ("skuid", "VARCHAR(99)"),  # skuid should be VARCHAR(99). Do not change character encoding.
        "SKU_EDIT_ID": ("sku_edit_id", "VARCHAR(99)"),  # sku_edit_id should be VARCHAR(99). Do not change character encoding. Prohibited.
        "BRAND_ID": ("brand_id", "INTEGER"),  # brand_id should be INTEGER. Do not change character encoding.
        "SPEC_ID": ("spec_id", "INTEGER"),  # spec_id should be INTEGER. Do not change character encoding.
        "SPEC_VALUE_ID": ("spec_value_id", "INTEGER"),  # spec_value_id should be INTEGER. Do not change character encoding.
        "SET_ID": ("set_id", "INTEGER"),  # set_id should be INTEGER. Do not change character encoding.
        "SP_CODE": ("sp_code", "VARCHAR(54)"),  # sp_code should be VARCHAR(54). Do not change character encoding.
        "POINT_CODE": ("point_code", "VARCHAR(10)"),  # point_code should be VARCHAR(10). Do not change character encoding.
        "OPTION_ID": ("option_id", "VARCHAR(16)"),  # option_id should be VARCHAR(16). Do not change character encoding.
        "OPTION_CHOICE_ID": ("option_choice_id", "VARCHAR(16)"),  # option_choice_id should be VARCHAR(16). Do not change character encoding.
        "ORDER_ID": ("order_id", "VARCHAR(50)"),  # order_id should be VARCHAR(50). Do not change character encoding.
        "ACCESS_SECONDS": ("access_seconds", "BIGINT"),  # *_access_seconds columns defined as INTEGER should be BIGINT.
        "MAIL_HEX": ("mail_hex", "VARCHAR(20)"),  # mail_hex of BYTE(N) type should be VARCHAR(N*2) (N is an integer).
    }

    # Count of [Changed], [Recommended Change], and [Prohibited] columns
    changed_columns = []
    recommended_columns = []
    prohibited_columns = []

    # Column conversion rules
    def convert_column(column_def):
        column_name = column_def[0].strip().upper()
        column_type = column_def[1].strip().upper()
        character_set = column_def[2].strip() if column_def[2] else ''
        not_case_specific = column_def[3].strip() if column_def[3] else ''
        not_null = column_def[4].strip() if column_def[4] else ''

        # Apply specific column conversion rules first
        if column_name in specific_columns:
            new_name, new_type = specific_columns[column_name]
            changed_columns.append((column_name, new_name))
            return f"{new_name} {new_type} {not_null}"
        # *_utc columns defined as INTEGER should be BIGINT
        if column_name.endswith("_UTC") and column_type == "INTEGER":
            return f"{column_name.lower()} BYTEINT {not_null}"
        
        # General column conversion rules
        # Condition: For columns not covered by specific rules, apply the following rules
        if column_type.startswith("BYTE") or column_type.startswith("VARBYTE"):
            # BYTE(N) or VARBYTE(N) should be VARCHAR(N*2) (N is an integer)
            byte_length = int(re.search(r"\d+", column_type).group())
            new_type = f"VARCHAR({byte_length * 2})"
        elif column_type == "BYTEINT":
            # BYTEINT should be SMALLINT
            new_type = "SMALLINT"
        elif column_type in ["SMALLINT", "INTEGER", "BIGINT"]:
            # SMALLINT, INTEGER, or BIGINT types should remain unchanged
            new_type = column_type
        elif column_type == "FLOAT":
            # FLOAT should be DOUBLE PRECISION
            new_type = "DOUBLE PRECISION"
        elif column_type.startswith("DECIMAL"):
            # DECIMAL types should remain unchanged
            new_type = column_type
        elif column_type == "TIMESTAMP":
            # TIMESTAMP should be TIMESTAMP WITHOUT TIME ZONE
            new_type = "TIMESTAMP WITHOUT TIME ZONE"
        elif column_type == "DATE":
            # DATE types should remain unchanged
            new_type = "DATE"
        elif column_type.startswith("VARCHAR") or column_type.startswith("CHAR"):
            if "UNICODE" in character_set:
                # UNICODE VARCHAR(N) or CHAR(N) should be VARCHAR(N*3) (N is an integer)
                char_length = int(re.search(r"\d+", column_type).group())
                new_type = f"VARCHAR({char_length * 3})"
            else:
                # LATIN VARCHAR(N) or CHAR(N) should remain VARCHAR(N) (N is an integer)
                new_type = column_type
        else:
            new_type = f"VARCHAR(255)"

        # Convert _flg to _flag
        # Condition: _flg should be renamed to _flag
        if column_name.endswith("_FLG"):
            new_name = column_name[:-4] + "_flag"
            return f"{new_name} VARCHAR(1)"
        else:
            return f"{column_name.lower()} {new_type}"

    # Generate Redshift DDL
    redshift_ddl = f"CREATE TABLE {redshift_table_name} (\n"
    redshift_ddl += ",\n".join([convert_column(col) for col in columns])
    redshift_ddl += "\n)\n"

    # Set DISTKEY and SORTKEY
    # Condition: The leftmost column specified in Primary Index or Unique Primary Index but not in Primary Partition Index should be DISTKEY
    primary_index_match = re.search(r"PRIMARY INDEX\s*\(([^)]+)\)", ddl, re.IGNORECASE)
    unique_primary_index_match = re.search(r"UNIQUE PRIMARY INDEX\s*\(([^)]+)\)", ddl, re.IGNORECASE)
    primary_partition_index_match = re.search(r"PRIMARY PARTITION INDEX\s*\(([^)]+)\)", ddl, re.IGNORECASE)

    primary_index = []
    if primary_index_match:
        primary_index = primary_index_match.group(1).split(",")
    if unique_primary_index_match:
        primary_index += unique_primary_index_match.group(1).split(",")

    primary_partition_index = primary_partition_index_match.group(1).split(",") if primary_partition_index_match else []

    # Set DISTKEY
    if primary_index:
        distkey = primary_index[0].strip().lower()
        redshift_ddl += f"DISTKEY({distkey})\n"
    else:
        distkey = ''

    # Condition: Columns specified in Primary Partition Index and Primary Index or Unique Primary Index should be SORTKEY. Use SORTKEY for single columns and COMPOUND SORTKEY for multiple columns.
    sortkeys = primary_partition_index + primary_index
    if sortkeys:
        if len(sortkeys) == 1:
            redshift_ddl += f"SORTKEY({sortkeys[0].strip().lower()})\n"
        else:
            redshift_ddl += f"COMPOUND SORTKEY({', '.join([x.strip().lower() for x in sortkeys])})\n"
    else:
        sortkeys = []

    # Set distribution style based on row count
    # Condition: If the table has fewer than 3 million rows (or no row count is specified), use ALL distribution. For 3 million or more rows, use KEY distribution.
    if row_count < 3000000:
        redshift_ddl += "DISTSTYLE ALL\n"
    else:
        redshift_ddl += "DISTSTYLE KEY\n"

    # Condition: Compress the table using ENCODE AUTO
    redshift_ddl += "ENCODE AUTO;"

    # Output statistics
    # Condition: If there are [Changed], [Recommended Change], or [Prohibited] columns, report how many and what changes were made.
    print(f"Number of changed columns: {len(changed_columns)}")
    print(f"Number of recommended changes: {len(recommended_columns)}")
    print(f"Number of prohibited columns: {len(prohibited_columns)}")
    print(f"Changes made: {changed_columns}")

    return redshift_ddl

if __name__ == "__main__":
    # Set up command-line arguments
    # Condition: Ask for the table row count and the DDL to convert in order.
    parser = argparse.ArgumentParser(description="Convert Teradata DDL to Redshift DDL")
    parser.add_argument("-i", "--input", required=True, help="Path to the input Teradata DDL file")
    parser.add_argument("-o", "--output", required=True, help="Path to the output Redshift DDL file")
    parser.add_argument("-r", "--row_count", type=int, required=True, help="Number of rows in the table")
    args = parser.parse_args()

    # Read the DDL file and convert it
    teradata_ddl = read_ddl_from_file(args.input)
    redshift_ddl = convert_teradata_to_redshift(teradata_ddl, args.row_count)
    write_ddl_to_file(args.output, redshift_ddl)
    print(f"Redshift DDL has been written to {args.output}.")

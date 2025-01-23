---

# Teradata to Redshift DDL Converter  
# Teradata 到 Redshift DDL 转换工具  

This script converts Teradata DDL (Data Definition Language) to Redshift DDL. It handles table definitions, column types, distribution keys, sort keys, and other Redshift-specific configurations.  
该脚本用于将 Teradata 的 DDL（数据定义语言）转换为 Redshift 的 DDL。它可以处理表定义、列类型、分布键、排序键以及其他 Redshift 特定的配置。

---

## Features / 功能  
- Converts Teradata table definitions to Redshift-compatible DDL.  
  将 Teradata 表定义转换为 Redshift 兼容的 DDL。  
- Handles specific column type conversions (e.g., `BYTE` to `VARCHAR`, `TIMESTAMP` to `TIMESTAMP WITHOUT TIME ZONE`).  
  处理特定的列类型转换（例如，`BYTE` 转换为 `VARCHAR`，`TIMESTAMP` 转换为 `TIMESTAMP WITHOUT TIME ZONE`）。  
- Automatically sets `DISTKEY` and `SORTKEY` based on Teradata's primary and partition indexes.  
  根据 Teradata 的主键和分区索引自动设置 `DISTKEY` 和 `SORTKEY`。  
- Supports row count-based distribution style (`ALL` for < 3M rows, `KEY` for >= 3M rows).  
  支持基于行数的分布方式（小于 300 万行使用 `ALL`，大于等于 300 万行使用 `KEY`）。  
- Outputs statistics on changed, recommended, and prohibited columns.  
  输出关于已更改、推荐更改和禁止使用的列的统计信息。  

---

## Usage / 使用方法  

### Prerequisites / 前提条件  
- Python 3.x  
- Input Teradata DDL file  

### Command / 命令  
Run the script with the following command:  
使用以下命令运行脚本：  

```bash
python convert_ddl.py -i <input_file> -o <output_file> -r <row_count>
```

### Arguments / 参数  
- `-i` or `--input`: Path to the input Teradata DDL file.  
  `-i` 或 `--input`：输入 Teradata DDL 文件的路径。  
- `-o` or `--output`: Path to the output Redshift DDL file.  
  `-o` 或 `--output`：输出 Redshift DDL 文件的路径。  
- `-r` or `--row_count`: Number of rows in the table (used to determine distribution style).  
  `-r` 或 `--row_count`：表的行数（用于确定分布方式）。  

### Example / 示例  
```bash
python convert_ddl.py -i teradata_ddl.sql -o redshift_ddl.sql -r 5000000
```

---

## Conversion Rules / 转换规则  

### Column Type Conversion / 列类型转换  
| Teradata Type       | Redshift Type               | Notes / 备注                                                                 |
|---------------------|-----------------------------|------------------------------------------------------------------------------|
| `BYTE(N)`           | `VARCHAR(N*2)`              | Converts byte length to character length. / 将字节长度转换为字符长度。       |
| `VARBYTE(N)`        | `VARCHAR(N*2)`              | Converts byte length to character length. / 将字节长度转换为字符长度。       |
| `BYTEINT`           | `SMALLINT`                  | Converts to a larger integer type. / 转换为更大的整数类型。                  |
| `INTEGER`           | `INTEGER`                   | No change. / 保持不变。                                                     |
| `BIGINT`            | `BIGINT`                    | No change. / 保持不变。                                                     |
| `FLOAT`             | `DOUBLE PRECISION`          | Converts to double precision. / 转换为双精度浮点数。                         |
| `DECIMAL`           | `DECIMAL`                   | No change. / 保持不变。                                                     |
| `TIMESTAMP`         | `TIMESTAMP WITHOUT TIME ZONE` | Converts to timestamp without time zone. / 转换为不带时区的时间戳。          |
| `DATE`              | `DATE`                      | No change. / 保持不变。                                                     |
| `VARCHAR(N)`        | `VARCHAR(N)` or `VARCHAR(N*3)` | If `UNICODE`, multiplies length by 3. / 如果是 `UNICODE`，长度乘以 3。       |
| `CHAR(N)`           | `CHAR(N)` or `VARCHAR(N*3)` | If `UNICODE`, multiplies length by 3. / 如果是 `UNICODE`，长度乘以 3。       |
| `*_UTC` (INTEGER)   | `BIGINT`                    | Converts to `BIGINT`. / 转换为 `BIGINT`。                                   |
| `*_FLG`             | `*_flag VARCHAR(1)`         | Renames `_flg` to `_flag`. / 将 `_flg` 重命名为 `_flag`。                   |

### Specific Column Rules / 特定列规则  
- Certain columns (e.g., `LOG_DATE`, `GUID`, `STORE_ID`) have predefined conversion rules.  
  某些列（例如 `LOG_DATE`、`GUID`、`STORE_ID`）有预定义的转换规则。  
- Columns ending with `_UTC` and defined as `INTEGER` are converted to `BIGINT`.  
  以 `_UTC` 结尾且定义为 `INTEGER` 的列将转换为 `BIGINT`。  
- Columns ending with `_FLG` are renamed to `_flag` and set as `VARCHAR(1)`.  
  以 `_FLG` 结尾的列将重命名为 `_flag` 并设置为 `VARCHAR(1)`。  

### Distribution and Sort Keys / 分布键和排序键  
- The leftmost column in the `PRIMARY INDEX` or `UNIQUE PRIMARY INDEX` (not in `PRIMARY PARTITION INDEX`) is set as `DISTKEY`.  
  `PRIMARY INDEX` 或 `UNIQUE PRIMARY INDEX` 中最左边的列（不在 `PRIMARY PARTITION INDEX` 中）将设置为 `DISTKEY`。  
- Columns in `PRIMARY PARTITION INDEX` and `PRIMARY INDEX` or `UNIQUE PRIMARY INDEX` are set as `SORTKEY`.  
  `PRIMARY PARTITION INDEX` 和 `PRIMARY INDEX` 或 `UNIQUE PRIMARY INDEX` 中的列将设置为 `SORTKEY`。  
  - Single column: `SORTKEY(<column>)`  
    单列：`SORTKEY(<column>)`  
  - Multiple columns: `COMPOUND SORTKEY(<column1>, <column2>, ...)`  
    多列：`COMPOUND SORTKEY(<column1>, <column2>, ...)`  

### Distribution Style / 分布方式  
- If the table has fewer than 3 million rows, `DISTSTYLE ALL` is used.  
  如果表的行数少于 300 万，则使用 `DISTSTYLE ALL`。  
- If the table has 3 million or more rows, `DISTSTYLE KEY` is used.  
  如果表的行数大于或等于 300 万，则使用 `DISTSTYLE KEY`。  

---

## Output / 输出  
- The converted Redshift DDL is written to the specified output file.  
  转换后的 Redshift DDL 将写入指定的输出文件。  
- Statistics on changed, recommended, and prohibited columns are printed to the console.  
  关于已更改、推荐更改和禁止使用的列的统计信息将打印到控制台。  

---

## License / 许可证  
This project is licensed under the MIT License.  
本项目基于 MIT 许可证。  

---

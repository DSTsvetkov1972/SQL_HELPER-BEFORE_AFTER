-- SELECT * FROM audit.{{ project }}_svod WHERE length(`{{ container_field }}`)<>11
CREATE OR REPLACE TABLE audit.{{ project }}_svod
ENGINE = MergeTree()
ORDER BY tuple()
AS (

SELECT
    {{ "".join(svod_table_columns).replace('\n\t\n\t', '\n\t') }}
FROM
    {{ svod_table_name }} -- SELECT * FROM {{ svod_table_name }}
 
)
{# #}

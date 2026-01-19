-- SELECT * FROM audit.{{ user }}_svod
CREATE OR REPLACE TABLE audit.{{ user }}_svod
ENGINE = MergeTree()
ORDER BY tuple()
AS (

SELECT
    {{ "".join(svod_table_columns).replace('\n\t\n\t', '\n\t') }}
FROM
    {{ svod_table_name }}
 
)
{# #}

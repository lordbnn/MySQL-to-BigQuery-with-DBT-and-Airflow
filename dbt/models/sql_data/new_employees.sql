{{ config(materialized='table') }}

with source_data_table as(

    select 
   index,
   emp_no,
   birth_date,
   first_name,
   last_name,
   CASE
    WHEN gender = 'M' THEN 'Male'
    WHEN gender = 'F' THEN 'Female'
    ELSE gender
   END AS gender,
   hire_date
    
    from {{ source('sql_data', 'employees') }}
),

final as(

    select * from source_data_table
)

select * from final
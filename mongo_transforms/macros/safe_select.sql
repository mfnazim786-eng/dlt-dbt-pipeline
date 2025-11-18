{# macros/safe_select.sql #}

{% macro safe_select_column(relation, column_name) %}
  
  {# Get all the columns that actually exist in the table from the database #}
  {% set table_columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | list %}

  {# Generate the desired alias for the final column (e.g., "surveydata_email") #}
  {% set alias = column_name | lower | replace('survey_data__', 'surveydata_') %}

  {# Check if the desired column is in the list of existing columns #}
  {% if column_name in table_columns %}
    
    -- The column exists, so we select it and cast it to a STRING
    CAST({{ column_name }} AS STRING) AS "{{ alias }}"

  {% else %}

    -- The column does not exist, so we create a NULL column and cast it to a STRING
    CAST(NULL AS STRING) AS "{{ alias }}"

  {% endif %}

{% endmacro %}
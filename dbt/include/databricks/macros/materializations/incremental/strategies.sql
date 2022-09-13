{% macro dbt_databricks_get_incremental_sql(strategy, source, target, unique_key, partition_by, partitions=None) %}
  {%- set predicates = [] -%}
  {% if partitions is not None %}
    {% if partitions %}
      {% set partition_match %}
        {%- for partition_values in partitions -%}
          (
            {%- for partition_key in partition_by -%}{%- if loop.first %}({% endif -%}DBT_INTERNAL_DEST.{{ partition_key }} = "{{ partition_values[partition_key] }}"{%- if not loop.last %} AND {% else -%}){% endif -%}{%- endfor -%}
          ){%- if not loop.last %} OR {% endif -%}
        {%- endfor -%}
      {% endset %}
      {% if partition_match %}
        {% do predicates.append(partition_match) %}
      {% else %}
        {% do predicates.append('FALSE') %}
      {% endif %}
    {% else %}
      {% do predicates.append('FALSE') %}
    {% endif %}
  {% endif %}


  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target) }}
  {%- elif strategy == 'merge' -%}
  {#-- merge all columns with databricks delta - schema changes are handled for us #}
    {{ get_merge_sql(target, source, unique_key, dest_columns=none, predicates=predicates) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}
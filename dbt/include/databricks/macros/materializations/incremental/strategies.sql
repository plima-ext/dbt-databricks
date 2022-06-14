{% macro dbt_databricks_get_insert_overwrite_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ dbt_databricks_partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}


{% macro dbt_databricks_get_insert_into_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}


{% macro dbt_databricks_get_incremental_sql(strategy, source, target, unique_key, partition_by, partitions=None) %}
  {%- set predicates = [] -%}
  {% if partitions %}
    {% set partition_match %}
      {% for partition_key in partition_by -%}
      {{- log("checking partition key: " ~partition_key, True) -}}
        {% if partitions[partition_key] %}
        {%- if loop.first %}({% endif -%}
        {% for partition_values in partitions[partition_key] -%}
          {{- log("partition_values: " ~partition_values, True) -}}
          {%- if loop.first %}({% endif -%}{{ target }}.partition_key = "{{ p[partition_by] }}"{%- if not loop.last %} AND{% else -%}){% endif -%}
        {%- endfor -%}
        {%- if not loop.last %} OR{% else -%}){% endif -%}
        {% endif %}
      {%- endfor -%}
    {% endset %}
    {{- log("partition_match: " ~partition_match, True) -}}
    {% do predicates.append(partition_match) %}
  {% else %}
    {% do predicates.append('FALSE') %}
  {% endif %}
  {{- log("predicates: " ~predicates, True) -}}

  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ dbt_databricks_get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ dbt_databricks_get_insert_overwrite_sql(source, target) }}
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

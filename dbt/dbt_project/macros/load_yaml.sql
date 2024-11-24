{% macro load_yaml(file_path) %}
    {% set yaml_content = read_file(file_path) %}
    {{ return(fromyaml(yaml_content)) }}
{% endmacro %}

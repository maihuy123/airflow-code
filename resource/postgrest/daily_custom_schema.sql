select {% if column is not none %}
{{ column }}
{% else %}
*
{% endif %}
from {{ table }} 
{% if condition is not none %}
where {{ condition }}
{% endif %}
{% if limit is not none %}
limit {{ limit }}
{% endif %}
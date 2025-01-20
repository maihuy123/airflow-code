select {% if select is not none %}
{{ select }}
{% else %}
*
{% endif %}
from {{ table }} 
{% if where is not none %}
where {{ where }}
{% endif %}
{% if limit is not none %}
limit {{ limit }}
{% endif %}
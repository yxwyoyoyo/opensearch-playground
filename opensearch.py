from dotenv import dotenv_values
from opensearchpy import OpenSearch, helpers
from faker import Faker
from faker.providers import internet, date_time

config = dotenv_values()

host = config.get('OPENSEARCH_HOST', 'localhost')
port = config.get('OPENSEARCH_PORT', 9200)
auth = (config.get('OPENSEARCH_USER', 'admin'), config.get('OPENSEARCH_PASSWORD', 'admin'))
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_compression=True,
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
)
health = client.cat.health()
print(health)

template_name = 'logs-hss'
template_body = {
    'index_patterns': [
        'logs-hss'
    ],
    'data_stream': {},
    'priority': 100
}

create_index_template_result = client.indices.put_index_template(template_name, template_body)
get_index_template_result = client.indices.get_index_template(template_name)
print(get_index_template_result)

create_data_stream_result = client.indices.create_data_stream(template_name)
get_data_stream_result = client.indices.get_data_stream(template_name)
print(get_data_stream_result)

fake = Faker()
fake.add_provider(internet)
fake.add_provider(date_time)

logs = []
for _ in range(1_000):
    data = {
        'name': fake.name(),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'email': fake.email(),
        'ip_address': fake.ipv4(),
        '@timestamp': fake.date_time_this_month()
    }
    logs.append({
        '_index': template_name,
        '_op_type': 'create',
        '_source': data
    })

bulk_result = helpers.bulk(client=client, actions=logs)
print(bulk_result)

data_stream_status_result = client.indices.data_streams_stats(name=template_name)

client.indices.delete_index_template(template_name)
client.indices.delete_data_stream(template_name)

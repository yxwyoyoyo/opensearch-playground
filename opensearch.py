# # OpenSearch Playground

# ## Import packages
from dotenv import dotenv_values
from opensearchpy import OpenSearch, helpers
import json
import os
import time
from datetime import datetime
from faker import Faker
from faker.providers import internet, date_time

# ## Load ENV
config = dotenv_values()
os.environ['TZ'] = 'Asia/Shanghai'
time.tzset()

# ## Create OpenSearch client
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

# ## Define constants
fake = Faker()
fake.add_provider(internet)
fake.add_provider(date_time)
data_stream_name = 'logs_hss'
retention_unit = config.get('RETENTION_UNIT', 'd')
retention_value = config.get('RETENTION_VALUE', 30)
rollover_value = config.get('ROLLOVER_VALUE', 1)
policy_id = data_stream_name
policy_body = {
    'policy': {
        'description': 'Lifecycle policy for data stream {}'.format(data_stream_name),
        'default_state': 'rollover',
        'states': [
            {
                'name': 'rollover',
                'actions': [
                    {
                        'rollover': {
                            'min_size': '10gb',
                            'min_index_age': '{}{}'.format(rollover_value, retention_unit),
                        }
                    }
                ],
                'transitions': [
                    {
                        'state_name': 'delete',
                        'conditions': {
                            'min_index_age': '{}{}'.format(retention_value, retention_unit)
                        }
                    }
                ]
            },
            {
                'name': 'delete',
                'actions': [
                    {
                        'delete': {}
                    }
                ],
                'transitions': []
            }
        ],
        'ism_template': {
            'index_patterns': [policy_id],
            'priority': 100
        }
    }
}
json_body = json.dumps(policy_body)
template_name = data_stream_name
template_body = {
    'index_patterns': [
        template_name
    ],
    'data_stream': {
        'timestamp_field': {
            'name': '@timestamp'
        }
    },
    'template': {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0,
            'opendistro.index_state_management.policy_id': policy_id
        },
        'mappings': {
            'dynamic': False,
            'properties': {
                'name': { 'type': 'text' },
                'ip_address': { 'type': 'keyword' }
            }
        }
    },
    'priority': 100
}

# ## Create policy, index template, and data stream
create_policy_result = client.transport.perform_request(
    method='PUT', url='_plugins/_ism/policies/{}'.format(policy_id), body=policy_body)
print(create_policy_result)
create_index_template_result = client.indices.put_index_template(template_name, template_body)
get_index_template_result = client.indices.get_index_template(template_name)
print(get_index_template_result)
create_data_stream_result = client.indices.create_data_stream(template_name)
get_data_stream_result = client.indices.get_data_stream(template_name)
print(get_data_stream_result)

# ## Ingest data
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

# ## Show data stream status
data_stream_status_result = client.indices.data_streams_stats(name=template_name)
print(data_stream_status_result)

# ## Search in data stream
match_all_body = {
    'query': {
        'match_all': {}
    }
}
match_all_result = client.search(index=template_name, body=match_all_body)
print(match_all_result)

# ## Clean up
print(client.indices.delete_data_stream(template_name))
print(client.indices.delete_index_template(template_name))

# Simulate ingest
while True:
    data = {
        'name': fake.name(),
        'address': fake.address(),
        'phone_number': fake.phone_number(),
        'email': fake.email(),
        'ip_address': fake.ipv4(),
        '@timestamp': datetime.now(tz='Asia/Shanghai')
    }
    client.index(index=template_name, body=data)
    time.sleep(0.3)

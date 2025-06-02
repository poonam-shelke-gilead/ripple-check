# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC stored file to `/dbfs/FileStore/tables/service_now.xlsx`

# COMMAND ----------

# Install required libraries
%pip install pandas openpyxl gremlinpython networkx nest_asyncio

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Convert excel data into CSVs to load into Neptune
import pandas as pd

# 1. Load your Excel
input_file = "/dbfs/FileStore/tables/service_now.xlsx"  # <- Change this to your Excel filename
sheet_name = "Page 1"  # <- Change if needed

df = pd.read_excel(input_file, sheet_name=sheet_name, header=[0])

# 2. Standardize column names
df.columns = [
    'parent', 'parent_system_manager', 'parent_class', 'parent_operational_status', 'parent_environment', 'parent_gxp', 'parent_sox', 'parent_business_criticality', 'parent_hosting_type', 'parent_dr_subscribed', 'parent_dr_tier', 'parent_dr_capability_gap', 'parent_backup_required', 'type',
    'child', 'child_system_manager','child_secondary_system_manager', 'child_class', 'child_operational_status', 'child_environment', 'child_gxp', 'child_sox', 'child_location', 'child_manufacturer', 'discovery_source', 'created_by', 'child_ip_address', 'child_fully_qualified_domain_name', 'child_operating_System'
]

# 3. Create IDs
df['parent_id'] = (df['parent_class'] +'_'+ df['parent']).str.replace(r'\W+', '_', regex=True).str.lower()
df['child_id'] = (df['child_class'] +'_'+ df['child']).str.replace(r'\W+', '_', regex=True).str.lower()

# 4. Clean edge label from Type (forward direction only)
df['edge_label'] = df['type'].str.split("::").str[0].str.strip().str.lower().str.replace(r'\W+', '_', regex=True)

# 5. Create application nodes
applications = df[['parent_id', 'parent', 'parent_system_manager', 'parent_class', 'parent_operational_status', 'parent_environment', 'parent_gxp', 'parent_sox', 'parent_business_criticality', 'parent_hosting_type', 'parent_dr_subscribed', 'parent_dr_tier', 'parent_dr_capability_gap', 'parent_backup_required']].drop_duplicates()
applications = applications.rename(columns={
    'parent_id': '~id',
    'parent': 'name',
    'parent_system_manager': 'system_manager',
    'parent_class': '~label',
    'parent_operational_status': 'operational_status',
    'parent_environment': 'environment',
    'parent_gxp': 'gxp',
    'parent_sox': 'sox',
    'parent_business_criticality': 'business_criticality',
    'parent_hosting_type':'hosting_type',
    'parent_dr_subscribed':'dr_subscribed',
    'parent_dr_tier':'dr_tier',
    'parent_dr_capability_gap':'dr_capability_gap',
    'parent_backup_required':'backup_required'
})

# 6. Create component nodes
components = df[['child_id', 'child', 'child_system_manager','child_secondary_system_manager', 'child_class', 'child_operational_status', 'child_environment', 'child_gxp', 'child_sox', 'child_location', 'child_manufacturer', 'child_ip_address', 'child_fully_qualified_domain_name', 'child_operating_System']].drop_duplicates()
components = components.rename(columns={
    'child_id': '~id',
    'child':'name',
    'child_system_manager':'system_manager',
    'child_secondary_system_manager':'secondary_system_manager',
    'child_class':'~label',
    'child_operational_status':'operational_status',
    'child_environment':'environment',
    'child_gxp':'gxp',
    'child_sox':'sox',
    'child_location':'location',
    'child_manufacturer':'manufacturer',
    'child_ip_address':'ip_address',
    'child_fully_qualified_domain_name':'fully_qualified_domain_name',
    'child_operating_System':'operating_System'
})

# 7. Combine all nodes
nodes = pd.concat([applications, components])

# 8. Create edges
edges = df[['parent_id', 'child_id', 'edge_label', 'discovery_source', 'created_by']].drop_duplicates().rename(columns={
    'parent_id': '~from',
    'child_id': '~to',
    'edge_label': '~label'
})
edges['~id'] = ['e' + str(i) for i in range(1, len(edges) + 1)] 

# Important: Order columns nicely
# edges = edges[['~id', '~from', '~to', '~label', 'discovery_source', 'created_by']]
# 5. Save to CSV

# node_columns = ['~id', 'label', 'name', 'type', 'Environment', 'CPU', 'Disk Size']
# edge_columns = ['~id', 'from', 'to', 'label']

# nodes.to_csv('nodes.csv', columns=node_columns, index=False)
# edges.to_csv('edges.csv', columns=edge_columns, index=False)
nodes.to_csv('nodes.csv', index=False, encoding='utf-8')
edges.to_csv('edges.csv', index=False, encoding='utf-8')

print("✅ Done! Created 'nodes.csv' and 'edges.csv'")

# COMMAND ----------

# MAGIC %md
# MAGIC Files generated are stored in path `/Workspace/Users/poonam.shelke@gilead.com`
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /Workspace/Users/poonam.shelke@gilead.com

# COMMAND ----------

# Upload CSVs created in previous step into S3
import boto3

bucket_name = 'gilead-edp-ithackathon-dev-us-west-2-ghx-graph-lab'
prefix = 'servicenow-data/'
region = 'us-west-2'            # <-- Adjust if needed

# Upload from DBFS
s3 = boto3.client('s3', region_name=region)

s3.upload_file('/Workspace/Users/poonam.shelke@gilead.com/nodes.csv', bucket_name, prefix + 'nodes.csv')
s3.upload_file('/Workspace/Users/poonam.shelke@gilead.com/edges.csv', bucket_name, prefix + 'edges.csv')

print("✅ Uploaded 'nodes.csv' and 'edges.csv' to S3")


# COMMAND ----------

import json
import requests

neptune_endpoint = 'https://ghx-graph-lab-2025041016175056330000000d.c5wqa64eyomx.us-west-2.neptune.amazonaws.com:8182'
s3_path = f's3://{bucket_name}/{prefix}'
iam_role_arn = 'arn:aws:iam::296062546804:role/ghx-graph-lab-neptune'

load_url = f"{neptune_endpoint}/loader"

payload = {
    "source": s3_path,
    "format": "csv",
    "iamRoleArn": iam_role_arn,
    "region": "us-west-2",
    "failOnError": True,
    "parallelism": "HIGH",
    "updateSingleCardinalityProperties": True,
    "queueRequest": True
}

headers = {
    "Content-Type": "application/json"
}

response = requests.post(load_url, data=json.dumps(payload), headers=headers)
print("📤 Neptune load job started.")
print(json.dumps(response.json(), indent=2))


# COMMAND ----------

import asyncio
from gremlin_python.driver import client, serializer
import nest_asyncio
nest_asyncio.apply()


NEPTUNE_ENDPOINT = "wss://ghx-graph-lab-2025041016175056330000000d.c5wqa64eyomx.us-west-2.neptune.amazonaws.com:8182/gremlin"

gremlin_client = client.Client(
    NEPTUNE_ENDPOINT,
    'g',
    username="",
    password="",
    message_serializer=serializer.GraphSONSerializersV2d0()
)


# Function to fetch vertices
async def get_vertices():
    return gremlin_client.submit("g.V().valueMap(true)").all().result()

# Function to fetch edges
async def get_edges():
    return gremlin_client.submit('''
       g.E().project('from', 'to', 'label', 'discovery_source', 'created_by')
      .by(outV().id())
      .by(inV().id())
      .by(label())
      .by(values('discovery_source').fold())
      .by(values('created_by').fold())
                                 ''').all().result()

# Run inside notebook-safe loop
vertices = asyncio.get_event_loop().run_until_complete(get_vertices())
edges = asyncio.get_event_loop().run_until_complete(get_edges())

print(f"Vertices: {len(vertices)}")
print(f"Edges: {len(edges)}")

print(edges[0].keys())
print(vertices[0].keys())




# COMMAND ----------

import networkx as nx

G = nx.Graph()

# Add nodes
for v in vertices:
    node_id = v['id'][0] if isinstance(v['id'], list) else v['id']
    G.add_node(node_id, **{k: v[k][0] if isinstance(v[k], list) else v[k] for k in v if k != 'id'})

# Add edges
for e in edges:
    src = e['from'][0] if isinstance(e['from'], list) else e['from']
    dst = e['to'][0] if isinstance(e['to'], list) else e['to']
    G.add_edge(src, dst, **{k: e[k][0] if isinstance(e[k], list) else e[k] for k in e if k not in ['from', 'to']})

# COMMAND ----------

# G.V('business_application_lms_kite').addE('depends_on').to(G.V('business_application_grasp')) \
#   .property('discovery_source', 'test') \
#   .property('created_by', 'poonam')

G.add_edge('business_application_lims_kite', 'business_application_grasp')

# COMMAND ----------

from collections import deque

# Start from the initially down node
start_node = 'linux_server_a1s4hdbqa3g02'  # or any other known down node
G.nodes[start_node]['operational_status'] = 'down'

queue = deque([start_node])
visited = set([start_node])

while queue:
    current = queue.popleft()

    for neighbor in G.neighbors(current):
        if neighbor not in visited:
            visited.add(neighbor)
            queue.append(neighbor)

            node_type = G.nodes[neighbor].get('label')
            if node_type in ['Business Application']:
                G.nodes[neighbor]['operational_status'] = 'down'


# COMMAND ----------

import plotly.graph_objects as go

pos = nx.spring_layout(G,k=8, iterations=3000) 
highlight_id = "business_application_gilead_backup_archive_and_restore_gbar_"

node_border_colors = [
    '#000' if G.nodes[n].get('operational_status') == 'down' else '#FFF'  # red if matched, white otherwise
    for n in G.nodes()
]

node_border_widths = [
    4 if G.nodes[n].get('operational_status') == 'down' else 1
    for n in G.nodes()
]
# {'Cloud Load Balancer', 'Tag-Based Application Service', 'Windows Server', 'Linux Server', 'Server', 'Virtual Machine Instance', 'Compute Security Group', 'Business Application', 'Storage Server', 'Cloud Object Storage', 'MSFT SQL Instance'}
# Define consistent color per type
label_color_map = {
    'Cloud Load Balancer': '#1f77b4',
    'Tag-Based Application Service': '#ff7f0e',
    'Windows Server': '#2ca02c',
    'Linux Server': '#d62728',
    'Server': '#9467bd',
    'Virtual Machine Instance': '#8c564b',
    'Compute Security Group': '#e377c2',
    'Business Application': '#f781bf',
    'Storage Server': '#bcbd22',
    'Cloud Object Storage': '#17becf',
    'MSFT SQL Instance': '#aec7e8',
    'unknown': '#ffbb78'
}

# Get node types and apply fixed colors
node_types = [G.nodes[n].get('label', 'unknown') for n in G.nodes()]
node_colors = [label_color_map.get(t, 'black') for t in node_types]

label_size_map = {
    'Business Application': 30,
    'Cloud Load Balancer': 24,
    'Tag-Based Application Service': 22,
    'MSFT SQL Instance': 22,
    'Storage Server': 20,
    'Cloud Object Storage': 20,
    'Windows Server': 18,
    'Linux Server': 18,
    'Server': 16,
    'Virtual Machine Instance': 16,
    'Compute Security Group': 14,
    'Other': 10
}

# 2. Build list of sizes
node_sizes = [
    label_size_map.get(G.nodes[n].get('label', 'Other'), 10) 
    for n in G.nodes()
]

type_to_symbol = {
    'Business Application': 'octagon-dot',
    'Cloud Load Balancer': 'star-dot',
    'Tag-Based Application Service': 'hexagram-dot',
    'MSFT SQL Instance': 'square-x',
    'Storage Server': 'hash-dot',
    'Cloud Object Storage': 'bowtie',
    'Windows Server': 'star-square-dot',
    'Linux Server': 'x-dot',
    'Server': 'circle-cross',
    'Virtual Machine Instance': 'diamond-x',
    'Compute Security Group': 'cross-dot',
    'Other': 'circle'
}

node_symbols = [
    type_to_symbol.get(G.nodes[n].get('label', 'Other'), 'circle')
    for n in G.nodes()
]

# Extract edge coordinates
edge_x = []
edge_y = []
for edge in G.edges():
    x0, y0 = pos[edge[0]]
    x1, y1 = pos[edge[1]]
    edge_x.extend([x0, x1, None])
    edge_y.extend([y0, y1, None])

edge_trace = go.Scatter(
    x=edge_x, y=edge_y,
    line=dict(width=1, color='#888'),
    hoverinfo='none',
    mode='lines')

# Extract node coordinates and info
node_x = []
node_y = []
node_text = []
for node in G.nodes(data=True):
    x, y = pos[node[0]]
    node_x.append(x)
    node_y.append(y)
    node_text.append(str(node[1]))  # display node properties on hover

node_trace = go.Scatter(
    x=node_x, y=node_y,
    mode='markers',
    textposition="top center",
    hoverinfo='text',
    marker=dict(
    showscale=True,
    colorscale='Viridis',
    reversescale=False,
    size=node_sizes,
    color=node_colors,
    symbol=node_symbols,
    colorbar=dict(
        thickness=15,
        title='Node Type',
        xanchor='left',
        titleside='right'
    ),
    line=dict(
        width=node_border_widths,
        color=node_border_colors
    )
),

    text=[str(n) for n in G.nodes()],
    textfont=dict(size=9),
    hovertext=[str(id) for id in G.nodes()]
)

fig = go.Figure(data=[edge_trace, node_trace],
                layout=go.Layout(
                    title='Graph Visualization',
                    showlegend=False,
                    hovermode='closest',
                    margin=dict(b=20,l=5,r=5,t=40)
                ))

fig.show()

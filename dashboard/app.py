import os
import sys

import dash
import redis
from box import Box

from dash import dcc, html, Output, Input

import dash_bootstrap_components as dbc
from google.cloud import bigquery

from util.dashboard.navigation import get_navbar, navigate, navigate_filters

# Read configuration from YAML

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'
PAGES = [
    "overview",
    "collaboration",
    "author"
]

# Load the configuration file
config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

# -------------------- REDIS SETUP --------------------

# Initialize connection settings
settings = dict(
    redis_client=redis.StrictRedis.from_url(config.DASHBOARD.REDIS_URL),
    bq_client=bigquery.Client(project=config.GCP.PROJECT_ID),
    config=config
)

# -------------------- DASH APP --------------------

# Initialize the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app layout
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    get_navbar(),
    html.Div(id='page-content')
])


# Update the page content based on the URL
@app.callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(path_name: str):
    """
    Display the page content based on the URL.
    :param path_name: Path name.
    :return: Page content.
    """
    return navigate(path_name=path_name,
                    settings=settings)


@app.callback(
    [Output(f"navlink-{i}", "active") for i in PAGES],
    [Input('url', 'pathname')]
)
def toggle_active_links(path_name: str) -> list:
    """
    Toggle the active links in the navbar.
    :param path_name: Path name.
    :return: List of booleans.
    """
    return [path_name == "/" or path_name == '/overview', path_name == '/collaboration', path_name == '/author']


# Update the page content based on the URL
@app.callback(Output('navbar-filters', 'children'),
              Input('url', 'pathname'))
def display_page(path_name: str):
    return navigate_filters(path_name=path_name)


# -------------------- MAIN FUNCTION --------------------

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)

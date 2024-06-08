import dash

from dash import dcc, html, Output, Input

import dash_bootstrap_components as dbc

import util.dashboard.page_overview as page_overview
import util.dashboard.page_author as page_author


def get_navbar():
    return dbc.Navbar(
        dbc.Container(children=[
            html.A(
                dbc.Row([
                    dbc.Col(html.Img(src='/assets/eutopia.png', height="80px")),
                ], align="center"),
                href="/",
                style={"textDecoration": "none"}
            ),
            dbc.Collapse(
                dbc.Nav(
                    [
                        dbc.NavItem(dbc.NavLink("OVERVIEW", href="/overview", id='navlink-overview', className='h5')),
                        dbc.NavItem(dbc.NavLink("COLLABORATION IMPACT", href="/collaboration",
                                                id='navlink-collaboration', className='h5')),
                        dbc.NavItem(dbc.NavLink("AUTHOR", href="/author", id='navlink-author', className='h5')),
                    ], className="ml-auto", navbar=True),
                id="navbar-collapse",
                navbar=True
            ),
            dbc.Row(

                children=[],
                justify="end",
                align="center",
                className="ml-auto",
                id='navbar-filters'
            ),
        ],
            fluid=True),
        className='navbar-expand-lg navbar-custom',
        color="light",
        dark=False
    )


def get_page_collaboration():
    return dbc.Container(children=[
        dbc.Row(
            dbc.Col(
                html.H1("Welcome to Page 2!", className="text-left"),
                width=12
            )
        ),
        dbc.Row(
            dbc.Col(
                dcc.Graph(
                    figure={
                        'data': [
                            {'x': [1, 2, 3, 4], 'y': [10, 11, 12, 13], 'type': 'line', 'name': 'Mon'},
                            {'x': [1, 2, 3, 4], 'y': [14, 15, 16, 17], 'type': 'line', 'name': 'Tue'},
                        ],
                        'layout': {
                            'title': 'Line Plots'
                        }
                    }
                ),
                width=12
            )
        )
    ],
        fluid=True,
        className="mt-4")


def unknown_page():
    return dbc.Container(children=[
        dbc.Row(
            dbc.Col(
                html.H1("404 - Page not found", className="text-center"),
                width=12
            )
        )
    ],
        fluid=True,
        className="mt-4")


def navigate(path_name: str,
             settings: dict) -> dash.html.Div:
    if path_name == '/overview':
        return page_overview.layout(settings=settings)
    elif path_name == '/collaboration':
        return unknown_page()
    elif path_name == '/author':
        return page_author.layout(settings=settings)
    else:
        return unknown_page()


def navigate_filters(path_name: str = '/overview'):
    if path_name == '/overview':
        return get_filters_overview()
    elif path_name == '/collaboration':
        return get_filters_collaboration()
    else:
        return get_filters_overview()


def get_filters_overview():
    """
    Get the filters for the overview page.
    :return: The filters.
    """
    return [dbc.Col(
        dcc.Dropdown(
            id='filter-dropdown-1',
            options=[
                {'label': 'Option 1', 'value': 'option1'},
                {'label': 'Option 2', 'value': 'option2'},
                {'label': 'Option 3', 'value': 'option3'},
            ],
            placeholder="Select a filter",
            style={'minWidth': '150px'}
        ),
        width="auto"
    ),
        dbc.Col(
            dcc.Dropdown(
                id='filter-dropdown-2',
                options=[
                    {'label': 'Option A', 'value': 'optionA'},
                    {'label': 'Option B', 'value': 'optionB'},
                    {'label': 'Option C', 'value': 'optionC'},
                ],
                placeholder="Select a filter",
                style={'minWidth': '150px'}
            ),
            width="auto"
        )
    ]


def get_filters_collaboration():
    return [dbc.Col(
        dcc.Dropdown(
            id='filter-dropdown-2',
            options=[
                {'label': 'Option A', 'value': 'optionA'},
                {'label': 'Option B', 'value': 'optionB'},
                {'label': 'Option C', 'value': 'optionC'},
            ],
            placeholder="Select a filter",
            style={'minWidth': '150px'}
        ),
        width="auto"
    )
    ]


def get_filters_author(settings: dict):
    return [dbc.Col(
        dcc.Dropdown(
            id='filter-dropdown-2',
            options=[
                {'label': 'Option A', 'value': 'optionA'},
                {'label': 'Option B', 'value': 'optionB'},
                {'label': 'Option C', 'value': 'optionC'},
            ],
            placeholder="Select a filter",
            style={'minWidth': '150px'}
        ),
        width="auto"
    )
    ]
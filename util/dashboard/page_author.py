import dash

from dash import dcc, html, Output, Input
import plotly.graph_objects as go

import dash_bootstrap_components as dbc

import util.dashboard.query as queries



def trend_eutopia_collaboration(settings: dict):
    """
    Get the trend of Eutopia collaborations.
    :param settings: The settings for connection to Redis and BigQuery.
    :return: The trend of Eutopia collaborations.
    """
    df_trend_eutopia_collaboration = queries.overview_trend_eutopia_collaboration(settings=settings)

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df_trend_eutopia_collaboration['Year'],
                             y=df_trend_eutopia_collaboration['Eutopian Collaborations'],
                             mode='lines+markers',
                             name='Eutopian Collaborations',
                             marker=dict(color=settings['config'].DASHBOARD.COLORS.CLASS_COLORS[0]),
                             line=dict(color=settings['config'].DASHBOARD.COLORS.CLASS_COLORS[0]))
                  )

    fig.update_layout(
        title='EUTOPIA COLLABORATION TREND',
        xaxis=dict(
            title='Year',
            showgrid=False,
            color=settings['config'].DASHBOARD.COLORS.TEXT_COLOR
        ),
        yaxis=dict(
            title='EUTOPIA Collaboration Articles',
            showgrid=False,
            color=settings['config'].DASHBOARD.COLORS.TEXT_COLOR,
            zeroline=False
        ),
        font=dict(
            family='Open Sans, sans-serif'
        ),
        plot_bgcolor=settings['config'].DASHBOARD.COLORS.BACKGROUND_COLOR,
        paper_bgcolor=settings['config'].DASHBOARD.COLORS.BACKGROUND_COLOR,
        font_color=settings['config'].DASHBOARD.COLORS.TEXT_COLOR)

    return dcc.Graph(figure=fig)


def layout(settings: dict):
    """
    Get the layout for the overview page.
    :param settings: The settings for connection to Redis and BigQuery.
    :return: The layout.
    """
    return dbc.Container(children=[
        dbc.Row(
            dbc.Col(
                html.H4("COLLABORATION OVERVIEW", className="text-left p-2 font-italic"),
                width=12
            )
        ),
        # Some space between the title and the cards
        dbc.Row(children=cards_base_metrics(settings=settings),
                className="gray-background-custom m-1"),
        dbc.Row(
            children=[
                dbc.Col(
                    trend_eutopia_collaboration(settings=settings),
                    className="m-1 mt-4"
                ),
                dbc.Col(
                    breakdown_publications_by_institution(settings=settings),
                    className="m-1 mt-4"
                )
            ]
        )
    ],
        className='p-4',
        fluid=True
    )

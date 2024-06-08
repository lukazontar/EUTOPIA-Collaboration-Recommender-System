import dash

from dash import dcc, html, Output, Input
import plotly.graph_objects as go

import dash_bootstrap_components as dbc

import util.dashboard.query as queries


def create_card(value: float,
                title: str) -> dbc.Card:
    """
    Create a card with a value and a title.
    :param value:  Numeric value.
    :param title: Metric title.
    :return: The card.
    """
    return dbc.Card(
        dbc.CardBody([
            html.H3(f"{value:,}", className="card-text text-center"),
            html.P(title, className="card-title text-center")
        ]),
        className="card-custom"
    )


def cards_base_metrics(settings: dict):
    """
    Get the base metrics for the overview page.
    :param settings: The settings for connection to Redis and BigQuery.
    :return: The base metrics cards.
    """
    df_cards = queries.overview_cards(settings=settings)

    children = [dbc.Col(
        create_card(value=df_cards[col].values[0],
                    title=col),
        width=2,
        className="mt-2"
    ) for col in df_cards.columns]

    return children


def breakdown_publications_by_institution(settings: dict):
    """
    Get the breakdown of publications by institution.
    :param settings: The settings for connection to Redis and BigQuery.
    :return: The breakdown of publications by institution.
    """
    df_breakdown_publications_by_institution = queries.overview_breakdown_publications_by_institution(settings=settings)

    fig = go.Figure()

    fig.add_trace(go.Bar(x=df_breakdown_publications_by_institution['Articles'],
                         y=df_breakdown_publications_by_institution['Institution'],
                         marker=dict(color=settings['config'].DASHBOARD.COLORS.CLASS_COLORS[0]),
                         orientation='h'
                         ),

                  )

    fig.update_layout(
        title='BREAKDOWN OF ARTICLES BY INSTITUTION',
        xaxis=dict(
            title='Articles',
            showgrid=False,
            color=settings['config'].DASHBOARD.COLORS.TEXT_COLOR,
            zeroline=False
        ),
        yaxis=dict(
            title='Institution',
            showgrid=False,
            color=settings['config'].DASHBOARD.COLORS.TEXT_COLOR
        ),
        font=dict(
            family='Open Sans, sans-serif'
        ),
        plot_bgcolor=settings['config'].DASHBOARD.COLORS.BACKGROUND_COLOR,
        paper_bgcolor=settings['config'].DASHBOARD.COLORS.BACKGROUND_COLOR,
        font_color=settings['config'].DASHBOARD.COLORS.TEXT_COLOR)

    return dcc.Graph(figure=fig)


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

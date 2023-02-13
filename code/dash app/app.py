
import dash
import dash_html_components as html
import plotly.graph_objects as go
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import plotly.express as px
from dash.dependencies import Input, Output
from datetime import datetime as dt
import pathlib

import os
import pandas as pd

# initialize app
app = dash.Dash(__name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}]
        )

server = app.server 

# set path
BASE_PATH = pathlib.Path(__file__).parent.resolve()
DATA_PATH = BASE_PATH.joinpath("data").resolve()

# read data
df = pd.read_csv(DATA_PATH.joinpath('stables_mb_clean.csv'))
df_mint = df[df['action'].isin(['mint'])]
df_burn = df[df['action'].isin(['burn'])]

# github button
button_github = dbc.Button(
    "View Code on github",
    outline=True,
    color="dark",
    href="https://github.com/brendanpmalone/onchain_data_blog",
    id="gh-link",
    style={"text-transform": "none","width": "200px"},
)

# card for header/nav bar 
header = dbc.Navbar(
    dbc.Container(
        [
            dbc.Row(
                [
                    dbc.Col(
                        html.Img(
                            id="logo",
                            src=app.get_asset_url("Policy.png"),
                            height="82px",
                            width="200px"
                        ),
                        md="auto",
                    ),
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.H3("Fiat-backed stablecoins on Ethereum"),
                                    html.P("Mint/Burn Dashboard"),
                                ],
                                id="app-title",
                            )
                        ],
                        md=True,
                        align="center",
                    ),
                ],
                align="center",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.NavbarToggler(id="navbar-toggler"),
                            dbc.Collapse(
                                dbc.Nav(
                                    [
                                        dbc.NavItem(button_github),
                                    ],
                                    navbar=True,
                                ),
                                id="navbar-collapse",
                                navbar=True,
                            ),
                        ],
                        md=2,
                    ),
                ],
                align="center",
            ),
        ],
        fluid=True,
    ),
    dark=True,
    color="white",
    sticky="top",
)

# card for parameter selector
first_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("Parameters", className="card-title"),
            html.Br(),
            html.P("Date range:"),
            dcc.DatePickerRange(
                id="date-picker-select",
                start_date=dt(2017, 11, 28),
                end_date=dt(2022, 12, 31),
                min_date_allowed=dt(2017, 11, 28),
                max_date_allowed=dt(2022, 12, 31),
                initial_visible_month=dt(2017, 11, 28),
                style={"color": "black", "border-color": "black"}
            ),
            html.P(""),
            html.P("Stablecoins included:"),
            dcc.Checklist(
                id='stablecoins_list',
                options=["USDC", "USDT", "Binance USD", "Pax Dollar", "Gemini Dollar"],
                value=["USDC"],
                labelStyle={'display': 'block'},
                style={"height":200, "width":200, "overflow":"auto"},
                inputStyle= {"margin-right":10}
            ),
        ]
    )
)

# card for charts
second_card = dbc.Card(
    dbc.CardBody(
        [
            html.H5("Charts", className="card-title"),
            html.P(""),
            html.P("Notes:"),
            html.Ul([
                html.Li("Charts 1 and 2 display the sum of mints or burns (per hour and weekday combination) for all stablecoins selected in the date range."),
                html.Li("Chart 3 plots each mint/burn event for all stablecoins selected in the date range.")
            ]),
            dcc.Graph(id = 'heatmap1'),
            dcc.Graph(id = 'heatmap2'),
            dcc.Graph(id = 'scatter'),
        ]
    )
)

cards = dbc.Row(
    [
        dbc.Col(first_card, 
        xs=dict(order=1, size=12),
        sm=dict(order=1, size=4)),

        dbc.Col(second_card, 
        xs=dict(order=2, size=12),
        sm=dict(order=1, size=8)),
    ]
)

# set layout 
app.layout = html.Div(
    [
        header, 
        dbc.Container(cards, fluid=True, class_name='root-container'),
        html.P("   Disclaimer: This data is being provided for informational purposes only. No guarantee, representation or warranty is being made, express or implied, as to the accuracy of the data provided."),
    ]
)

### minting heatmap 
@app.callback(Output(component_id='heatmap1', component_property= 'figure'),
              [
                Input(component_id='stablecoins_list', component_property= 'value'),
                Input("date-picker-select", "start_date"),
                Input("date-picker-select","end_date"), 
              ]
            )

def minting(checklist_value, start, end):
    print(checklist_value)
    print(start)
    print(end)

    filtered_df_mint = df_mint[df_mint['stablecoin'].isin(checklist_value)]
    filtered_df_mint = filtered_df_mint.sort_values('date').set_index('date')[
        start:end
    ]
    
    filtered_df_mint = filtered_df_mint.groupby(['weekday','hour'],as_index=False)['amount_usd'].sum()

    fig = go.Figure(data=go.Heatmap(x=filtered_df_mint['weekday'], y=filtered_df_mint['hour'], z=filtered_df_mint['amount_usd'], colorscale='Viridis')
                    )

    fig.update_layout(title = '1. Stablecoin Minting',
                      xaxis_title = 'Day of Week',
                      yaxis_title = 'Hour of Day (UTC)',
                      xaxis = dict(
                        tickmode = 'array',
                        tickvals = [0, 1, 2, 3, 4, 5, 6],
                        ticktext = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]),
                      font = dict(
                        family = "RiformaLL",
                        color = 'black'
                      )
                    )
    return fig  

### burning heatmap 
@app.callback(Output(component_id='heatmap2', component_property= 'figure'),
              [
                Input(component_id='stablecoins_list', component_property= 'value'),
                Input("date-picker-select", "start_date"),
                Input("date-picker-select","end_date"), 
              ]
            )

def burning(checklist_value, start, end):
    print(checklist_value)
    print(start)
    print(end)

    filtered_df_burn = df_burn[df_burn['stablecoin'].isin(checklist_value)]
    filtered_df_burn = filtered_df_burn.sort_values('date').set_index('date')[
        start:end
    ]

    filtered_df_burn = filtered_df_burn.groupby(['weekday','hour'],as_index=False)['amount_usd'].sum()

    fig = go.Figure(data=go.Heatmap(x=filtered_df_burn['weekday'], y=filtered_df_burn['hour'], z=filtered_df_burn['amount_usd'], colorscale='Inferno')
                    )

    fig.update_layout(title = '2. Stablecoin Burning',
                      xaxis_title = 'Day of Week',
                      yaxis_title = 'Hour of Day (UTC)',
                      xaxis = dict(
                        tickmode = 'array',
                        tickvals = [0, 1, 2, 3, 4, 5, 6],
                        ticktext = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]),
                      font = dict(
                        family = "RiformaLL",
                        color = 'black'
                      )
                    )
    return fig  

### scatter plot 
@app.callback(Output(component_id='scatter', component_property= 'figure'),
              [
                Input(component_id='stablecoins_list', component_property= 'value'),
                Input("date-picker-select", "start_date"),
                Input("date-picker-select","end_date"), 
              ]
            )

def scatter(checklist_value, start, end):
    print(checklist_value)
    print(start)
    print(end)

    df['scatter_date'] = df['date']
    filtered_df_scatter = df[df['stablecoin'].isin(checklist_value)]
    filtered_df_scatter = filtered_df_scatter.sort_values('date').set_index('date')[
        start:end
    ]

    fig = px.scatter(filtered_df_scatter,
        x='scatter_date', 
        y='net_issuance', 
        color='stablecoin', 
        opacity = 0.5,
        color_discrete_map={
            'USDC': 'royalblue',
            'USDT': 'teal',
            'Binance USD': 'gold',
            'Pax Dollar': 'mediumseagreen',
            'Gemini Dollar': 'darkturquoise'
            }
        )
      
    fig.update_layout(title = '3. Net Issuance',
                      xaxis_title = 'Date',
                      yaxis_title = 'Size of Mint (positive) / Burn (negative)',
                      legend_title = 'Stablecoin',
                      font = dict(
                        family = "RiformaLL",
                        color = 'black'
                      )
                    )

    return fig 

# start the Dash server
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8080)
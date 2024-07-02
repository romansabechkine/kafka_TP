import datetime
from json import dumps
import os
import urllib.parse
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go

from dash import Dash, html, dcc, callback, dash_table, Output, Input

from pymongo import MongoClient
from dotenv import load_dotenv
from bson import json_util


def run_dash_app():

    load_dotenv()
    MONGO_USER = os.getenv('MONGO_USER')
    MONGO_PWD = os.getenv('MONGO_PWD')
    MONGO_IP = os.getenv('MONGO_IP')
    MONGO_PORT = os.getenv('MONGO_PORT')
  

    mongo_username = urllib.parse.quote_plus(MONGO_USER)
    mongo_password = urllib.parse.quote_plus(MONGO_PWD)
    mongo_ip = urllib.parse.quote_plus(MONGO_IP)
    mongo_port = urllib.parse.quote_plus(MONGO_PORT)

    mongo_url = 'mongodb://%s:%s@%s:%s/' % (mongo_username , mongo_password, mongo_ip, mongo_port)


    app = Dash(external_stylesheets=[dbc.themes.CYBORG])

    style_table = {
        "style_filter": {'backgroundColor': 'black'}, 
        "style_header":{'backgroundColor': 'black', 'fontSize': 15, 'fontWeight': 'bold', 'color': 'steelblue'},
        "style_cell":{'backgroundColor': 'black', 'color': 'white', 'fontSize': 12, 'maxWidth':20}
    }

    app.layout = dbc.Container(children=[
                dcc.Interval(
                    id='interval-component',
                    interval=60000, # in milliseconds, update graphs every minute
                    n_intervals=0
                ),
                dbc.Row([
                    dbc.Col([
                        html.Div(className="board-title", children=["FRANCE Weather Dashboard"])
                    ])
                ]),
                dbc.Row(children=[
                    dbc.Col(children = 
                        [
                            dcc.Graph(id='fig-temp', animate=True)
                        ], 
                    ),
                    dbc.Col(
                        children =
                        [
                            dcc.Graph(id='fig-wind', animate=True)
                        ], 
                    ),
                    dbc.Col(
                        children =
                        [
                            dcc.Graph(id='fig-humidity', animate=True)
                        ], 
                    )
                ]),
                dbc.Row([
                    dbc.Col([
                         html.Div(className="board-header", children=["Day Statistics"])
                    ])
                ]),
                dbc.Row([
                    dbc.Col([
                        html.Div(className="table-title", children=["Temperature, °C"]),
                        dash_table.DataTable(id='table-temp', **style_table, style_as_list_view=True)
                    ], width=4),
                    dbc.Col([
                        html.Div(className="table-title", children=["Wind, kph"]),
                        dash_table.DataTable(id='table-wind', **style_table, style_as_list_view=True)
                    ], width=4),
                    dbc.Col([
                        html.Div(className="table-title", children=["Humidity, %"]),
                        dash_table.DataTable(id='table-humidity', **style_table, style_as_list_view=True)
                    ], width=4),
                ])
               
            ])
    

    @callback(Output('fig-temp', 'figure'),
              Output('fig-wind', 'figure'),
              Output('fig-humidity', 'figure'),
              Output('table-temp', 'data'),
              Output('table-wind', 'data'),
              Output('table-humidity', 'data'),
              Input('interval-component', 'n_intervals'))
    def update_metrics(n):

        client2 = MongoClient(mongo_url)
        db2 = client2["weather"]
        collection = db2["weather-collection"]
        cursor = collection.find(filter={}, projection={'_id': False})
        list_cur = list(cursor)
        json_data = json_util.dumps(list_cur)
        df = pd.read_json(json_data)

        current_datetime = datetime.datetime.now()
        midnight_datetime = datetime.datetime.combine(current_datetime, datetime.datetime.min.time())

        dct_metrics = {"temperature": None, "wind_kph": None, "humidity": None}

        for metric in dct_metrics.keys():
            dct = {"city": [], "mean": [], "min": [], "max": []}
            for city in df["city"].unique():
                mean = np.round(df[(df['city'] == city) & (df['datetime'] >= midnight_datetime)][metric].mean(), 2)
                min = np.round(df[(df['city'] == city) & (df['datetime'] >= midnight_datetime)][metric].min(), 2)
                max = np.round(df[(df['city'] == city) & (df['datetime'] >= midnight_datetime)][metric].max(), 2)
                dct["city"].append(city)
                dct["mean"].append(mean)
                dct["min"].append(min)
                dct["max"].append(max)
            dct_metrics[metric] = dct
        

        margin = dict(l=20, r=20, t=20, b=20)
        fig1 = px.line(df, x='datetime', y='temperature', color='city', template='plotly_dark')
        fig1.update_layout(width=400, height=300, margin=margin, title_font_family="Times New Roman", yaxis_title="Temperature, °C", xaxis_title=None, font_size=10)
        fig1.update_traces(line=dict(width=0.5))
        fig2 = px.line(df, x='datetime', y='wind_kph', color='city', template='plotly_dark')
        fig2.update_layout(width=400, height=300, margin=margin, yaxis_title="Wind, kph", xaxis_title=None, font_size=10)
        fig2.update_traces(line=dict(width=0.5))
        fig3 = px.line(df, x='datetime', y='humidity', color='city', template='plotly_dark')
        fig3.update_layout(width=400, height=300, margin=margin, yaxis_title="Humidity, %", xaxis_title=None, font_size=10)
        fig3.update_traces(line=dict(width=0.5))

        #print(dct_metrics['temperature'])
        #print('-'*50)
        #print(df.from_dict(dct_metrics['temperature']).to_dict('records'))

        data_temp=df.from_dict(dct_metrics['temperature']).to_dict('records')
        data_wind=df.from_dict(dct_metrics['wind_kph']).to_dict('records')
        data_humidity=df.from_dict(dct_metrics['humidity']).to_dict('records')

        print("LOG: Updating dashboard: %s" % datetime.datetime.now().isoformat(timespec='minutes'))

        
        return [
            fig1, fig2, fig3, data_temp, data_wind, data_humidity
        ]
        
    app.run_server(debug=True,
                   use_reloader=False,
                   host='0.0.0.0',
                   port=8050)
    
'''if __name__ == "__main__":
    run_dash_app()'''


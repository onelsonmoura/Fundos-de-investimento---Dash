from dash import Dash, html, dcc
import plotly.express as px

app = Dash(__name__)

app.layout = html.Div([

    html.Div(className='grid_details', children=[
            html.Div('Olá Mundo')
        ])
    ])

if __name__ == '__main__':
    app.run(debug= True, port = 8050)
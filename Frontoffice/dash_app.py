import sys
import os

# Add the parent directory (project_root) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from dash import Dash, dcc, html, Output, Input
import dash_bootstrap_components as dbc
from Backoffice.loading import dash_data_functions as ddf

app = Dash(__name__, external_stylesheets=[dbc.themes.SOLAR])

# --------------------------------- Data ------------------------------------
# Fetch initial market data (static at startup, can be dynamic in callbacks)
market_data = ddf.read_market_data_summary_1_24()

# Define the default columns to display
default_columns = ['symbol_ticker', 'close', 'posts_created', 'interactions', 'include_etl']

# Columns to choose from in the dropdown
column_options = [{'label': col, 'value': col} for col in market_data.columns]

# Predefined filters
filter_options = [
    {'label': 'All', 'value': 'all'},
    {'label': 'Watched', 'value': 'watched'},
    {'label': '$ < 0.001', 'value': 'micro'}
]

# --------------------------------- Layout Elements ------------------------------------
header = dbc.Row(
    dbc.Col(
        html.H1("GoldenEye - Attention Trading", className="text-center text-primary"),
        className="bg-light py-2"
    ),
    className="header"
)

content = dbc.Row(
    [
        # Left Column - Market Stats and related controls
        dbc.Col(
            [
                html.H2("Symbols", className="text-center text-info mb-4"),
                # Data Table showing selected columns
                html.Div(id='market-stats-output', className="left-column market-stats-table mb-4"),
                # Dropdown for selecting which columns to display
                dcc.Dropdown(
                    id='column-dropdown',
                    options=column_options,
                    value=default_columns,  # Default selected columns
                    multi=True,
                    className="mb-4"
                ),
                html.H4("Filters", className="text-center text-muted mb-4"),
                # Radio buttons for predefined filters
                dcc.RadioItems(
                    id='filter-radio',
                    options=filter_options,
                    value='all',  # Default to 'all'
                    labelStyle={'display': 'inline-block', 'margin-right': '10px'},
                    className="mb-4"
                ),
            ],
            width=4
        ),
        
        # Right Column - Symbol-specific data
        dbc.Col(
            [
                html.Div(id='symbol-data-output', className="right-column bg-dark text-white p-3"),
                html.Div(ddf.hello, className="right-column bg-dark text-white p-3")
            ],
            width=8
        ),
    ],
    className="content-row"
)

app.layout = dbc.Container([header, content], fluid=True, className="dash-container")

# --------------------------------- Callbacks ------------------------------------
@app.callback(
    Output('market-stats-output', 'children'),
    Input('column-dropdown', 'value'),
    Input('filter-radio', 'value')
)
def update_market_stats(selected_columns, selected_filter):
    # Filter the data based on the selected filter
    if selected_filter == 'watched':
        filtered_data = market_data[market_data['include_etl'] == True]
    else:
        filtered_data = market_data.copy()  # All data

    # Sort by last_update descending
    filtered_data.sort_values(by='last_update', ascending=False, inplace=True)

    # Select only the chosen columns
    displayed_data = filtered_data[selected_columns]

    # Optionally, limit the number of rows to display
    #displayed_data = displayed_data.head(10)

    # Return the data as a table
    return dbc.Table.from_dataframe(displayed_data, striped=True, bordered=True, hover=True)

# --------------------------------- Run App ------------------------------------
if __name__ == '__main__':
    app.run_server(port=8000)

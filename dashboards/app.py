import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'spotify_data')
DB_USER = os.getenv('DB_USER', 'spotify_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'spotify_password')

# Create SQLAlchemy engine
connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = create_engine(connection_string)

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# App layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Spotify Data Pipeline Dashboard", className="text-center my-4"), width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            html.H4("Database Stats"),
            dbc.Card(
                dbc.CardBody([
                    html.Div(id="db-stats"),
                ]),
                className="mb-4"
            )
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            html.H4("Streaming Activity Over Time"),
            dcc.DatePickerRange(
                id='date-picker-range',
                start_date_placeholder_text="Start Date",
                end_date_placeholder_text="End Date",
                calendar_orientation='horizontal',
                className="mb-3"
            ),
            dcc.Graph(id="streaming-timeline")
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            html.H4("Top Artists"),
            dcc.RadioItems(
                id='top-artists-metric',
                options=[
                    {'label': 'By Play Count', 'value': 'count'},
                    {'label': 'By Unique Listeners', 'value': 'unique'}
                ],
                value='count',
                inline=True,
                className="mb-3"
            ),
            dcc.Graph(id="top-artists-chart")
        ], width=6),
        
        dbc.Col([
            html.H4("Top Tracks"),
            dcc.RadioItems(
                id='top-tracks-metric',
                options=[
                    {'label': 'By Play Count', 'value': 'count'},
                    {'label': 'By Unique Listeners', 'value': 'unique'}
                ],
                value='count',
                inline=True,
                className="mb-3"
            ),
            dcc.Graph(id="top-tracks-chart")
        ], width=6)
    ]),
    
    dbc.Row([
        dbc.Col([
            html.H4("Listening by Hour of Day"),
            dcc.Graph(id="hourly-distribution")
        ], width=12)
    ]),
    
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # in milliseconds (1 minute)
        n_intervals=0
    )
], fluid=True)

@app.callback(
    Output("db-stats", "children"),
    Input("interval-component", "n_intervals")
)
def update_db_stats(n):
    try:
        # Get table row counts
        stats = []
        for schema in ['raw', 'processed', 'analytics']:
            for table in get_tables_in_schema(schema):
                count = get_table_count(schema, table)
                stats.append(
                    dbc.ListGroupItem(f"{schema}.{table}: {count:,} rows")
                )
        
        return dbc.ListGroup(stats)
    except Exception as e:
        logger.error(f"Error updating DB stats: {e}")
        return html.Div(f"Error: {str(e)}")

@app.callback(
    Output("streaming-timeline", "figure"),
    [Input("date-picker-range", "start_date"),
     Input("date-picker-range", "end_date")]
)
def update_streaming_timeline(start_date, end_date):
    try:
        # Default date range if none selected
        if not start_date or not end_date:
            query = """
            SELECT 
                DATE(played_at) as date, 
                COUNT(*) as play_count
            FROM 
                raw.streaming_history
            GROUP BY 
                DATE(played_at)
            ORDER BY 
                date
            """
        else:
            query = f"""
            SELECT 
                DATE(played_at) as date, 
                COUNT(*) as play_count
            FROM 
                raw.streaming_history
            WHERE 
                DATE(played_at) BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 
                DATE(played_at)
            ORDER BY 
                date
            """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return empty_figure("No streaming data available for the selected date range")
        
        fig = px.line(
            df, 
            x="date", 
            y="play_count",
            title="Daily Streaming Activity",
            labels={"date": "Date", "play_count": "Number of Streams"}
        )
        
        return fig
    except Exception as e:
        logger.error(f"Error updating streaming timeline: {e}")
        return empty_figure(f"Error: {str(e)}")

@app.callback(
    Output("top-artists-chart", "figure"),
    Input("top-artists-metric", "value")
)
def update_top_artists(metric):
    try:
        metric_column = "COUNT(*)" if metric == "count" else "COUNT(DISTINCT user_id)"
        metric_name = "Play Count" if metric == "count" else "Unique Listeners"
        
        query = f"""
        SELECT 
            a.name as artist_name, 
            {metric_column} as metric
        FROM 
            raw.streaming_history sh
        JOIN 
            raw.tracks t ON sh.track_id = t.track_id
        JOIN 
            raw.artists a ON t.artist_id = a.artist_id
        GROUP BY 
            a.name
        ORDER BY 
            metric DESC
        LIMIT 10
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return empty_figure("No artist data available")
        
        fig = px.bar(
            df, 
            x="artist_name", 
            y="metric",
            title=f"Top 10 Artists by {metric_name}",
            labels={"artist_name": "Artist", "metric": metric_name}
        )
        
        fig.update_layout(xaxis={'categoryorder': 'total descending'})
        
        return fig
    except Exception as e:
        logger.error(f"Error updating top artists: {e}")
        return empty_figure(f"Error: {str(e)}")

@app.callback(
    Output("top-tracks-chart", "figure"),
    Input("top-tracks-metric", "value")
)
def update_top_tracks(metric):
    try:
        metric_column = "COUNT(*)" if metric == "count" else "COUNT(DISTINCT user_id)"
        metric_name = "Play Count" if metric == "count" else "Unique Listeners"
        
        query = f"""
        SELECT 
            t.name as track_name, 
            a.name as artist_name,
            {metric_column} as metric
        FROM 
            raw.streaming_history sh
        JOIN 
            raw.tracks t ON sh.track_id = t.track_id
        JOIN 
            raw.artists a ON t.artist_id = a.artist_id
        GROUP BY 
            t.name, a.name
        ORDER BY 
            metric DESC
        LIMIT 10
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return empty_figure("No track data available")
        
        # Create a combined label for track and artist
        df['track_label'] = df['track_name'] + '<br><i>by ' + df['artist_name'] + '</i>'
        
        fig = px.bar(
            df, 
            x="metric", 
            y="track_label",
            title=f"Top 10 Tracks by {metric_name}",
            labels={"track_label": "Track", "metric": metric_name}
        )
        
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        
        return fig
    except Exception as e:
        logger.error(f"Error updating top tracks: {e}")
        return empty_figure(f"Error: {str(e)}")

@app.callback(
    Output("hourly-distribution", "figure"),
    Input("interval-component", "n_intervals")
)
def update_hourly_distribution(n):
    try:
        query = """
        SELECT 
            EXTRACT(HOUR FROM played_at) as hour,
            COUNT(*) as play_count
        FROM 
            raw.streaming_history
        GROUP BY 
            hour
        ORDER BY 
            hour
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return empty_figure("No hourly distribution data available")
        
        # Ensure all hours are represented
        all_hours = pd.DataFrame({'hour': range(24)})
        df = pd.merge(all_hours, df, on='hour', how='left').fillna(0)
        
        fig = px.bar(
            df, 
            x="hour", 
            y="play_count",
            title="Listening Activity by Hour of Day",
            labels={"hour": "Hour of Day (24h)", "play_count": "Number of Streams"}
        )
        
        # Add a smoother trend line
        fig.add_trace(
            go.Scatter(
                x=df["hour"],
                y=df["play_count"].rolling(3, center=True).mean(),
                mode='lines',
                name='Trend',
                line=dict(color='red')
            )
        )
        
        fig.update_layout(xaxis=dict(
            tickmode='linear',
            tick0=0,
            dtick=1
        ))
        
        return fig
    except Exception as e:
        logger.error(f"Error updating hourly distribution: {e}")
        return empty_figure(f"Error: {str(e)}")

def get_tables_in_schema(schema):
    """Get list of tables in a schema"""
    try:
        query = f"""
        SELECT 
            table_name 
        FROM 
            information_schema.tables 
        WHERE 
            table_schema = '{schema}'
        ORDER BY 
            table_name
        """
        df = pd.read_sql(query, engine)
        return df['table_name'].tolist()
    except Exception as e:
        logger.error(f"Error getting tables in schema {schema}: {e}")
        return []

def get_table_count(schema, table):
    """Get row count for a table"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
            return result.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting count for {schema}.{table}: {e}")
        return 0

def empty_figure(message):
    """Return an empty figure with an error message"""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False
    )
    fig.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        plot_bgcolor="white",
        paper_bgcolor="white"
    )
    return fig

# Run the application
if __name__ == '__main__':
    try:
        # Test database connection
        with engine.connect() as connection:
            logger.info("Successfully connected to database")
            
        # Run server
        app.run_server(debug=os.getenv('DASH_DEBUG_MODE', 'False').lower() == 'true',
                      host='0.0.0.0', port=8050)
    except Exception as e:
        logger.error(f"Application error: {e}") 

import dash
from dash import dcc, html
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# DB Connection
engine = create_engine(os.getenv("PG_CONN_STR"))

# Query crop yield + precipitation by season
def load_yield_data():
    query = '''
    SELECT
        s.name AS season,
        cy.year,
        AVG(cy.yield_per_acre) AS yield_per_acre,
        AVG(we.precipitation) AS avg_precipitation,
        c.name AS crop
    FROM crop_yield cy
    JOIN season s ON cy.season_id = s.season_id
    JOIN crop c ON cy.crop_id = c.crop_id
    JOIN location l ON cy.location_id = l.location_id
    JOIN weather_station ws ON ws.location_id = l.location_id
    JOIN weather_event we ON we.station_id = ws.station_id AND we.season_id = cy.season_id
    GROUP BY cy.year, s.name, c.name
    ORDER BY cy.year, s.name;
    '''
    return pd.read_sql(query, engine)

# Query yield by county for map
def load_map_data():
    query = '''
    SELECT
        l.county_name,
        l.fips_code,
        AVG(cy.yield_per_acre) AS yield_per_acre,
        c.name AS crop
    FROM crop_yield cy
    JOIN crop c ON cy.crop_id = c.crop_id
    JOIN location l ON cy.location_id = l.location_id
    GROUP BY l.county_name, l.fips_code, c.name
    '''
    return pd.read_sql(query, engine)

# Dash app
app = dash.Dash(__name__, external_stylesheets=[
    "https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css"
])
app.title = "Missouri Crop Yields"

# App layout
app.layout = html.Div([
    html.H2("📊 Missouri Crop Yield Dashboard", className="text-center mt-4"),
    dcc.Dropdown(
        id="crop-select",
        options=[{"label": c, "value": c} for c in pd.read_sql("SELECT DISTINCT name FROM crop", engine)["name"]],
        value="Corn",
        className="mb-4"
    ),
    dcc.Graph(id="yield-graph"),
    dcc.Graph(id="yield-map")
], className="container")

# Callbacks
@app.callback(
    [dash.dependencies.Output("yield-graph", "figure"),
     dash.dependencies.Output("yield-map", "figure")],
    [dash.dependencies.Input("crop-select", "value")]
)
def update_graph(selected_crop):
    df = load_yield_data()
    df = df[df["crop"] == selected_crop]

    # Time series with dual y-axes
    fig1 = px.line(df, x="year", y="yield_per_acre", color="season", markers=True)
    fig1.update_traces(name="Yield (bu/acre)", yaxis="y1")

    for season in df["season"].unique():
        df_season = df[df["season"] == season]
        fig1.add_scatter(
            x=df_season["year"],
            y=df_season["avg_precipitation"],
            mode="lines+markers",
            name=f"{season} Precip (mm)",
            yaxis="y2"
        )

    fig1.update_layout(
        title=f"{selected_crop} Yield vs Precipitation",
        xaxis_title="Year",
        yaxis=dict(title="Yield (bu/acre)", side="left"),
        yaxis2=dict(title="Precipitation (mm)", overlaying="y", side="right"),
        legend=dict(x=1.05)
    )

    # Choropleth map
    df_map = load_map_data()
    df_map = df_map[df_map["crop"] == selected_crop]

    fig2 = px.choropleth(
        df_map,
        geojson="https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json",
        locations="fips_code",
        color="yield_per_acre",
        scope="usa",
        title=f"{selected_crop} Yield by County",
        color_continuous_scale="YlGn"
    )
    fig2.update_geos(fitbounds="locations", visible=False)

    return fig1, fig2

if __name__ == "__main__":
    app.run(debug=True)

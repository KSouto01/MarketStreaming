import dash
import duckdb
import pandas as pd
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, no_update
from datetime import datetime

# --- FORMATADORES ---
format_b3_js = {"function": "params.value && params.value !== 0 ? parseFloat(params.value).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '-'"}
format_usd_js = {"function": "params.value && params.value !== 0 ? parseFloat(params.value).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '-'"}

style_change = {
    "styleConditions": [
        {"condition": "params.value > 0", "style": {"color": "#00e676"}},
        {"condition": "params.value < 0", "style": {"color": "#ff5252"}},
        {"condition": "params.value == 0", "style": {"color": "#aaaaaa"}}
    ]
}

# --- DEFINIÇÃO DE COLUNAS (Sem width fixo, usando Flex) ---
def build_cols(is_usd=False):
    fmt = format_usd_js if is_usd else format_b3_js
    return [
        # Symbol fixo para não espremer o nome
        {"field": "symbol", "headerName": "Ativo", "minWidth": 90, "maxWidth": 110, "pinned": "left", "cellStyle": {'fontWeight': 'bold', 'color': '#fff'}},
        {"field": "Maturity", "headerName": "Vcto", "minWidth": 85},
        {"field": "Last", "headerName": "Último", "cellStyle": {'fontWeight': 'bold', 'color': '#4db6ac'}, "valueFormatter": fmt},
        {"field": "Change", "headerName": "Dif", "cellStyle": style_change, "valueFormatter": fmt},
        {"field": "PChange", "headerName": "%", "cellStyle": style_change, "valueFormatter": {"function": "params.value ? params.value.toFixed(2) + '%' : '-'"}},
        {"field": "High", "headerName": "Máx", "valueFormatter": fmt},
        {"field": "Low", "headerName": "Mín", "valueFormatter": fmt},
        {"field": "Volume", "headerName": "Vol"},
        {"field": "Time", "headerName": "Hora"}
    ]

grid_cbot_cols = build_cols(is_usd=True)
grid_b3_cols = build_cols(is_usd=False)

# --- OPÇÕES DE GRID (Auto-ajuste) ---
def get_grid_opts(cols):
    return {
        "defaultColDef": {
            "resizable": True, 
            "sortable": True, 
            "filter": True,
            "flex": 1,         # IMPORTANTE: Faz as colunas esticarem
            "minWidth": 70     # Garante que não fiquem ilegíveis
        }, 
        "columnDefs": cols, 
        "rowData": [], 
        "headerHeight": 32, 
        "rowHeight": 32,
        "suppressMovableColumns": True
    }

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Fazendão Streaming"

def create_table_card(title, grid_id, cols):
    return dbc.Card([
        dbc.CardHeader(title, className="fw-bold text-white bg-dark border-bottom border-secondary py-1", style={"fontSize": "0.95rem"}),
        dbc.CardBody(dag.AgGrid(
            id=grid_id, 
            className="ag-theme-balham-dark", 
            dashGridOptions=get_grid_opts(cols),
            columnSize="sizeToFit", # FORÇA O AJUSTE À TELA
            style={"height": "100%", "width": "100%"},
            rowData=[] 
        ), className="p-0 d-flex flex-column flex-grow-1")
    ], className="border-secondary shadow-sm d-flex flex-column h-100")

app.layout = dbc.Container([
    # Cabeçalho
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.Div([
                    html.H4("Fazendão - Streaming | Bolsa de Valores", className="mb-0 text-white fw-bold"),
                ], style={'display': 'flex', 'alignItems': 'center'}),
                html.Div([
                    html.Div([html.P("Status: Conectado", className="mb-0 text-end small text-success")], style={'marginRight': '20px'}),
                    html.Div([html.P("Última Atualização:", className="mb-0 text-center small text-white-50"), html.P(id="last-update-time", className="mb-0 text-center fw-bold text-info", style={'fontSize': '1.1rem'})], style={'backgroundColor': 'rgba(0,0,0,0.3)', 'padding': '5px 15px', 'borderRadius': '8px'})
                ], style={'display': 'flex', 'alignItems': 'center'})
            ], className="d-flex align-items-center justify-content-between p-2")
        ], style={'backgroundColor': '#0C5959', 'height': '70px'}), width=12)
    ], className="mb-2"),
    
    # Corpo
    html.Div([
        dbc.Row([
            # ESQUERDA: CBOT
            dbc.Col([
                html.Div(create_table_card("CBOT - Soja (USD)", "grid-soja", grid_cbot_cols), className="pb-1", style={"flex": "1"}),
                html.Div(create_table_card("CBOT - Milho (USD)", "grid-milho", grid_cbot_cols), className="pb-1", style={"flex": "1"}),
                html.Div(create_table_card("CBOT - Farelo (USD)", "grid-farelo", grid_cbot_cols), className="pb-1", style={"flex": "1"}),
                html.Div(create_table_card("CBOT - Óleo (USD)", "grid-oleo", grid_cbot_cols), style={"flex": "1"}),
            ], md=6, className="d-flex flex-column h-100 pe-1"),
            
            # DIREITA: B3
            dbc.Col([
                html.Div(create_table_card("B3 - Dólar Futuro (BRL)", "grid-dolar", grid_b3_cols), className="pb-1", style={"flex": "1"}),
                html.Div(create_table_card("B3 - Milho Futuro (BRL)", "grid-ccm", grid_b3_cols), className="pb-1", style={"flex": "1"}),
                html.Div(create_table_card("B3 - Boi Gordo (BRL)", "grid-boi", grid_b3_cols), className="pb-1", style={"flex": "1"}),
                html.Div(dbc.Card([
                    dbc.CardHeader("Gráfico Histórico - Dólar (3 Meses)", className="fw-bold text-white bg-dark border-bottom border-secondary py-1", style={"fontSize": "0.9rem"}),
                    dbc.CardBody(dcc.Graph(id="chart-dolar", style={"height": "100%"}), className="p-1 h-100")
                ], className="h-100 border-secondary shadow-sm"), style={"flex": "2"})
            ], md=6, className="d-flex flex-column h-100 ps-1"),
        ], style={"height": "100%"}),
    ], style={"height": "calc(95vh - 80px)", "display": "flex"}),
    
    dcc.Interval(id='interval-updater', interval=2000, n_intervals=0)
], fluid=True, style={"height": "100vh", "backgroundColor": "#000", "overflow": "hidden"})

@app.callback(
    [Output("grid-soja", "rowData"), Output("grid-milho", "rowData"), Output("grid-farelo", "rowData"), Output("grid-oleo", "rowData"),
     Output("grid-dolar", "rowData"), Output("grid-ccm", "rowData"), Output("grid-boi", "rowData"),
     Output("chart-dolar", "figure"), Output("last-update-time", "children")],
    [Input("interval-updater", "n_intervals")]
)
def update(n):
    try:
        conn = duckdb.connect('cma_data.duckdb', read_only=True)
        try: df = conn.execute("SELECT * FROM market_snapshot ORDER BY Maturity ASC, symbol ASC").fetchdf()
        except: df = pd.DataFrame()
        try: df_hist = conn.execute("SELECT * FROM market_history ORDER BY date_ref ASC").fetchdf()
        except: df_hist = pd.DataFrame()
        conn.close()
        
        column_mapping = {
            'last': 'Last', 'high': 'High', 'low': 'Low', 'open': 'Open',
            'change': 'Change', 'pchange': 'PChange', 'previous': 'Previous',
            'volume': 'Volume', 'time': 'Time', 'bid': 'Bid', 'ask': 'Ask',
            'maturity': 'Maturity', 'symbol': 'symbol', 'group_name': 'group_name', 'product_name': 'product_name'
        }
        
        if not df.empty:
            df.columns = [x.lower() for x in df.columns]
            df = df.rename(columns=column_mapping)
            df = df.fillna(0)
            df['Maturity'] = df['Maturity'].astype(str)
        
        def d(g, p): 
            if df.empty: return []
            return df[(df['group_name']==g) & (df['product_name']==p)].to_dict('records')

        fig = go.Figure()
        if not df_hist.empty:
            df_hist.columns = [x.lower() for x in df_hist.columns]
            symbol_atual = df_hist.iloc[0]['symbol'] if 'symbol' in df_hist.columns else 'Dolar'
            fig.add_trace(go.Candlestick(
                x=df_hist['date_ref'], open=df_hist['open'], high=df_hist['high'], low=df_hist['low'], close=df_hist['close'],
                increasing_line_color='#00e676', decreasing_line_color='#ff5252'
            ))
            fig.update_layout(
                template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=40, r=20, t=30, b=30), xaxis_rangeslider_visible=False,
                title=dict(text=f"Contrato: {symbol_atual}", font=dict(size=12, color='#ccc'))
            )
        else:
            fig.update_layout(template="plotly_dark", title="Carregando...", paper_bgcolor='rgba(0,0,0,0)')
        
        return (d('CBOT', 'Soja'), d('CBOT', 'Milho'), d('CBOT', 'Farelo'), d('CBOT', 'Oleo'),
                d('B3', 'Dolar'), d('B3', 'Milho'), d('B3', 'Boi'),
                fig, datetime.now().strftime('%H:%M:%S'))
    except Exception as e: 
        print(f"Erro App: {e}")
        return [no_update]*8 + ["Erro"]

if __name__ == "__main__":
    app.run(debug=True, port=8050)
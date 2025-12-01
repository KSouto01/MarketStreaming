import dash
import duckdb
import pandas as pd
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, no_update
from datetime import datetime
import os 

# --- CAMINHO ABSOLUTO ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_SOURCE = os.path.join(BASE_DIR, 'cma_data.duckdb')

# --- FORMATAÇÃO VETORIZADA ---
def format_currency_vec(series, decimals=2):
    return series.apply(lambda x: 
        '-' if pd.isna(x) or x == 0 else 
        f"{x:,.{decimals}f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    )

def format_pct_vec(series):
    return series.apply(lambda x: 
        '-' if pd.isna(x) else 
        f"{'+' if x > 0 else ''}{x:,.2f}%".replace('.', ',')
    )

# --- ESTILOS ---
style_bold = {'fontSize': '16px', 'fontWeight': 'bold', 'color': '#fff', 'display': 'flex', 'alignItems': 'center'}
style_last = {'fontSize': '16px', 'fontWeight': 'bold', 'color': '#4db6ac', 'display': 'flex', 'alignItems': 'center'}
style_base = {'fontSize': '16px', 'fontFamily': 'Roboto Mono', 'display': 'flex', 'alignItems': 'center'}

style_change = {
    "styleConditions": [
        {"condition": "params.data.Change_raw > 0", "style": {"color": "#00e676", "fontWeight": "bold", "fontSize": "16px"}},
        {"condition": "params.data.Change_raw < 0", "style": {"color": "#ff5252", "fontWeight": "bold", "fontSize": "16px"}},
        {"condition": "params.data.Change_raw == 0", "style": {"color": "#aaaaaa", "fontSize": "16px"}}
    ]
}

# --- COLUNAS ---
cols_def = [
    {"field": "symbol", "headerName": "Ativo", "minWidth": 110, "pinned": "left", "cellStyle": style_bold},
    {"field": "Last_fmt", "headerName": "Último", "cellStyle": style_last},
    {"field": "Change_fmt", "headerName": "Dif", "cellStyle": style_change},
    {"field": "PChange_fmt", "headerName": "%", "cellStyle": style_change},
    {"field": "High_fmt", "headerName": "Máx", "cellStyle": style_base},
    {"field": "Low_fmt", "headerName": "Mín", "cellStyle": style_base},
    {"field": "Time", "headerName": "Hora", "width": 90, "cellStyle": {"color": "#888", "fontSize": "13px", "display": "flex", "alignItems": "center"}},
    {"field": "Change_raw", "hide": True}
]

def get_grid_opts():
    return {
        "defaultColDef": {"resizable": True, "sortable": True, "flex": 1, "minWidth": 80}, 
        "columnDefs": cols_def, 
        "rowData": [], 
        "headerHeight": 35, "rowHeight": 35, 
        "suppressMovableColumns": True
    }

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Fazendão Streaming"

def create_card(title, grid_id):
    return dbc.Card([
        dbc.CardHeader(title, className="fw-bold text-white border-bottom border-secondary py-1 ps-3", style={"backgroundColor": "black", "fontSize": "0.95rem"}),
        dbc.CardBody(dag.AgGrid(
            id=grid_id, className="ag-theme-balham-dark", dashGridOptions=get_grid_opts(),
            columnSize="sizeToFit", style={"height": "100%", "width": "100%"}, rowData=[]
        ), className="p-0 d-flex flex-column flex-grow-1")
    ], className="h-100 border-secondary shadow-sm")

app.layout = dbc.Container([
    # HEADER PERSONALIZADO
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                # Lado Esquerdo: Título + Assinatura Equipe
                html.Div([
                    html.H4("MONITORAMENTO DE MERCADO", className="mb-0 fw-bold", style={'color': '#66bb6a', 'fontSize': '1.4rem'}),
                    # Nova Frase Solicitada
                    html.Span("Equipe TI | Klaus Maya Souto", className="ms-3", style={'color': '#ffffff', 'fontSize': '0.90rem', 'opacity': '0.7', 'letterSpacing': '0.5px'})
                ], style={'display': 'flex', 'alignItems': 'baseline'}), # baseline alinha o texto pequeno com a base do grande

                # Lado Direito: Status e Hora (Agora Brancos)
                html.Div([
                    html.Span("CONECTADO", className="fw-bold me-3", style={'color': '#ffffff', 'fontSize': '0.9rem'}),
                    html.Span(id="last-update-time", className="fw-bold", style={'fontFamily': 'monospace', 'fontSize': '1.1rem', 'color': '#ffffff'})
                ], className="d-flex align-items-center")
            ], className="d-flex align-items-center justify-content-between p-2")
        ], style={'backgroundColor': '#000', 'height': '70px', 'borderBottom': '1px solid #333'}), width=12)
    ], className="mb-2"),
    
    # BODY
    html.Div([
        dbc.Row([
            dbc.Col([
                html.Div(create_card("CBOT - Soja (USD/bu)", "grid-soja"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("CBOT - Milho (USD/bu)", "grid-milho"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("CBOT - Farelo (USD/st)", "grid-farelo"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("CBOT - Óleo (USD/lb)", "grid-oleo"), style={"flex": "1"}),
            ], md=6, className="d-flex flex-column h-100 pe-1"),
            dbc.Col([
                html.Div(create_card("B3 - Dólar Futuro (BRL)", "grid-dolar"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("B3 - Milho Futuro (BRL)", "grid-ccm"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("B3 - Boi Gordo (BRL)", "grid-boi"), className="pb-1", style={"flex": "1"}),
                html.Div(dbc.Card([
                    dbc.CardHeader("CURVA DO DÓLAR (90 Dias)", className="fw-bold text-white border-bottom border-secondary py-1 ps-3", style={"backgroundColor": "black", "fontSize": "0.95rem"}),
                    dbc.CardBody(dcc.Graph(id="chart-dolar", style={"height": "100%"}), className="p-1 h-100")
                ], className="h-100 border-secondary shadow-sm"), style={"flex": "2"})
            ], md=6, className="d-flex flex-column h-100 ps-1"),
        ], style={"height": "100%", "width": "100%", "margin": "0"}),
    ], style={"height": "calc(95vh - 80px)", "display": "flex", "width": "100%"}),
    
    dcc.Interval(id='interval-updater', interval=2000, n_intervals=0)
], fluid=True, style={"height": "100vh", "backgroundColor": "#111", "overflow": "hidden"})

@app.callback(
    [Output("grid-soja", "rowData"), Output("grid-milho", "rowData"), Output("grid-farelo", "rowData"), Output("grid-oleo", "rowData"),
     Output("grid-dolar", "rowData"), Output("grid-ccm", "rowData"), Output("grid-boi", "rowData"),
     Output("chart-dolar", "figure"), Output("last-update-time", "children")],
    [Input("interval-updater", "n_intervals")]
)
def update(n):
    try:
        conn = duckdb.connect(DB_SOURCE, read_only=True)
        try: df = conn.execute("SELECT * FROM market_snapshot ORDER BY Maturity ASC, symbol ASC").fetchdf()
        except: df = pd.DataFrame()
        try: df_hist = conn.execute("SELECT * FROM market_history ORDER BY date_ref ASC").fetchdf()
        except: df_hist = pd.DataFrame()
        conn.close()
        
        col_map = {
            'last': 'Last', 'high': 'High', 'low': 'Low', 'open': 'Open',
            'change': 'Change', 'pchange': 'PChange', 'previous': 'Previous',
            'volume': 'Volume', 'time': 'Time', 'bid': 'Bid', 'ask': 'Ask',
            'maturity': 'Maturity', 'symbol': 'symbol', 'group_name': 'group_name', 'product_name': 'product_name'
        }
        
        if not df.empty:
            df.columns = [x.lower() for x in df.columns]
            df = df.rename(columns=col_map)
            df = df.fillna(0)
            
            cols_num = ['Last', 'High', 'Low', 'Open', 'Change', 'PChange']
            for col in cols_num:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            mask_dolar = df['product_name'].str.lower() == 'dolar'
            cols_div = ['Last', 'High', 'Low', 'Change']
            if mask_dolar.any():
                df.loc[mask_dolar, cols_div] = df.loc[mask_dolar, cols_div] / 1000.0

            if mask_dolar.any():
                for col in cols_div:
                    df.loc[mask_dolar, f"{col}_fmt"] = format_currency_vec(df.loc[mask_dolar, col], 4)
            
            if (~mask_dolar).any():
                for col in cols_div:
                    df.loc[~mask_dolar, f"{col}_fmt"] = format_currency_vec(df.loc[~mask_dolar, col], 2)
            
            df['PChange_fmt'] = format_pct_vec(df['PChange'])
            df['Change_raw'] = df['Change']

        def d(g, p): return df[(df['group_name']==g) & (df['product_name']==p)].to_dict('records') if not df.empty else []

        fig = go.Figure()
        if not df_hist.empty:
            df_hist.columns = [x.lower() for x in df_hist.columns]
            sym = df_hist.iloc[0]['symbol'] if 'symbol' in df_hist.columns else 'Dolar'
            
            if 'dol' in sym.lower():
                for col in ['open', 'max', 'min', 'close']:
                    if col in df_hist.columns:
                        df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce').fillna(0) / 1000.0

            fig.add_trace(go.Candlestick(
                x=df_hist['date_ref'], 
                open=df_hist['open'], high=df_hist.get('max', df_hist['open']),
                low=df_hist.get('min', df_hist['open']), close=df_hist['close'],
                increasing_line_color='#00e676', increasing_fillcolor='#00e676', 
                decreasing_line_color='#ff1744', decreasing_fillcolor='#ff1744',
                xhoverformat="%d/%m/%Y", yhoverformat=',.4f'
            ))
            fig.update_layout(
                template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                margin=dict(l=20, r=55, t=30, b=30), xaxis_rangeslider_visible=False,
                yaxis=dict(side='right', tickformat=",", showgrid=True, gridcolor='#333'),
                separators=',.', 
                title=dict(text=f"CONTRATO: {sym}", font=dict(size=14, color='#ccc'))
            )
        else:
            fig.update_layout(template="plotly_dark", title="Aguardando dados...", paper_bgcolor='rgba(0,0,0,0)')
        
        return (d('CBOT', 'Soja'), d('CBOT', 'Milho'), d('CBOT', 'Farelo'), d('CBOT', 'Oleo'),
                d('B3', 'Dolar'), d('B3', 'Milho'), d('B3', 'Boi'),
                fig, datetime.now().strftime('%H:%M:%S'))
    except Exception as e: 
        print(f"Erro App: {e}")
        return [no_update]*8 + ["Reconectando..."]

if __name__ == "__main__":
    app.run(debug=False, port=8050, host='0.0.0.0')
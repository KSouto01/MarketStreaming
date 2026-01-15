import dash
import duckdb
import pandas as pd
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, State, no_update, ctx
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import os 
import time
import shutil
import logging

# --- CONFIGURAÇÃO ---
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_REALTIME = os.path.join(BASE_DIR, 'cma_realtime.duckdb')
DB_HISTORY = os.path.join(BASE_DIR, 'cma_history.duckdb')
CONFIG_FILE = os.path.join(BASE_DIR, 'config.json')
CHART_FILE = os.path.join(BASE_DIR, 'active_chart.json')
SERVER_START_TIME = str(time.time())

# MAPA DE MESES (IGUAL AO ROBÔ)
MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

def safe_read_db(db_path, query):
    if os.name == 'nt':
        shadow_path = db_path.replace('.duckdb', '_shadow.duckdb')
        try:
            if os.path.exists(db_path):
                shutil.copy2(db_path, shadow_path)
                conn = duckdb.connect(shadow_path, read_only=True)
                df = conn.execute(query).fetchdf()
                conn.close()
                return df
        except: time.sleep(0.1)
        return pd.DataFrame()
    else:
        for _ in range(5):
            try:
                conn = duckdb.connect(db_path, read_only=True)
                df = conn.execute(query).fetchdf()
                conn.close()
                return df
            except: time.sleep(0.05)
        return pd.DataFrame()

def load_config():
    try:
        with open(CONFIG_FILE, 'r') as f: return json.load(f)
    except: return {}

def get_source_id_for_product(product_name):
    config = load_config()
    for group, items in config.items():
        for item in items:
            if item.get('name') == product_name: return str(item.get('source', '57'))
    return "57"

def save_active_chart(symbol, product_name):
    try:
        source_id = get_source_id_for_product(product_name)
        current = {}
        if os.path.exists(CHART_FILE):
            with open(CHART_FILE, 'r') as f: current = json.load(f)
        if current.get('symbol') != symbol or current.get('sourceId') != source_id:
            with open(CHART_FILE, 'w') as f: json.dump({"symbol": symbol, "sourceId": source_id}, f)
    except: pass

def format_currency_vec(series, decimals=2):
    s_num = pd.to_numeric(series, errors='coerce').fillna(0)
    return s_num.apply(lambda x: '-' if x == 0 else f"{x:,.{decimals}f}".replace(',', 'X').replace('.', ',').replace('X', '.'))

def format_pct_vec(series):
    s_num = pd.to_numeric(series, errors='coerce').fillna(0)
    return s_num.apply(lambda x: '-' if x == 0 else f"{'+' if x > 0 else ''}{x:,.2f}%".replace('.', ','))

style_bold = {'fontSize': '15px', 'fontWeight': 'bold', 'color': '#fff', 'display': 'flex', 'alignItems': 'center'}
style_last = {'fontSize': '15px', 'fontWeight': 'bold', 'color': '#ffffff', 'display': 'flex', 'alignItems': 'center'}
style_base = {'fontSize': '15px', 'fontFamily': 'Roboto Mono', 'display': 'flex', 'alignItems': 'center'}
style_change = {
    "styleConditions": [
        {"condition": "params.data.Change_raw > 0", "style": {"color": "#00e676", "fontWeight": "bold", "fontSize": "15px"}},
        {"condition": "params.data.Change_raw < 0", "style": {"color": "#ff5252", "fontWeight": "bold", "fontSize": "15px"}},
        {"condition": "params.data.Change_raw == 0", "style": {"color": "#aaaaaa", "fontSize": "15px"}}
    ]
}

cols_def = [
    {"field": "symbol", "headerName": "SYMBOL", "minWidth": 80, "pinned": "left", "cellStyle": style_bold},
    {"field": "Last_fmt", "headerName": "LAST", "cellStyle": style_last, "minWidth": 55},
    {"field": "Change_fmt", "headerName": "CHG", "cellStyle": style_change, "minWidth": 55},
    {"field": "PChange_fmt", "headerName": "%", "cellStyle": style_change, "minWidth": 55},
    {"field": "High_fmt", "headerName": "HIGH", "cellStyle": style_base, "minWidth": 55},
    {"field": "Low_fmt", "headerName": "LOW", "cellStyle": style_base, "minWidth": 55},
    {"field": "Change_raw", "hide": True}
]

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG, "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css"], suppress_callback_exceptions=True)
app.title = "Fazendão Streaming"

def create_card(title, grid_id):
    return dbc.Card([
        dbc.CardHeader(title, className="fw-bold text-white border-bottom border-secondary py-1 ps-2", style={"backgroundColor": "black", "fontSize": "0.85rem", "letterSpacing": "1px"}),
        dbc.CardBody(dag.AgGrid(id=grid_id, className="ag-theme-balham-dark", getRowId="params.data.symbol", dashGridOptions={"defaultColDef": {"resizable": True, "sortable": True, "flex": 1, "minWidth": 55, "enableCellChangeFlash": True}, "columnDefs": cols_def, "headerHeight": 32, "rowHeight": 32, "suppressMovableColumns": True}, columnSize="sizeToFit", style={"height": "100%", "width": "100%"}, rowData=[]), className="p-0 d-flex flex-column flex-grow-1")
    ], className="h-100 shadow-sm")

filter_modal = dbc.Modal([
    dbc.ModalHeader(dbc.ModalTitle("Chart Filter"), close_button=True),
    dbc.ModalBody([
        dbc.Row([
            dbc.Col([dbc.Label("Exchange"), dcc.Dropdown(id="filter-exchange", options=[{'label': 'B3', 'value': 'B3'}, {'label': 'CBOT', 'value': 'CBOT'}], placeholder="Select...", className="text-dark")]),
            dbc.Col([dbc.Label("Product"), dcc.Dropdown(id="filter-product", options=[], placeholder="Select...", className="text-dark")])
        ], className="mb-3"),
        dbc.Row([
            dbc.Col([dbc.Label("Symbol"), dcc.Dropdown(id="filter-symbol", options=[], placeholder="Select contract...", className="text-dark", style={'width': '100%'})], width=12),
        ])
    ]),
    dbc.ModalFooter([html.Div(id="filter-error-msg", className="text-danger me-auto small"), dbc.Button("Apply Filter", id="apply-filter", color="primary", n_clicks=0)])
], id="modal-filter", is_open=False, centered=True)

app.layout = dbc.Container([
    dcc.Store(id='client-version-store', data=SERVER_START_TIME),
    dcc.Interval(id='version-checker', interval=10*1000, n_intervals=0),
    dcc.Location(id='page-reloader', refresh=True),
    dbc.Row([dbc.Col(dbc.Card([dbc.CardBody([
        html.Div([html.H4("MARKET DASHBOARD", className="mb-0 fw-bold", style={'color': '#66bb6a', 'fontSize': '1.2rem', 'letterSpacing': '1px'}),
                  html.Div([html.Span("Dev: Klaus Maya Souto", className="ms-3", style={'color': '#ccc', 'fontSize': '0.9rem', 'fontWeight': 'bold'}), html.Span(" | IT Team Fazendão", style={'color': '#ccc', 'fontSize': '0.9rem'})])], style={'display': 'flex', 'alignItems': 'baseline'}), 
        html.Div([html.Span("Streaming", className="fw-bold me-3", style={'color': '#fff', 'fontSize': '0.8rem', 'textTransform': 'uppercase', 'letterSpacing': '1px'}), html.Span(id="server-clock", className="fw-bold", style={'fontFamily': 'monospace', 'fontSize': '1rem', 'color': '#fff'})], className="d-flex align-items-center")
    ], className="d-flex align-items-center justify-content-between p-1")], style={'backgroundColor': '#000', 'height': '50px', 'borderBottom': '1px solid #333'}), width=12)], className="mb-1"),
    html.Div([
        dbc.Row([
            dbc.Col([html.Div(create_card("CBOT - Soybean (USD/bu)", "grid-soja"), className="pb-1", style={"flex": "1"}), html.Div(create_card("CBOT - Meal (USD/st)", "grid-farelo"), className="pb-1", style={"flex": "1"}), html.Div(create_card("CBOT - Oil (USD/lb)", "grid-oleo"), style={"flex": "1"})], width=4, className="d-flex flex-column h-100 pe-1"),
            dbc.Col([html.Div(create_card("CBOT - Corn (USD/bu)", "grid-milho"), className="pb-1", style={"flex": "1"}), html.Div(create_card("B3 - Corn (BRL)", "grid-ccm"), className="pb-1", style={"flex": "1"}), html.Div(create_card("B3 - Live Cattle (BRL)", "grid-boi"), style={"flex": "1"})], width=4, className="d-flex flex-column h-100 px-1"),
            dbc.Col([html.Div(create_card("B3 - Dollar Future (BRL)", "grid-dolar"), className="pb-1", style={"flex": "1"}), html.Div(create_card("Spot Rate (BRL)", "grid-spot"), className="pb-1", style={"flex": "1"}), html.Div(dbc.Card([dbc.CardHeader([html.Span("CHART ANALYSIS (30D)", className="fw-bold text-white", style={"fontSize": "0.85rem"}), dbc.Button(html.I(className="fa-solid fa-filter"), id="open-filter", color="light", size="sm", className="bg-transparent border-0 text-white")], className="d-flex justify-content-between align-items-center bg-black border-bottom border-secondary py-1 ps-2 pe-2"), dbc.CardBody(dcc.Graph(id="chart-main", style={"height": "100%"}, config={'displayModeBar': False, 'displaylogo': False, 'showTips': False, 'modeBarButtonsToRemove': ['toImage', 'sendDataToCloud', 'editInChartStudio']}), className="p-1 h-100")], className="h-100 border-secondary shadow-sm"), style={"flex": "1"})], width=4, className="d-flex flex-column h-100 ps-1"),
        ], style={"height": "100%", "width": "100%", "margin": "0"}),
    ], style={"height": "calc(98vh - 60px)", "display": "flex", "width": "100%"}),
    filter_modal, dcc.Interval(id='interval-main', interval=1000, n_intervals=0), dcc.Store(id='current-filter', data={}) 
], fluid=True, style={"height": "100vh", "backgroundColor": "#111", "overflow": "hidden", "padding": "5px"})

@app.callback(Output('page-reloader', 'href'), Input('version-checker', 'n_intervals'), State('client-version-store', 'data'))
def check_version(n, client_version): return "/" if client_version != SERVER_START_TIME else no_update

@app.callback(
    [Output("grid-soja", "rowData"), Output("grid-milho", "rowData"), Output("grid-farelo", "rowData"), Output("grid-oleo", "rowData"),
     Output("grid-dolar", "rowData"), Output("grid-ccm", "rowData"), Output("grid-boi", "rowData"),
     Output("grid-spot", "rowData"), Output("chart-main", "figure"), Output("server-clock", "children"),
     Output("filter-product", "options"), Output("filter-symbol", "options"),
     Output("modal-filter", "is_open"), Output("filter-error-msg", "children"), Output("current-filter", "data")],
    [Input("interval-main", "n_intervals"), Input("open-filter", "n_clicks"), Input("apply-filter", "n_clicks")],
    [State("modal-filter", "is_open"), State("filter-exchange", "value"), State("filter-product", "value"), State("filter-symbol", "value"), State("current-filter", "data")]
)
def update_all(n, n_open, n_apply, is_open, sel_exchange, sel_prod, sel_symbol, current_filter_data):
    trigger = ctx.triggered_id
    if trigger == 'open-filter': return [no_update]*12 + [True, "", no_update]
    if trigger == 'apply-filter':
        if not all([sel_exchange, sel_prod, sel_symbol]): return [no_update]*12 + [True, "Preencha all fields!", no_update]
        save_active_chart(sel_symbol, sel_prod)
        current_filter_data = {'symbol': sel_symbol, 'product': sel_prod}
        is_open = False

    try:
        df = safe_read_db(DB_REALTIME, "SELECT * FROM market_snapshot")
        df_hist = safe_read_db(DB_HISTORY, "SELECT * FROM market_history ORDER BY date_ref ASC")
        if df.empty: return [no_update]*15

        df['Maturity_dt'] = pd.to_datetime(df['Maturity'], errors='coerce')
        df = df.sort_values(by=['group_name', 'product_name', 'Maturity_dt'], ascending=[True, True, True])
        
        col_map = {'last':'Last','high':'High','low':'Low','open':'Open','change':'Change','pchange':'PChange','previous':'Previous','volume':'Volume','time':'Time','bid':'Bid','ask':'Ask','maturity':'Maturity','symbol':'symbol','group_name':'group_name','product_name':'product_name'}
        df.columns = [x.lower() for x in df.columns]
        df = df.rename(columns=col_map)
        df = df.fillna(0)
        df['product_name'] = df['product_name'].astype(str).str.strip()
        df['group_name'] = df['group_name'].astype(str).str.strip()
        cols_num = ['Last', 'High', 'Low', 'Open', 'Change', 'PChange']
        for col in cols_num: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        mask_dolar = df['product_name'].str.lower() == 'dolar'
        mask_dolar_com = df['product_name'].str.lower() == 'dolar comercial'
        if mask_dolar.any(): df.loc[mask_dolar, ['Last', 'High', 'Low', 'Change']] /= 1000.0
        
        for c in ['Last', 'High', 'Low', 'Change']: df[f"{c}_fmt"] = format_currency_vec(df[c], 2)
        if mask_dolar.any():
            for c in ['Last', 'High', 'Low', 'Change']: df.loc[mask_dolar, f"{c}_fmt"] = format_currency_vec(df.loc[mask_dolar, c], 4)
        if mask_dolar_com.any():
            for c in ['Last', 'High', 'Low', 'Change']: df.loc[mask_dolar_com, f"{c}_fmt"] = format_currency_vec(df.loc[mask_dolar_com, c], 4)

        df['PChange_fmt'] = format_pct_vec(df['PChange'])
        df['Change_raw'] = df['Change']

        def d(g, p): 
            if df.empty: return []
            filtered = df[(df['group_name'].str.lower() == g.lower()) & (df['product_name'].str.lower() == p.lower())]
            return filtered.to_dict('records')

        fig = go.Figure()
        prods, symbols = [], []
        
        if not df.empty:
            if sel_exchange: prods = sorted(df[df['group_name'].str.upper() == sel_exchange]['product_name'].unique())
            else: prods = sorted(df['product_name'].unique())
            if sel_prod: symbols = sorted(df[df['product_name'] == sel_prod]['symbol'].unique())

            target_symbol = current_filter_data.get('symbol')
            target_prod_name = current_filter_data.get('product') or "Dólar"
            
            # --- LÓGICA DE CÁLCULO DE TARGET (MATH LOGIC) ---
            if not target_symbol:
                today = datetime.now()
                # Calcula exatamente quem é o contrato do próximo mês
                target_date = today + relativedelta(months=1)
                if today.day > 25: target_date = today + relativedelta(months=2) # Pula se estiver no fim do mês
                
                m_char = MONTH_CODES[target_date.month]
                y_str = target_date.strftime('%y')
                target_symbol = f"DOL{m_char}{y_str}"

            if not df_hist.empty and target_symbol:
                hist_data = df_hist[df_hist['symbol'] == target_symbol].copy()
                if not hist_data.empty:
                    hist_data.columns = [x.lower() for x in hist_data.columns]
                    rename_h = {'max': 'high', 'min': 'low'}
                    hist_data = hist_data.rename(columns=rename_h)
                    if 'dol' in target_symbol.lower():
                        for c in ['open', 'high', 'low', 'close']: 
                             if c in hist_data.columns: hist_data[c] = pd.to_numeric(hist_data[c], errors='coerce').fillna(0) / 1000.0
                    
                    live_row = df[df['symbol'] == target_symbol]
                    if not live_row.empty:
                        last_real = live_row.iloc[0]['Last']
                        open_real = live_row.iloc[0]['Open']
                        high_real = live_row.iloc[0]['High']
                        low_real = live_row.iloc[0]['Low']
                        if open_real == 0 and last_real > 0: open_real = high_real = low_real = last_real
                        if last_real > 0:
                            today_str = datetime.now().strftime('%Y-%m-%d')
                            last_hist_date = str(hist_data.iloc[-1]['date_ref'])
                            if last_hist_date == today_str:
                                idx = hist_data.index[-1]
                                hist_data.at[idx, 'close'] = last_real
                                hist_data.at[idx, 'high'] = max(hist_data.at[idx, 'high'], high_real)
                                hist_data.at[idx, 'low'] = min(hist_data.at[idx, 'low'], low_real)
                            else:
                                new_df = pd.DataFrame([{'symbol': target_symbol, 'date_ref': today_str, 'open': open_real, 'high': high_real, 'low': low_real, 'close': last_real}])
                                hist_data = pd.concat([hist_data, new_df], ignore_index=True)

                    last_c = hist_data.iloc[-1]['close']
                    prev_c = hist_data.iloc[-2]['close'] if len(hist_data) > 1 else last_c
                    line_color = '#00e676' if last_c >= prev_c else '#ff5252'

                    fig.add_trace(go.Candlestick(x=hist_data['date_ref'], open=hist_data['open'], high=hist_data['high'], low=hist_data['low'], close=hist_data['close'], increasing_line_color='#00e676', decreasing_line_color='#ff1744', xhoverformat="%d/%m/%Y", yhoverformat=',.4f'))
                    fig.add_hline(y=last_c, line_dash="dot", line_color=line_color, annotation_text=f"<b> {last_c:,.4f} </b>".replace('.', ','), annotation_position="right", annotation_bgcolor=line_color, annotation_font_color="white", annotation_font=dict(size=18, family="Roboto Mono"), annotation_borderpad=6)
                    fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', margin=dict(l=10, r=60, t=30, b=20), xaxis_rangeslider_visible=False, yaxis=dict(side='right', tickformat=".4f", showgrid=True, gridcolor='#333'), separators=',.', title=dict(text=f"{target_prod_name} - {target_symbol}", font=dict(size=14, color='#ccc')), uirevision=target_symbol)
                else: fig.update_layout(template="plotly_dark", title=f"History Empty: {target_symbol}", paper_bgcolor='rgba(0,0,0,0)')
            else: fig.update_layout(template="plotly_dark", title="Waiting Data...", paper_bgcolor='rgba(0,0,0,0)')
        else: fig.update_layout(template="plotly_dark", title="Starting...", paper_bgcolor='rgba(0,0,0,0)')

        return (d('CBOT', 'Soja'), d('CBOT', 'Milho'), d('CBOT', 'Farelo'), d('CBOT', 'Oleo'),
                d('B3', 'Dolar'), d('B3', 'Milho'), d('B3', 'Boi'),
                d('ECONOMIA', 'Dolar Comercial'),
                fig, datetime.now().strftime('%d/%m/%Y %H:%M:%S'), prods, symbols, is_open, "", current_filter_data)
    except Exception as e: return [no_update]*15

if __name__ == "__main__":
    app.run(debug=False, port=8050, host='0.0.0.0')
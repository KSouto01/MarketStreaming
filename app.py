import dash
import duckdb
import pandas as pd
import dash_ag_grid as dag
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import dcc, html, Input, Output, State, no_update, ctx
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import os 
import time
import shutil
import logging
import traceback

# --- CONFIGURAÇÃO ---
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_REALTIME = os.path.join(BASE_DIR, 'cma_realtime.duckdb')
DB_HISTORY = os.path.join(BASE_DIR, 'cma_history.duckdb')
CONFIG_FILE = os.path.join(BASE_DIR, 'config.json')
CHART_FILE = os.path.join(BASE_DIR, 'active_chart.json')
SERVER_START_TIME = str(time.time())

MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

# --- CACHE GLOBAL PARA DROPDOWNS (PERFORMANCE EXTREMA) ---
METADATA_CACHE = {"last_update": 0, "df": pd.DataFrame()}

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

# Função otimizada para carregar lista de ativos (Cache de 60s)
def get_cached_metadata():
    now = time.time()
    # Se o cache tem mais de 60 segundos ou está vazio, recarrega do disco
    if now - METADATA_CACHE["last_update"] > 60 or METADATA_CACHE["df"].empty:
        try:
            df = safe_read_db(DB_REALTIME, "SELECT DISTINCT group_name, product_name, symbol FROM market_snapshot")
            if not df.empty:
                df['group_name'] = df['group_name'].str.strip().str.upper()
                df['product_name'] = df['product_name'].str.strip()
                METADATA_CACHE["df"] = df
                METADATA_CACHE["last_update"] = now
        except: pass
    return METADATA_CACHE["df"]

def load_config():
    try:
        with open(CONFIG_FILE, 'r') as f: return json.load(f)
    except: return {}

def get_source_id_for_product(product_name):
    try:
        config = load_config()
        for group, items in config.items():
            for item in items:
                if item.get('name') == product_name: return str(item.get('source', '57'))
    except: pass
    return "57"

def save_active_chart(symbol, product_name, period="1M"):
    try:
        source_id = get_source_id_for_product(product_name)
        new_data = {"symbol": symbol, "sourceId": source_id, "product": product_name, "period": period}
        
        if os.path.exists(CHART_FILE):
            try:
                with open(CHART_FILE, 'r') as f: 
                    current = json.load(f)
                    if not symbol: new_data['symbol'] = current.get('symbol')
                    if not product_name: new_data['product'] = current.get('product')
            except: pass

        with open(CHART_FILE, 'w') as f: 
            json.dump(new_data, f)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        print(f"❌ Erro Save: {e}")

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
        dbc.CardBody(dag.AgGrid(
            id=grid_id, className="ag-theme-balham-dark", getRowId="params.data.symbol", 
            dashGridOptions={"defaultColDef": {"resizable": True, "sortable": True, "flex": 1, "minWidth": 55, "enableCellChangeFlash": True}, "columnDefs": cols_def, "headerHeight": 32, "rowHeight": 32, "suppressMovableColumns": True}, 
            columnSize="sizeToFit", style={"height": "100%", "width": "100%"}, rowData=[]
        ), className="p-0 d-flex flex-column flex-grow-1")
    ], className="h-100 shadow-sm")

filter_modal = dbc.Modal([
    dbc.ModalHeader(dbc.ModalTitle("Filtrar Gráfico"), close_button=True),
    dbc.ModalBody([
        dbc.Row([
            dbc.Col([dbc.Label("Bolsa"), dcc.Dropdown(id="filter-exchange", options=[{'label': 'B3', 'value': 'B3'}, {'label': 'CBOT', 'value': 'CBOT'}], placeholder="Selecione...", className="text-dark")]),
            dbc.Col([dbc.Label("Produto"), dcc.Dropdown(id="filter-product", options=[], placeholder="Selecione...", className="text-dark")])
        ], className="mb-3"),
        dbc.Row([
            dbc.Col([dbc.Label("Ativo (Símbolo)"), dcc.Dropdown(id="filter-symbol", options=[], placeholder="Selecione...", className="text-dark", style={'width': '100%'})], width=12),
        ])
    ]),
    dbc.ModalFooter([
        html.Div(id="filter-error-msg", className="text-danger me-auto small"),
        dbc.Button("Aplicar", id="apply-filter", color="primary", n_clicks=0)
    ])
], id="modal-filter", is_open=False, centered=True)

app.layout = dbc.Container([
    dcc.Store(id='client-version-store', data=SERVER_START_TIME),
    dcc.Store(id='current-active-symbol', data={'symbol': None, 'product': None}),
    dcc.Store(id='chart-range-store', data="1M"), 
    dcc.Interval(id='version-checker', interval=10*1000, n_intervals=0),
    dcc.Location(id='page-reloader', refresh=True),
    
    dbc.Row([dbc.Col(dbc.Card([dbc.CardBody([
        html.Div([html.H4("MARKET DASHBOARD", className="mb-0 fw-bold", style={'color': '#66bb6a', 'fontSize': '1.2rem', 'letterSpacing': '1px'}),
                  html.Div([html.Span("Dev: Klaus Maya Souto", className="ms-3", style={'color': '#ccc', 'fontSize': '0.9rem', 'fontWeight': 'bold'}), html.Span(" | IT Team Fazendão", style={'color': '#ccc', 'fontSize': '0.9rem'})])], style={'display': 'flex', 'alignItems': 'baseline'}), 
        html.Div([html.Span("Streaming", className="fw-bold me-3", style={'color': '#fff', 'fontSize': '0.8rem', 'textTransform': 'uppercase', 'letterSpacing': '1px'}), html.Span(id="server-clock", className="fw-bold", style={'fontFamily': 'monospace', 'fontSize': '1rem', 'color': '#fff'})], className="d-flex align-items-center")
    ], className="d-flex align-items-center justify-content-between p-1")], style={'backgroundColor': '#000', 'height': '50px', 'borderBottom': '1px solid #333'}), width=12)], className="mb-1"),
    
    html.Div([
        dbc.Row([
            # --- TÍTULOS ATUALIZADOS AQUI ---
            dbc.Col([
                html.Div(create_card("Soybeans - USD/bu [CBOT]", "grid-soja"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("Meal - USD/st [CBOT]", "grid-farelo"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("Soybean Oil - USD/lb [CBOT]", "grid-oleo"), style={"flex": "1"})
            ], width=4, className="d-flex flex-column h-100 pe-1"),
            
            dbc.Col([
                html.Div(create_card("Corn - USD/bu [CBOT]", "grid-milho"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("Corn - BRL [B³]", "grid-ccm"), className="pb-1", style={"flex": "1"}),
                html.Div(create_card("Live Cattle - BRL [B³]", "grid-boi"), style={"flex": "1"})
            ], width=4, className="d-flex flex-column h-100 px-1"),
            
            dbc.Col([
                html.Div(dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.Span(id="chart-title", children="CHART ANALYSIS", className="fw-bold text-white text-nowrap me-auto", style={"fontSize": "0.85rem", "overflow": "hidden", "textOverflow": "ellipsis"}),
                            html.Div([
                                dbc.ButtonGroup([
                                    dbc.Button("1M", id="btn-1m", color="secondary", outline=True, size="sm", className="px-2 py-0", style={"fontSize": "0.75rem"}),
                                    dbc.Button("3M", id="btn-3m", color="secondary", outline=True, size="sm", className="px-2 py-0", style={"fontSize": "0.75rem"}),
                                    dbc.Button("6M", id="btn-6m", color="secondary", outline=True, size="sm", className="px-2 py-0", style={"fontSize": "0.75rem"}),
                                    dbc.Button("1Y", id="btn-1y", color="secondary", outline=True, size="sm", className="px-2 py-0", style={"fontSize": "0.75rem"}),
                                ], className="me-2"),
                                dbc.Button(html.I(className="fa-solid fa-filter"), id="open-filter", color="light", size="sm", className="bg-transparent border-0 text-white p-0")
                            ], className="d-flex align-items-center gap-2")
                        ], className="d-flex align-items-center w-100 justify-content-between")
                    ], className="bg-black border-bottom border-secondary py-1 ps-2 pe-2", style={"height": "35px"}),
                    
                    dbc.CardBody(
                        html.Div(id="chart-container", className="h-100 w-100 d-flex justify-content-center align-items-center"), 
                        className="p-1 h-100",
                        style={"overflow": "hidden"}
                    )
                ], className="h-100 border-secondary shadow-sm"), style={"flex": "1", "minHeight": "0", "marginBottom": "4px"}),
                
                html.Div(create_card("Dolar Spot - BRL", "grid-spot"), className="pb-1", style={"height": "15vh", "flex": "none"}),
                html.Div(create_card("Dollar Future - BRL [B³]", "grid-dolar"), style={"height": "25vh", "flex": "none"}),
            ], width=4, className="d-flex flex-column h-100 ps-1"),
        ], style={"height": "100%", "width": "100%", "margin": "0"}),
    ], style={"height": "calc(98vh - 60px)", "display": "flex", "width": "100%"}),
    filter_modal,
    dcc.Interval(id='interval-main', interval=1000, n_intervals=0) 
], fluid=True, style={"height": "100vh", "backgroundColor": "#111", "overflow": "hidden", "padding": "5px"})

@app.callback(Output('page-reloader', 'href'), Input('version-checker', 'n_intervals'), State('client-version-store', 'data'))
def check_version(n, client_version): return "/" if client_version != SERVER_START_TIME else no_update

@app.callback(
    [Output("modal-filter", "is_open"), Output("filter-product", "options"), Output("filter-symbol", "options"), 
     Output("current-active-symbol", "data"), Output("chart-range-store", "data"),
     Output("btn-1m", "active"), Output("btn-3m", "active"), Output("btn-6m", "active"), Output("btn-1y", "active")], 
    [Input("open-filter", "n_clicks"), Input("apply-filter", "n_clicks"), 
     Input("btn-1m", "n_clicks"), Input("btn-3m", "n_clicks"), Input("btn-6m", "n_clicks"), Input("btn-1y", "n_clicks"),
     Input("filter-exchange", "value"), Input("filter-product", "value")],
    [State("modal-filter", "is_open"), State("filter-symbol", "value"), 
     State("current-active-symbol", "data"), State("chart-range-store", "data")]
)
def manage_ui_state(n_open, n_apply, n1, n3, n6, n1y, sel_exchange, sel_prod, is_open, sel_symbol, current_active, current_range):
    trigger = ctx.triggered_id
    
    if trigger in ["btn-1m", "btn-3m", "btn-6m", "btn-1y"]:
        new_range = "1M"
        if trigger == "btn-3m": new_range = "3M"
        if trigger == "btn-6m": new_range = "6M"
        if trigger == "btn-1y": new_range = "1Y"
        return is_open, no_update, no_update, no_update, new_range, (new_range=="1M"), (new_range=="3M"), (new_range=="6M"), (new_range=="1Y")

    if trigger == 'open-filter':
        return True, [], [], no_update, no_update, (current_range=="1M"), (current_range=="3M"), (current_range=="6M"), (current_range=="1Y")
    
    if trigger == 'apply-filter':
        if sel_symbol:
            save_active_chart(sel_symbol, sel_prod, period="1M")
            return False, [], [], {'symbol': sel_symbol, 'product': sel_prod}, "1M", True, False, False, False
        return is_open, [], [], no_update, no_update, (current_range=="1M"), (current_range=="3M"), (current_range=="6M"), (current_range=="1Y")

    # --- OTIMIZAÇÃO: USO DO CACHE DE MEMÓRIA (INSTANTÂNEO) ---
    if is_open or trigger in ['filter-exchange', 'filter-product']:
        try:
            # Pega do cache em vez de ir ao disco
            df = get_cached_metadata()
            if not df.empty:
                prods = sorted(df[df['group_name'] == sel_exchange]['product_name'].unique()) if sel_exchange else []
                syms = sorted(df[df['product_name'] == sel_prod]['symbol'].unique()) if sel_prod else []
                return is_open, prods, syms, no_update, no_update, (current_range=="1M"), (current_range=="3M"), (current_range=="6M"), (current_range=="1Y")
        except: pass
        
    return is_open, [], [], no_update, no_update, (current_range=="1M"), (current_range=="3M"), (current_range=="6M"), (current_range=="1Y")

@app.callback(
    [Output("grid-soja", "rowData"), Output("grid-milho", "rowData"), Output("grid-farelo", "rowData"), Output("grid-oleo", "rowData"),
     Output("grid-dolar", "rowData"), Output("grid-ccm", "rowData"), Output("grid-boi", "rowData"),
     Output("grid-spot", "rowData"), Output("chart-container", "children"), Output("server-clock", "children"), Output("chart-title", "children")],
    [Input("interval-main", "n_intervals"), Input("current-active-symbol", "data"), Input("chart-range-store", "data")]
)
def update_all(n, active_data, range_val):
    try:
        df = safe_read_db(DB_REALTIME, "SELECT * FROM market_snapshot")
        df_hist = safe_read_db(DB_HISTORY, "SELECT * FROM market_history ORDER BY date_ref ASC")
        if df.empty: return [no_update]*11

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

        mask_dolar = df['symbol'].str.upper().str.startswith('DOL') | df['product_name'].str.lower().str.contains('dolar')
        if mask_dolar.any():
            for c in ['Last', 'High', 'Low', 'Open', 'Change']:
                df.loc[mask_dolar & (df[c] > 100), c] /= 1000.0
        
        for c in ['Last', 'High', 'Low', 'Change']: df[f"{c}_fmt"] = format_currency_vec(df[c], 2)
        if mask_dolar.any():
            for c in ['Last', 'High', 'Low', 'Change']: df.loc[mask_dolar, f"{c}_fmt"] = format_currency_vec(df.loc[mask_dolar, c], 4)
        
        df['PChange_fmt'] = format_pct_vec(df['PChange'])
        df['Change_raw'] = df['Change']

        def d(g, p): 
            if df.empty: return []
            filtered = df[(df['group_name'].str.lower() == g.lower()) & (df['product_name'].str.lower() == p.lower())]
            return filtered.to_dict('records')

        target_symbol = None
        target_prod_name = "Dólar"

        if active_data and active_data.get('symbol'):
            target_symbol = active_data['symbol']
            target_prod_name = active_data.get('product') or "Chart"
        
        if not target_symbol:
            today = datetime.now()
            target_date = today + relativedelta(months=1)
            if today.day > 25: target_date = today + relativedelta(months=2)
            m_char = MONTH_CODES.get(target_date.month, 'F')
            y_str = target_date.strftime('%y')
            target_symbol = f"DOL{m_char}{y_str}"
            target_prod_name = "Dólar Futuro"

        chart_title_text = f"{target_prod_name} - {target_symbol}"
        chart_component = None
        
        has_data = False
        hist_data = pd.DataFrame()
        if not df_hist.empty:
            hist_data = df_hist[df_hist['symbol'] == target_symbol].copy()
            if not hist_data.empty: has_data = True

        if not has_data:
            chart_component = dbc.Spinner(color="success", type="grow", size="lg")
        else:
            hist_data.columns = [x.lower() for x in hist_data.columns]
            hist_data = hist_data.rename(columns={'max': 'high', 'min': 'low'})
            
            if 'dol' in target_symbol.lower():
                for c in ['open', 'high', 'low', 'close']: 
                    if c in hist_data.columns and (hist_data[c] > 100).any():
                        hist_data[c] = pd.to_numeric(hist_data[c], errors='coerce').fillna(0) / 1000.0
            
            live_row = df[df['symbol'] == target_symbol]
            if not live_row.empty:
                last_real = live_row.iloc[0]['Last']
                open_real = live_row.iloc[0]['Open']
                high_real = live_row.iloc[0]['High']
                low_real = live_row.iloc[0]['Low']
                
                if 'dol' in target_symbol.lower():
                    if last_real > 100: last_real /= 1000.0
                    if open_real > 100: open_real /= 1000.0
                    if high_real > 100: high_real /= 1000.0
                    if low_real > 100: low_real /= 1000.0

                if open_real <= 0.1: open_real = last_real
                if low_real <= 0.1: low_real = min(open_real, last_real)
                if high_real <= 0.1: high_real = max(open_real, last_real)

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
            
            days_to_show = 30
            if range_val == "3M": days_to_show = 90
            elif range_val == "6M": days_to_show = 180
            elif range_val == "1Y": days_to_show = 365
            
            cutoff_date = (datetime.now() - timedelta(days=days_to_show)).date()
            hist_data['date_ref_dt'] = pd.to_datetime(hist_data['date_ref']).dt.date
            view_data = hist_data[hist_data['date_ref_dt'] >= cutoff_date]
            
            fig = go.Figure()
            fig.add_trace(go.Candlestick(x=view_data['date_ref'], open=view_data['open'], high=view_data['high'], low=view_data['low'], close=view_data['close'], increasing_line_color='#00e676', decreasing_line_color='#ff1744', xhoverformat="%d/%b", yhoverformat=',.4f'))
            fig.add_hline(y=last_c, line_dash="dot", line_color='#00e676', annotation_text=f"<b> {last_c:,.4f} </b>".replace('.', ','), annotation_position="right", annotation_bgcolor='#00e676', annotation_font_color="white", annotation_font=dict(size=18, family="Roboto Mono"), annotation_borderpad=6)
            
            fig.update_layout(
                template="plotly_dark", 
                paper_bgcolor='rgba(0,0,0,0)', 
                plot_bgcolor='rgba(0,0,0,0)', 
                margin=dict(l=10, r=80, t=10, b=50), 
                xaxis_rangeslider_visible=False, 
                xaxis=dict(showgrid=True, gridcolor='#333', tickformat='%d/%b', tickangle=-45), 
                yaxis=dict(side='right', tickformat=".4f", showgrid=True, gridcolor='#333', fixedrange=False, autorange=True), 
                separators=',.', 
                uirevision=f"{target_symbol}-{range_val}"
            )
            
            chart_component = dcc.Graph(figure=fig, style={"height": "100%"}, config={'displayModeBar': False, 'displaylogo': False, 'showTips': False})

        return (d('CBOT', 'Soja'), d('CBOT', 'Milho'), d('CBOT', 'Farelo'), d('CBOT', 'Oleo'),
                d('B3', 'Dolar'), d('B3', 'Milho'), d('B3', 'Boi'),
                d('ECONOMIA', 'Dolar Comercial'),
                chart_component, 
                datetime.now().strftime('%d/%m/%Y %H:%M:%S'), chart_title_text)
    except Exception as e:
        print(f"Erro App: {e}")
        return [no_update]*11

if __name__ == "__main__":
    app.run(debug=False, port=8050, host='0.0.0.0')
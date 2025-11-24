import os
import json
import time
import requests
import duckdb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# --- CONFIGURAÇÕES GLOBAIS ---
FIELDS_MAP = {
    1: 'Time', 2: 'Open', 3: 'High', 4: 'Low', 8: 'Volume',
    10: 'Last', 12: 'Change', 13: 'PChange', 14: 'Bid', 15: 'Ask', 21: 'Previous'
}

MONTH_CODES = {
    1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
    7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
}

class SmartIngestor:
    def __init__(self):
        self.base_url = os.getenv("CMA_HOST") + "/execute"
        self.user = os.getenv("CMA_USER")
        self.password = os.getenv("CMA_PASS")
        self.session = requests.Session()
        self.session_id = None
        self.msg_id = 0
        self.config = self._load_config()
        self._init_db_structure()

    def _load_config(self):
        try:
            with open('config.json', 'r') as f: return json.load(f)
        except: return {}

    def _get_id(self):
        self.msg_id += 1
        return self.msg_id

    def _init_db_structure(self):
        try:
            with duckdb.connect('cma_data.duckdb') as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS market_snapshot (
                        timestamp TIMESTAMP, group_name VARCHAR, product_name VARCHAR, symbol VARCHAR, description VARCHAR,
                        Last DOUBLE, High DOUBLE, Low DOUBLE, Open DOUBLE, Change DOUBLE, PChange DOUBLE, Previous DOUBLE, 
                        Volume DOUBLE, Time VARCHAR, Bid DOUBLE, Ask DOUBLE, Maturity DATE
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS market_history (
                        symbol VARCHAR, date_ref DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE
                    )
                """)
        except Exception as e: print(f"⚠️ Erro DB Init: {e}")

    def _safe_float(self, value):
        if value is None or value == '': return 0.0
        if isinstance(value, (int, float)): return float(value)
        try:
            s_val = str(value)
            if ',' in s_val: s_val = s_val.replace('.', '').replace(',', '.')
            return float(s_val)
        except: return 0.0

    def _post(self, name, payload):
        current_session = self.session_id if self.session_id else ""
        data = {"id": self._get_id(), "name": name, "sessionId": current_session, **payload}
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(data)}, timeout=15)
            if resp.status_code == 200: return resp.json()
            return None
        except Exception as e:
            print(f"❌ Erro Conexão: {e}")
            return None

    def login(self):
        print(f"🔐 Autenticando...")
        payload = {
            "user": self.user, "pass": self.password, "type": "s", "service": "m", 
            "transport": "Polling", "version": 1, "sync": True,
            "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}
        }
        resp = self._post("LoginRequest", payload)
        if resp and resp.get("success"):
            self.session_id = resp.get("sessionId")
            print(f"✅ Login OK! Sessão: {self.session_id[:8]}...")
            return True
        print(f"⛔ Falha Login")
        return False

    def generate_symbol_list(self):
        candidates = []
        today = datetime.now()
        chart_symbol = None 
        for group_name, products in self.config.items():
            for prod in products:
                for i in range(14): 
                    future_date = today + relativedelta(months=i)
                    month_char = MONTH_CODES[future_date.month]
                    if month_char in prod.get('months', "FGHJKMNQUVXZ"):
                        symbol = f"{prod['root']}{month_char}{future_date.strftime('%y')}"
                        item = {"symbol": symbol, "sourceId": str(prod['source']), "group": group_name, "product": prod['name'], "maturity_est": future_date.replace(day=1).date()}
                        candidates.append(item)
                        if prod.get('chart') == True and chart_symbol is None:
                            if future_date.date() >= today.date(): chart_symbol = item
        return candidates, chart_symbol

    def capture_history(self, target):
        if not target: return
        dt_to = datetime.now()
        # --- AJUSTE V3.1: Histórico reduzido para 90 dias (3 meses) ---
        dt_from = dt_to - timedelta(days=90)
        
        payload = {
            "type": "c", "sync": True, "symbolId": {"sourceId": target['sourceId'], "symbol": target['symbol']},
            "dateFrom": dt_from.strftime("%Y-%m-%d"), "dateTo": dt_to.strftime("%Y-%m-%d"), "period": 1
        }
        resp = self._post("DailyGraphRequest", payload)
        if resp and 'bars' in resp:
            rows = []
            for b in resp['bars']:
                try:
                    d_str = str(b.get('02', ''))
                    if len(d_str) == 8:
                        d_fmt = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
                        rows.append((target['symbol'], d_fmt, self._safe_float(b.get('06')), self._safe_float(b.get('04')), self._safe_float(b.get('05')), self._safe_float(b.get('03')), self._safe_float(b.get('08'))))
                except: continue
            if rows:
                try:
                    with duckdb.connect('cma_data.duckdb') as conn:
                        conn.execute("DELETE FROM market_history")
                        conn.executemany("INSERT INTO market_history VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
                except: pass

    def capture_snapshot(self):
        candidates, chart_target = self.generate_symbol_list()
        if not candidates: return
        api_symbols = [{"symbol": x['symbol'], "sourceId": x['sourceId']} for x in candidates]
        meta_map = {x['symbol']: x for x in candidates}
        requested_fields = list(FIELDS_MAP.keys())
        valid_rows = []
        
        for i in range(0, len(api_symbols), 50):
            resp = self._post("QuotesRequest", {"type": "q", "sync": True, "symbols": api_symbols[i:i+50], "fields": requested_fields})
            if resp and 'arrQuotes' in resp:
                now_ts = datetime.now()
                for q in resp['arrQuotes']:
                    sym = q.get('symbolId', {}).get('symbol')
                    meta = meta_map.get(sym)
                    if not meta: continue
                    vals = {FIELDS_MAP[item['id']]: item['value'] for item in q.get('arrValues', []) if item['id'] in FIELDS_MAP}
                    
                    last = self._safe_float(vals.get('Last'))
                    previous = self._safe_float(vals.get('Previous'))
                    if last == 0 and previous > 0: last = previous
                    
                    valid_rows.append((
                        now_ts, meta['group'], meta['product'], sym, f"{meta['product']} {sym[-3:]}",
                        last, self._safe_float(vals.get('High')), self._safe_float(vals.get('Low')),
                        self._safe_float(vals.get('Open')), self._safe_float(vals.get('Change')),
                        self._safe_float(vals.get('PChange')), previous,
                        self._safe_float(vals.get('Volume')), vals.get('Time', '-'),
                        self._safe_float(vals.get('Bid')), self._safe_float(vals.get('Ask')), meta['maturity_est']
                    ))

        if valid_rows:
            try:
                with duckdb.connect('cma_data.duckdb') as conn:
                    conn.execute("DELETE FROM market_snapshot")
                    conn.executemany("INSERT INTO market_snapshot VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", valid_rows)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 📡 Atualizado: {len(valid_rows)} ativos.")
            except Exception as e: print(f"❌ Erro DB: {e}")
        if chart_target: self.capture_history(chart_target)

    def run_loop(self):
        while True:
            if not self.session_id:
                if not self.login():
                    time.sleep(10)
                    continue
            try:
                self.capture_snapshot()
                time.sleep(2)
            except KeyboardInterrupt: break
            except Exception: self.session_id = None

if __name__ == "__main__":
    print("🚀 Iniciando Ingestor v3.1 (3 Meses Histórico)")
    bot = SmartIngestor()
    bot.run_loop()
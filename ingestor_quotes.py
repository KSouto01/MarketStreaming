import os
import json
import time
import requests
import duckdb
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
DB_REALTIME = os.path.join(BASE_DIR, 'cma_realtime.duckdb')

FIELDS_MAP = {
    '24': 'Time', '18': 'Open', '16': 'High', '17': 'Low', '13': 'Volume',
    '10': 'Last', '26': 'Change', '01': 'PChange', '14': 'Bid', '15': 'Ask', '1A': 'Previous'
}
MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

class RealtimeIngestor:
    def __init__(self):
        host = os.getenv("CMA_HOST")
        if not host: raise ValueError("❌ Erro: CMA_HOST vazio.")
        self.base_url = host + "/execute"
        self.user = os.getenv("CMA_USER")
        self.password = os.getenv("CMA_PASS")
        self.session = requests.Session()
        self.session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20))
        self.session_id = None
        self.msg_id = 0
        self.config = self._load_config()
        print(f"📂 Banco Realtime: {DB_REALTIME}")
        self._init_db()

    def _load_config(self):
        try:
            with open(os.path.join(BASE_DIR, 'config.json'), 'r') as f: return json.load(f)
        except: return {}

    def _init_db(self):
        try:
            with duckdb.connect(DB_REALTIME) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS market_snapshot (timestamp TIMESTAMP, group_name VARCHAR, product_name VARCHAR, symbol VARCHAR, description VARCHAR, Last DOUBLE, High DOUBLE, Low DOUBLE, Open DOUBLE, Change DOUBLE, PChange DOUBLE, Previous DOUBLE, Volume DOUBLE, Time VARCHAR, Bid DOUBLE, Ask DOUBLE, Maturity DATE)")
        except Exception as e: print(f"⚠️ Erro Init DB: {e}")

    def _fast_float(self, val):
        if val is None or val == "": return 0.0
        try: return float(val)
        except:
            try: return float(str(val).replace('+', '').replace(',', '.').strip())
            except: return 0.0

    def _post(self, name, payload):
        data = {"id": self.msg_id, "name": name, "sessionId": self.session_id or "", **payload}
        self.msg_id += 1
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(data)}, timeout=4)
            return resp.json() if resp.status_code == 200 else None
        except: return None

    def login(self):
        print("⚡ [Quotes] Autenticando...")
        self.session.cookies.clear()
        resp = self._post("LoginRequest", {"user": self.user, "pass": self.password, "type": "s", "service": "m", "transport": "Polling", "version": 1, "sync": True, "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}})
        if resp and resp.get("success"):
            self.session_id = resp.get("sessionId")
            print("✅ [Quotes] Conectado! Iniciando Streaming...")
            return True
        return False

    def generate_symbol_list(self):
        candidates = []
        today = datetime.now()
        for group, prods in self.config.items():
            for prod in prods:
                for i in range(24):
                    fut = today + relativedelta(months=i)
                    m_char = MONTH_CODES[fut.month]
                    if m_char in prod.get('months', "FGHJKMNQUVXZ"):
                        s_id = str(prod['source'])
                        y_suf = fut.strftime('%y')[-1] if s_id == '30' else fut.strftime('%y')
                        sym = f"{prod['root']}{m_char}{y_suf}"
                        candidates.append({"symbol": sym, "sourceId": s_id, "group": group, "product": prod['name'], "maturity_est": fut.replace(day=1).date()})
        return candidates

    def save_to_db(self, rows):
        for _ in range(3):
            try:
                con = duckdb.connect(DB_REALTIME)
                con.execute("DELETE FROM market_snapshot")
                if rows:
                    con.executemany("INSERT INTO market_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
                con.close()
                return True
            except duckdb.CatalogException: self._init_db()
            except: time.sleep(0.1)
        return False

    def run(self):
        while True:
            if not self.session_id:
                if not self.login(): time.sleep(5); continue
            
            candidates = self.generate_symbol_list()
            api_symbols = [{"symbol": x['symbol'], "sourceId": x['sourceId']} for x in candidates]
            meta_map = {x['symbol']: x for x in candidates}
            
            try:
                valid_rows = []
                for i in range(0, len(api_symbols), 100):
                    chunk = api_symbols[i:i+100]
                    resp = self._post("QuotesRequest", {"type": "q", "sync": True, "symbols": chunk, "fields": list(FIELDS_MAP.keys())})
                    
                    if resp and 'arrQuotes' in resp:
                        now = datetime.now()
                        for q in resp['arrQuotes']:
                            sym = q.get('symbolId', {}).get('symbol')
                            meta = meta_map.get(sym)
                            if not meta: continue
                            
                            vals = {}
                            for item in q.get('arrValues', []):
                                for k, v in item.items():
                                    if str(k).upper() in FIELDS_MAP: vals[FIELDS_MAP[str(k).upper()]] = v
                            
                            last = self._fast_float(vals.get('Last'))
                            prev = self._fast_float(vals.get('Previous'))
                            
                            # Fallback se Last for 0 mas tiver Previous
                            if last == 0 and prev > 0: last = prev 
                            
                            # --- FILTRO NA FONTE (O SEGREDO) ---
                            # Só salva se tiver preço > 0. Ativos zerados são descartados aqui.
                            if last > 0:
                                valid_rows.append((
                                    now, meta['group'], meta['product'], sym, f"{meta['product']} {sym}",
                                    last, self._fast_float(vals.get('High')), self._fast_float(vals.get('Low')),
                                    self._fast_float(vals.get('Open')), self._fast_float(vals.get('Change')),
                                    self._fast_float(vals.get('PChange')), prev,
                                    self._fast_float(vals.get('Volume')), str(vals.get('Time', '-')),
                                    self._fast_float(vals.get('Bid')), self._fast_float(vals.get('Ask')), 
                                    meta['maturity_est']
                                ))

                self.save_to_db(valid_rows)
                print(f"\r🚀 Streaming: {len(valid_rows)} ativos válidos (Last > 0) | {datetime.now().strftime('%H:%M:%S')}", end="", flush=True)
                
                time.sleep(0.5)

            except KeyboardInterrupt: break
            except: time.sleep(1)

if __name__ == "__main__":
    RealtimeIngestor().run()
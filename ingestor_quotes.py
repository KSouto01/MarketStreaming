import os
import json
import time
import sys
import requests
import duckdb
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
DB_REALTIME = os.path.join(BASE_DIR, 'cma_realtime.duckdb')
SESSION_FILE = os.path.join(BASE_DIR, 'session_token.json')

FIELDS_MAP = {
    '24': 'Time', '18': 'Open', '16': 'High', '17': 'Low', '13': 'Volume',
    '10': 'Last', '26': 'Change', '01': 'PChange', '14': 'Bid', '15': 'Ask', '1A': 'Previous'
}
MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}

class RealtimeIngestor:
    def __init__(self):
        host = os.getenv("CMA_HOST")
        if not host: sys.exit("❌ Erro: CMA_HOST vazio.")
        self.base_url = host + "/execute"
        self.user = os.getenv("CMA_USER")
        self.password = os.getenv("CMA_PASS")
        self.session = requests.Session()
        self.session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20))
        self.session_id = None
        self.msg_id = 0
        self.error_count = 0 
        self.config = self._load_config()
        self._init_db()

    def _load_config(self):
        try:
            with open(os.path.join(BASE_DIR, 'config.json'), 'r') as f: return json.load(f)
        except: return {}

    def _init_db(self):
        try:
            with duckdb.connect(DB_REALTIME) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS market_snapshot (timestamp TIMESTAMP, group_name VARCHAR, product_name VARCHAR, symbol VARCHAR, description VARCHAR, Last DOUBLE, High DOUBLE, Low DOUBLE, Open DOUBLE, Change DOUBLE, PChange DOUBLE, Previous DOUBLE, Volume DOUBLE, Time VARCHAR, Bid DOUBLE, Ask DOUBLE, Maturity DATE)")
        except: pass

    def _fast_float(self, val):
        if val is None or val == "": return 0.0
        try: return float(val)
        except: return 0.0

    def get_shared_session(self):
        try:
            if os.path.exists(SESSION_FILE):
                with open(SESSION_FILE, 'r') as f: return json.load(f).get('sessionId')
        except: pass
        return None

    def save_shared_session(self, new_id):
        try:
            with open(SESSION_FILE, 'w') as f: json.dump({'sessionId': new_id, 'updated_at': time.time()}, f)
        except: pass

    # --- NOVO: INVALIDAÇÃO DE SESSÃO ---
    def invalidate_session(self):
        print("🔥 [Quotes] Sessão Inválida! Resetando...")
        self.session_id = None
        try:
            if os.path.exists(SESSION_FILE): os.remove(SESSION_FILE)
        except: pass

    def login(self):
        print("⚡ [Quotes] Verificando sessão...")
        shared_id = self.get_shared_session()
        if shared_id and shared_id != self.session_id:
            print(f"♻️ Usando sessão compartilhada: {shared_id[:10]}...")
            self.session_id = shared_id
            return True

        print("⚡ [Quotes] Autenticando...")
        self.session.cookies.clear()
        self.msg_id += 1
        payload = {"id": self.msg_id, "name": "LoginRequest", "sessionId": "", "user": self.user, "pass": self.password, "type": "s", "service": "m", "transport": "Polling", "version": 1, "sync": True, "oms": {"ip": "0.0.0.0", "channel": "API", "language": "PT"}}
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(payload)}, timeout=10)
            if resp.status_code == 200:
                rj = resp.json()
                if rj.get("success"):
                    self.session_id = rj.get("sessionId")
                    self.save_shared_session(self.session_id)
                    self.error_count = 0
                    print(f"✅ [Quotes] Nova Sessão: {self.session_id[:10]}...")
                    return True
        except Exception as e: print(f"❌ Erro Login: {e}")
        return False

    def _post(self, name, payload):
        self.msg_id += 1
        data = {"id": self.msg_id, "name": name, "sessionId": self.session_id or "", **payload}
        try:
            resp = self.session.post(self.base_url, data={'JSONRequest': json.dumps(data)}, timeout=5)
            if resp.status_code == 200: return resp.json()
            return None
        except: return None

    def generate_symbol_list(self):
        candidates = []
        today = datetime.now()
        for group, prods in self.config.items():
            for prod in prods:
                months_str = prod.get('months', "")
                s_id = str(prod['source'])
                
                if not months_str: 
                    candidates.append({"symbol": prod['root'], "sourceId": s_id, "group": group, "product": prod['name'], "maturity_est": today.date()})
                else: 
                    search_range = 24 if s_id == '30' else 14
                    for i in range(search_range): 
                        fut = today + relativedelta(months=i)
                        m_char = MONTH_CODES[fut.month]
                        if m_char in months_str:
                            y_suf = fut.strftime('%y')[-1] if s_id == '30' else fut.strftime('%y')
                            sym = f"{prod['root']}{m_char}{y_suf}"
                            candidates.append({"symbol": sym, "sourceId": s_id, "group": group, "product": prod['name'], "maturity_est": fut.replace(day=1).date()})
        return candidates

    def save_to_db(self, rows):
        for _ in range(3):
            try:
                con = duckdb.connect(DB_REALTIME)
                con.execute("DELETE FROM market_snapshot")
                if rows: con.executemany("INSERT INTO market_snapshot VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
                con.close()
                return True
            except: time.sleep(0.1)
        return False

    def run(self):
        print("🚀 [Quotes] Ingestor Iniciado...")
        while True:
            if self.error_count > 15:
                print("💀 [Quotes] Reiniciando...")
                self.invalidate_session()
                sys.exit(1)

            if not self.session_id:
                if not self.login(): 
                    self.error_count += 1
                    time.sleep(5)
                    continue
            
            candidates = self.generate_symbol_list()
            api_symbols = [{"symbol": x['symbol'], "sourceId": x['sourceId']} for x in candidates]
            meta_map = {x['symbol']: x for x in candidates}
            
            try:
                valid_rows = []
                total_encontrados = 0
                
                for i in range(0, len(api_symbols), 100):
                    chunk = api_symbols[i:i+100]
                    resp = self._post("QuotesRequest", {"type": "q", "sync": True, "symbols": chunk, "fields": list(FIELDS_MAP.keys())})
                    
                    if not resp: 
                        print("❌ Falha API")
                        self.error_count += 1
                        continue

                    # DETECÇÃO DE SESSÃO INVÁLIDA (Status 10004 ou success false por auth)
                    if not resp.get("success"):
                        code = resp.get("status")
                        if code == 10004 or resp.get("code") in [401, 403, -1]:
                            self.invalidate_session()
                            raise Exception("Sessão Inválida - Resetando")

                    if 'arrQuotes' in resp:
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
                            bid = self._fast_float(vals.get('Bid'))
                            ask = self._fast_float(vals.get('Ask'))
                            
                            if last > 0 or prev > 0 or bid > 0 or ask > 0:
                                display_last = last if last > 0 else (prev if prev > 0 else bid)
                                valid_rows.append((now, meta['group'], meta['product'], sym, f"{meta['product']} {sym}", display_last, self._fast_float(vals.get('High')), self._fast_float(vals.get('Low')), self._fast_float(vals.get('Open')), self._fast_float(vals.get('Change')), self._fast_float(vals.get('PChange')), prev, self._fast_float(vals.get('Volume')), str(vals.get('Time', '-')), bid, ask, meta['maturity_est']))
                                total_encontrados += 1

                if self.save_to_db(valid_rows):
                    self.error_count = 0
                    print(f"\r🚀 Quotes: {total_encontrados} / {len(api_symbols)} ativos | {datetime.now().strftime('%H:%M:%S')}", end="", flush=True)
                time.sleep(0.5)

            except Exception as e:
                self.error_count += 1
                if self.error_count > 2:
                   self.invalidate_session()
                time.sleep(2)

if __name__ == "__main__":
    RealtimeIngestor().run()
import pandas as pd
import os
import heapq
import json
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CURRENT_DIR)
BASE_DIR = os.path.dirname(CURRENT_DIR)

from engine import DecisionEngine, MarketEvent

DATA_DIR = "/data" if os.path.exists("/data") else os.path.join(BASE_DIR, "validation")
BASE_OUTPUT = "/output" if os.path.exists("/output") else os.path.join(BASE_DIR, "output")
OUTPUT_DIR = os.path.join(BASE_OUTPUT, "historical")

os.makedirs(OUTPUT_DIR, exist_ok=True)

class CsvStreamer:
    def __init__(self, files):
        self.readers = {}
        self.queue = [] 
        self.seq = 0        
        for name, path in files.items():
            if os.path.exists(path):
                self.readers[name] = pd.read_csv(path, chunksize=50000)
                self._load_next_chunk(name)

    def _load_next_chunk(self, name):
        try:
            chunk = next(self.readers[name])
            chunk.columns = [c.strip().lower() for c in chunk.columns]
            ts_col = next((c for c in chunk.columns if 'time' in c or 'ts' in c), None)
            if not ts_col: return
            id_col = next((c for c in chunk.columns if c == 'id'), None)
            for _, row in chunk.iterrows():
                ts = int(row[ts_col])
                data_dict = row.to_dict()
                if id_col and pd.notnull(row[id_col]):
                    data_dict['id'] = row[id_col]
                heapq.heappush(self.queue, (ts, self.seq, name, data_dict))
                self.seq += 1
        except StopIteration: pass

    def __iter__(self):
        while self.queue:
            ts, _, name, data = heapq.heappop(self.queue)
            if len(self.queue) < 1000: 
                for k in self.readers.keys():
                    if sum(1 for item in self.queue if item[2] == k) < 100: self._load_next_chunk(k)
            local_ts = data.get('local_timestamp', ts)
            yield MarketEvent(event_time=ts, local_time=int(local_ts) if pd.notnull(local_ts) else ts, type=name.upper(), data=data)

def run_historical():
    print(f">>> Historical Validation Mode")
    print(f"    Data Dir: {DATA_DIR}")
    print(f"    Output Dir: {OUTPUT_DIR}")

    config_path = "/output/model_config.json" if os.path.exists("/output/model_config.json") else os.path.join(BASE_DIR, "output", "model_config.json")
    
    engine = DecisionEngine(config_path=config_path)
    
    files = {
        'trade': os.path.join(DATA_DIR, 'trades.csv'), 
        'orderbook': os.path.join(DATA_DIR, 'orderbook.csv'), 
        'ticker': os.path.join(DATA_DIR, 'ticker.csv'),
        'liquidation': os.path.join(DATA_DIR, 'liquidations.csv') 
    }
    
    streamer = CsvStreamer(files)
    
    f_dec = open(os.path.join(OUTPUT_DIR, 'decisions.jsonl'), 'w')
    f_trans = open(os.path.join(OUTPUT_DIR, 'state_transitions.jsonl'), 'w')
    
    cnt, blocked_cnt, last_state = 0, 0, "BOOTSTRAP"
    try:
        for event in streamer:
            result = engine.process_event(event)

            if result['action'] != 'ALLOWED':
                blocked_cnt += 1
                f_dec.write(json.dumps({k: v for k, v in result.items() if not k.startswith('_')}) + "\n")

            curr_state = result['_internal_state']
            if curr_state != last_state:
                trust, hypo = engine.get_state_info()
                trans_log = {"ts": result['ts'], "data_trust": trust, "hypothesis": hypo, "decision": result['action'], "trigger": result['_trigger_detail']}
                f_trans.write(json.dumps(trans_log) + "\n")
                last_state = curr_state
            
            cnt += 1
            if cnt % 50000 == 0: sys.stdout.write(f"\rProcessed: {cnt} | Blocked: {blocked_cnt}")
            
    except KeyboardInterrupt: pass
    finally:
        f_dec.close(); f_trans.close()
        with open(os.path.join(OUTPUT_DIR, 'summary.json'), 'w') as f: 
            json.dump({"total_events": cnt, "blocked_events": blocked_cnt, "final_state": last_state}, f, indent=4)
    print(f"\nDone. Saved to {OUTPUT_DIR}")

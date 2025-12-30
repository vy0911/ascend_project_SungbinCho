import pandas as pd
import numpy as np
import json
import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CONFIG_FILE = os.path.join(OUTPUT_DIR, "model_config.json")
RESEARCH_DIR = os.path.join(BASE_DIR, "research")
if not os.path.exists(RESEARCH_DIR): RESEARCH_DIR = os.path.join(BASE_DIR, "validation")

def run_research():
    print("[Phase 0] Research")
    trades_path = os.path.join(RESEARCH_DIR, "trades.csv")
    ob_path = os.path.join(RESEARCH_DIR, "orderbook.csv")
    
    trades = pd.read_csv(trades_path, nrows=100000) if os.path.exists(trades_path) else None
    books = pd.read_csv(ob_path, nrows=100000) if os.path.exists(ob_path) else None
    
    if trades is None or books is None:
        default_model = {"mu": [0.0, 0.0], "inv_cov": [[1.0, 0.0], [0.0, 1.0]], "threshold": 3.0}
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(CONFIG_FILE, 'w') as f: json.dump(default_model, f)
        return

    trades.columns = [c.strip().lower() for c in trades.columns]
    books.columns = [c.strip().lower() for c in books.columns]
    
    trades = trades.sort_values('timestamp')
    trades['ret'] = trades['price'].pct_change().fillna(0)
    trades['vol'] = trades['ret'].rolling(50).std().fillna(0)
    
    books = books.sort_values('timestamp')
    if 'side' in books.columns:
        bids = books[books['side']=='bid'].groupby('timestamp')['price'].max()
        asks = books[books['side']=='ask'].groupby('timestamp')['price'].min()
        spread_df = pd.concat([bids, asks], axis=1, keys=['bid', 'ask']).dropna()
        spread_df['spread'] = spread_df['ask'] - spread_df['bid']
        spread_df = spread_df[spread_df['spread'] > 0]
    else: return

    trades_idx = trades[['timestamp', 'vol']].set_index('timestamp')
    merged = pd.merge_asof(trades_idx.sort_index(), spread_df['spread'].sort_index(), left_index=True, right_index=True, direction='backward').dropna()
    
    X = merged[['vol', 'spread']].values
    X[:, 0] = np.log(X[:, 0] + 1e-9)
    X[:, 1] = np.log(X[:, 1] + 1e-9)
    
    mu = np.mean(X, axis=0)
    cov = np.cov(X, rowvar=False)
    inv_cov = np.linalg.inv(cov + np.eye(2) * 1e-6)
    
    dists = [np.sqrt((row-mu).T @ inv_cov @ (row-mu)) for row in X[::100]]
    threshold = float(np.percentile(dists, 99.9)) if dists else 3.0
    
    model_config = {"mu": mu.tolist(), "inv_cov": inv_cov.tolist(), "threshold": max(threshold, 3.0)}
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(CONFIG_FILE, 'w') as f: json.dump(model_config, f, indent=4)
    print(f"Config saved.")

if __name__ == "__main__": run_research()

import json
import sys

# Field indices based on README.md
IDX_SALE = 0
IDX_SALES_AMOUNT = 1
IDX_CLICK_TIMESTAMP = 3
IDX_PRODUCT_PRICE = 5
IDX_PARTNER_ID = 15
IDX_USER_ID = 16

INPUT_PATH = 'data/CriteoSearchData'
OUTPUT_PATH = 'q1_realtime_stream_processing/data/sample_ad_events.jsonl'
MAX_LINES = 10000

with open(INPUT_PATH, 'r') as fin, open(OUTPUT_PATH, 'w') as fout:
    for i, line in enumerate(fin):
        if i >= MAX_LINES:
            break
        parts = line.strip().split('\t')
        if len(parts) < 17:
            continue
        try:
            sale = int(parts[IDX_SALE])
            sales_amount = float(parts[IDX_SALES_AMOUNT]) if parts[IDX_SALES_AMOUNT] != '-1' else 0.0
            click_ts = int(parts[IDX_CLICK_TIMESTAMP]) if parts[IDX_CLICK_TIMESTAMP] != '-1' else None
            product_price = float(parts[IDX_PRODUCT_PRICE]) if parts[IDX_PRODUCT_PRICE] != '-1' else 0.0
            partner_id = parts[IDX_PARTNER_ID]
            user_id = parts[IDX_USER_ID]
            if not click_ts or partner_id == '-1' or user_id == '-1':
                continue
            event_type = 'conversion' if sale == 1 else 'click'
            bid_amount_usd = sales_amount if sales_amount > 0 else product_price
            # Convert timestamp to ISO8601
            from datetime import datetime
            ts_iso = datetime.utcfromtimestamp(click_ts).isoformat() + 'Z'
            event = {
                "timestamp": ts_iso,
                "campaign_id": partner_id,
                "event_type": event_type,
                "user_id": user_id,
                "bid_amount_usd": bid_amount_usd
            }
            fout.write(json.dumps(event) + '\n')
        except Exception as e:
            continue 
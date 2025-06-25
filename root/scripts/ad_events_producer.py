import json
import time
import sys
import logging
import argparse
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
KAFKA_TOPIC = 'ad-events'
RECONNECT_DELAY_S = 5
MAX_RETRIES = 10
LOOP_DELAY_S = 10 
EVENT_DELAY_S = 0.01

# Preview mode lines
PREVIEW_LINES_DEFAULT = 0

# --- Field indices for the raw Criteo TSV data ---
IDX_SALE = 0
IDX_SALES_AMOUNT_EURO = 1
IDX_TIME_DELAY_CONV = 2
IDX_CLICK_TIMESTAMP = 3
IDX_NB_CLICKS_WEEK = 4
IDX_PRODUCT_PRICE = 5
IDX_PRODUCT_AGE_GROUP = 6
IDX_DEVICE_TYPE = 7
IDX_AUDIENCE_ID = 8
IDX_PRODUCT_GENDER = 9
IDX_PRODUCT_BRAND = 10
IDX_PRODUCT_CATEGORY_1 = 11
IDX_PRODUCT_CATEGORY_2 = 12
IDX_PRODUCT_CATEGORY_3 = 13
IDX_PRODUCT_CATEGORY_4 = 14
IDX_PRODUCT_CATEGORY_5 = 15
IDX_PRODUCT_CATEGORY_6 = 16
IDX_PRODUCT_CATEGORY_7 = 17
IDX_PRODUCT_COUNTRY = 18
IDX_PRODUCT_ID = 19
IDX_PRODUCT_TITLE = 20
IDX_PARTNER_ID = 21
IDX_USER_ID = 22
# total expected columns
EXPECTED_COLS = 23

# Helper to convert "-1" to None with optional cast
def safe_cast(value: str, cast_fn, default=None):
    try:
        if value == "-1" or value == "":
            return default
        return cast_fn(value)
    except Exception:
        return default

def get_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Kafka producer for streaming ad events.")
    parser.add_argument(
        '--broker',
        default='localhost:9092',
        help='Kafka broker address (default: localhost:9092)'
    )
    parser.add_argument(
        '--file',
        default='root/data/CriteoSearchData',
        help='Path to the raw data file (default: root/data/CriteoSearchData)'
    )
    parser.add_argument(
        '--loop',
        action='store_true',
        help='Stream file in an infinite loop (default: disabled – single pass)'
    )
    parser.add_argument(
        '--preview-lines',
        type=int,
        default=PREVIEW_LINES_DEFAULT,
        help='If >0, do not send to Kafka, just print first N parsed events and exit.'
    )
    return parser.parse_args()

def create_producer(broker_address: str) -> KafkaProducer:
    """Creates a Kafka producer, retrying if brokers are not available."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker_address,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logging.info("✅ Kafka producer connected successfully to %s.", broker_address)
            return producer
        except NoBrokersAvailable:
            retries += 1
            logging.warning("⚠️ Kafka brokers not available at %s. Retrying in %ds... (%d/%d)",
                            broker_address, RECONNECT_DELAY_S, retries, MAX_RETRIES)
            time.sleep(RECONNECT_DELAY_S)
    logging.error("❌ Failed to connect to Kafka after multiple retries. Exiting.")
    sys.exit(1)

def parse_criteo_line(parts):
    """Converts a list of TSV parts into a base_payload dict.

    Returns None if mandatory fields are missing or malformed.
    This helper is reused by the producer and by optional preview/tests.
    """
    # Parse basic fields
    sale_flag = safe_cast(parts[IDX_SALE], int, 0)
    sales_amount_euro = safe_cast(parts[IDX_SALES_AMOUNT_EURO], float, 0.0)
    time_delay_conv = safe_cast(parts[IDX_TIME_DELAY_CONV], int)
    click_ts_raw = safe_cast(parts[IDX_CLICK_TIMESTAMP], int)
    nb_clicks_week = safe_cast(parts[IDX_NB_CLICKS_WEEK], int)
    product_price = safe_cast(parts[IDX_PRODUCT_PRICE], float, 0.0)
    product_age_group = parts[IDX_PRODUCT_AGE_GROUP]
    device_type = parts[IDX_DEVICE_TYPE]
    audience_id = parts[IDX_AUDIENCE_ID]
    product_gender = parts[IDX_PRODUCT_GENDER]
    product_brand = parts[IDX_PRODUCT_BRAND]
    product_category_1 = safe_cast(parts[IDX_PRODUCT_CATEGORY_1], int)
    product_category_2 = safe_cast(parts[IDX_PRODUCT_CATEGORY_2], int)
    product_category_3 = safe_cast(parts[IDX_PRODUCT_CATEGORY_3], int)
    product_category_4 = safe_cast(parts[IDX_PRODUCT_CATEGORY_4], int)
    product_category_5 = safe_cast(parts[IDX_PRODUCT_CATEGORY_5], int)
    product_category_6 = safe_cast(parts[IDX_PRODUCT_CATEGORY_6], int)
    product_category_7 = safe_cast(parts[IDX_PRODUCT_CATEGORY_7], int)
    country_code = parts[IDX_PRODUCT_COUNTRY]
    product_id = parts[IDX_PRODUCT_ID]
    product_title = parts[IDX_PRODUCT_TITLE]
    partner_id = parts[IDX_PARTNER_ID]
    user_id = parts[IDX_USER_ID]

    if partner_id == "-1" or user_id == "-1" or click_ts_raw is None:
        return None

    # Convert click timestamp (seconds since epoch) to ISO-8601 with Z suffix
    click_iso_ts = datetime.utcfromtimestamp(click_ts_raw).isoformat() + 'Z'

    # Use product_price as proxy for spend if sales amount is absent
    spend_usd = sales_amount_euro if sales_amount_euro and sales_amount_euro > 0 else product_price

    base_payload = {
        "timestamp": click_iso_ts,
        "campaign_id": partner_id,
        "user_id": user_id,
        "spend_usd": spend_usd,
        "device_type": device_type,
        "country_code": country_code,
        "product_brand": product_brand,
        "product_age_group": product_age_group,
        "product_category_1": product_category_1,
        "product_category_2": product_category_2,
        "product_category_3": product_category_3,
        "product_category_4": product_category_4,
        "product_category_5": product_category_5,
        "product_category_6": product_category_6,
        "product_category_7": product_category_7,
        "product_price": product_price,
        "sales_amount_euro": sales_amount_euro,
        "sale": bool(sale_flag),
        # Extra optional features
        "time_delay_for_conversion": time_delay_conv,
        "nb_clicks_1week": nb_clicks_week,
        "audience_id": audience_id,
        "product_gender": product_gender,
        "product_title": product_title,
        "product_id": product_id,
    }

    return base_payload

def process_file_once(producer: KafkaProducer, input_file: str, preview_lines: int = 0) -> int:
    """Reads the data file once and sends events to Kafka."""
    events_sent = 0
    try:
        with open(input_file, 'r') as f:
            for i, line in enumerate(f):
                try:
                    parts = line.rstrip("\n").split('\t')
                    if len(parts) < EXPECTED_COLS:
                        continue

                    payload = parse_criteo_line(parts)
                    if payload is None:
                        continue

                    if preview_lines > 0 and events_sent < preview_lines:
                        print(json.dumps(payload, indent=2))
                        events_sent += 1
                        continue

                    # Below this point we know preview is disabled -> send to Kafka
                    if producer is None:
                        continue
                        
                    base_payload = payload
                    sale_flag = 1 if base_payload["sale"] else 0
                    partner_id = base_payload["campaign_id"]
                    spend_usd = base_payload["spend_usd"]

                    # Impression event
                    impression_payload = base_payload.copy()
                    impression_payload["event_type"] = "impression"
                    impression_payload["spend_usd"] = spend_usd * 0.1  # fractional spend for impression
                    key_bytes = partner_id.encode('utf-8')
                    producer.send(KAFKA_TOPIC, value=impression_payload, key=key_bytes)
                    events_sent += 1
                    time.sleep(EVENT_DELAY_S)

                    # Click or conversion event
                    event_payload = base_payload.copy()
                    event_payload["event_type"] = "conversion" if sale_flag == 1 else "click"
                    # keep spend_usd as is
                    producer.send(KAFKA_TOPIC, value=event_payload, key=key_bytes)
                    events_sent += 1

                    if events_sent > 0 and events_sent % 1000 == 0:
                        logging.info("   ... sent %d events.", events_sent)
                    
                    time.sleep(EVENT_DELAY_S)

                except (ValueError, IndexError) as e:
                    logging.warning("⚠️ Skipping malformed line #%d. Error: %s", i + 1, e)
                    continue
        
        producer.flush()
        return events_sent
    except FileNotFoundError:
        logging.error("❌ ERROR: Input file not found at '%s'.", input_file)
        return 0
    except Exception as e:
        logging.error("❌ An unexpected error occurred while processing the file: %s", e, exc_info=True)
        return events_sent

def main():
    """Main execution function."""
    args = get_args()
    kafka_producer = None
    
    # Preview mode - don't need Kafka connection
    if args.preview_lines > 0:
        logging.info("🔍 Preview mode: showing %d sample events without sending to Kafka", args.preview_lines)
        events_sent = process_file_once(None, args.file, args.preview_lines)
        logging.info("✅ Preview complete. Displayed %d events.", events_sent)
        return
        
    try:
        kafka_producer = create_producer(args.broker)

        if args.loop:
            logging.info("🔁 Loop mode enabled. Streaming file continuously.")
            while True:
                events_sent = process_file_once(kafka_producer, args.file, args.preview_lines)
                if events_sent > 0:
                    logging.info("✅ Finished a full pass of the data file (%d events sent).", events_sent)
                logging.info("... Waiting %ds before next loop.", LOOP_DELAY_S)
                time.sleep(LOOP_DELAY_S)
        else:
            logging.info("📤 Single-pass mode: streaming file once then exiting.")
            events_sent = process_file_once(kafka_producer, args.file, args.preview_lines)
            logging.info("✅ Finished single pass (%d events sent). Exiting.", events_sent)

    except KeyboardInterrupt:
        logging.info("\n🛑 Stream manually interrupted by user.")
    finally:
        if kafka_producer:
            logging.info("👋 Closing Kafka producer...")
            kafka_producer.flush()
            kafka_producer.close()
            logging.info("✅ Kafka producer closed.")

if __name__ == "__main__":
    main() 
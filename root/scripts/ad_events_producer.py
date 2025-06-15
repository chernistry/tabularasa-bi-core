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

# --- Field indices for the raw Criteo TSV data ---
IDX_SALE = 0
IDX_SALES_AMOUNT = 1
IDX_CLICK_TIMESTAMP = 3
IDX_PRODUCT_PRICE = 5
IDX_PARTNER_ID = 15
IDX_USER_ID = 16

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

def process_file_once(producer: KafkaProducer, input_file: str) -> int:
    """Reads the data file once and sends events to Kafka."""
    events_sent = 0
    try:
        with open(input_file, 'r') as f:
            for i, line in enumerate(f):
                parts = line.strip().split('\t')
                if len(parts) < 17:
                    continue

                try:
                    sale = int(parts[IDX_SALE])
                    click_ts = int(parts[IDX_CLICK_TIMESTAMP]) if parts[IDX_CLICK_TIMESTAMP] != '-1' else None
                    partner_id = parts[IDX_PARTNER_ID]
                    user_id = parts[IDX_USER_ID]

                    if not click_ts or partner_id == '-1' or user_id == '-1':
                        continue
                    
                    sales_amount = float(parts[IDX_SALES_AMOUNT]) if parts[IDX_SALES_AMOUNT] != '-1' else 0.0
                    product_price = float(parts[IDX_PRODUCT_PRICE]) if parts[IDX_PRODUCT_PRICE] != '-1' else 0.0
                    bid_amount_usd = sales_amount if sales_amount > 0 else product_price

                    live_timestamp = datetime.utcnow().isoformat() + 'Z'
                    
                    # Для каждого клика сначала генерируем impression
                    impression_event = {
                        "timestamp": live_timestamp,
                        "campaign_id": partner_id,
                        "event_type": "impression",
                        "user_id": user_id,
                        "bid_amount_usd": bid_amount_usd * 0.1
                    }
                    key = partner_id.encode('utf-8')
                    producer.send(KAFKA_TOPIC, value=impression_event, key=key)
                    events_sent += 1
                    time.sleep(EVENT_DELAY_S)
                    
                    # Затем отправляем событие клика или конверсии
                    event = {
                        "timestamp": live_timestamp,
                        "campaign_id": partner_id,
                        "event_type": "conversion" if sale == 1 else "click",
                        "user_id": user_id,
                        "bid_amount_usd": bid_amount_usd
                    }
                    key = partner_id.encode('utf-8')
                    producer.send(KAFKA_TOPIC, value=event, key=key)
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
    try:
        kafka_producer = create_producer(args.broker)

        if args.loop:
            logging.info("🔁 Loop mode enabled. Streaming file continuously.")
            while True:
                events_sent = process_file_once(kafka_producer, args.file)
                if events_sent > 0:
                    logging.info("✅ Finished a full pass of the data file (%d events sent).", events_sent)
                logging.info("... Waiting %ds before next loop.", LOOP_DELAY_S)
                time.sleep(LOOP_DELAY_S)
        else:
            logging.info("📤 Single-pass mode: streaming file once then exiting.")
            events_sent = process_file_once(kafka_producer, args.file)
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
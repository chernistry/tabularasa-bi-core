# Ad Events Producer

This script simulates a real-time stream of ad events by parsing a raw data file and sending records to a Kafka topic in an infinite loop.

## Setup

It is crucial to use the correct Python libraries to avoid errors. The required library is `kafka-python`, not `kafka`.

1.  **Navigate to this directory:**
    ```bash
    cd root/scripts
    ```

2.  **Create a virtual environment (optional but recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Fix/Install dependencies:**
    Run these commands to remove the conflicting library and install the correct ones.
    ```bash
    pip uninstall -y kafka
    pip install -r requirements.txt
    ```

## Running the Stream

You can run the script with default settings or provide your own.

**Default:**
Connects to `localhost:9092` and uses data from `../data/CriteoSearchData`.
```bash
python ad_events_producer.py
```

**Custom:**
Specify a different Kafka broker or input file.
```bash
python ad_events_producer.py --broker YOUR_BROKER:9092 --file /path/to/your/data.tsv
``` 
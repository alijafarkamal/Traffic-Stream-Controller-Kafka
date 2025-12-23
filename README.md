# Real-Time Traffic Event Monitoring System

A real-time traffic monitoring system that processes vehicle events through Kafka, classifies traffic conditions using AI, and provides a live dashboard for monitoring and analysis.

## 🎯 What Problem Does This Solve?

**Problem**: Cities and traffic management systems need real-time insights into traffic conditions to:
- Detect accidents and congestion immediately
- Make data-driven decisions for traffic management
- Monitor traffic patterns and anomalies
- Respond quickly to traffic incidents

**Solution**: This system provides:
- **Real-time event streaming** via Kafka for high-throughput traffic data
- **AI-powered classification** to automatically categorize traffic conditions (normal, congested, accident, anomaly)
- **Live dashboard** for real-time monitoring and visualization
- **Scalable architecture** that can handle thousands of events per second

## 🏗️ Architecture

```
Vehicle Events → Kafka Producer → Kafka Topic → Kafka Consumer → AI Classification → Database → Dashboard
```

1. **Producer**: Generates simulated traffic events (vehicle ID, speed, location, timestamp)
2. **Kafka**: Message broker for reliable, scalable event streaming
3. **Consumer**: Processes events and classifies traffic conditions
4. **AI Classification**: Uses mock logic (default) or Gemini AI to classify traffic status
5. **Database**: Stores events and classifications in SQLite
6. **Dashboard**: Streamlit web UI for real-time monitoring and control

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- Docker (for Kafka)
- Git

### Installation

```bash
# 1. Clone and navigate to project
cd kafka-local

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start Kafka (Docker)
docker run -d --name zookeeper -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  confluentinc/cp-zookeeper:7.6.0

docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --link zookeeper:zookeeper \
  confluentinc/cp-kafka:7.6.0

# Wait 10-15 seconds for Kafka to be ready, then create topic
docker exec kafka kafka-topics --create --if-not-exists \
  --topic traffic-events --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### Configuration (Optional)

Create a `.env` file for Gemini API (only needed if using real AI):

```env
GEMINI_API_KEY=your_gemini_api_key_here
```

**Note**: The system uses **mock/hardcoded responses by default** (no API key needed, no costs).

### Running the Application

```bash
# Start the Streamlit dashboard
streamlit run streamlit_app.py
```

Open `http://localhost:8501` in your browser.

From the dashboard:
1. Click **"Start Producer"** to begin generating traffic events
2. Click **"Start Consumer"** to process and classify events
3. Watch real-time logs, metrics, and visualizations update automatically

## 📊 Features

### Real-Time Monitoring
- Live event streaming and processing
- Real-time dashboard with auto-refresh
- Color-coded logs for easy monitoring
- System health indicators (Kafka, Database, Processes)

### AI-Powered Classification
- **Mock Mode (Default)**: Fast, cost-free classification based on speed thresholds:
  - Speed < 5 km/h → "accident"
  - Speed < 20 km/h → "congested"
  - Speed 20-80 km/h → "normal"
  - Speed > 80 km/h → "anomaly"
- **Gemini AI Mode**: Real AI classification (optional, requires API key)

### Data Persistence
- SQLite database for structured storage
- CSV logging for easy analysis
- Historical data visualization

### Control & Configuration
- Start/stop producer and consumer from UI
- Adjust event generation rate
- Toggle between mock and real AI
- Enable/disable CSV logging

## 🔧 Configuration Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `EVENTS_PER_SECOND` | `10` | Number of events generated per second |
| `USE_REAL_GEMINI` | `0` | Set to `1` to use real Gemini API (requires API key) |
| `DRY_RUN` | `0` | Set to `1` to skip all classification |
| `CSV_LOG_ENABLED` | `1` | Set to `0` to disable CSV logging |
| `VERBOSE` | `0` | Set to `1` for detailed logging |

## 📁 Project Structure

```
kafka-local/
├── producer.py          # Kafka producer (generates traffic events)
├── consumer.py          # Kafka consumer (processes & classifies events)
├── streamlit_app.py     # Web dashboard UI
├── requirements.txt     # Python dependencies
├── .env                 # Environment variables (create this, not in git)
├── traffic_events.db    # SQLite database (auto-created)
├── traffic_events_log.csv  # CSV log (auto-created)
├── producer.log         # Producer logs
└── consumer.log         # Consumer logs
```

## 🛠️ Development

### Running Components Manually

```bash
# Producer (generates events)
python producer.py

# Consumer (processes events)
python consumer.py
```

### Testing

```bash
# Test Gemini API connection (optional)
python test_gemini.py
```

## 📝 Notes

- **Default Mode**: Uses mock responses (no API costs, no rate limits)
- **Kafka**: Requires Docker containers to be running
- **Database**: SQLite file is created automatically
- **Logs**: All output is logged to `.log` files for debugging

## 🔒 Security

- `.env` file is gitignored (never commit API keys)
- Mock mode is default (no external API calls)
- Dashboard runs on localhost by default

## 📄 License

This project is part of a hackathon/demo project.

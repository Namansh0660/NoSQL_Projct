# NoSQL Knowledge Graph Project

A comprehensive real-time knowledge graph system for academic papers with MongoDB Atlas sharding, Apache Kafka streaming, and interactive Streamlit dashboard. This project implements a complete pipeline for building and visualizing a knowledge graph of academic papers using NoSQL technologies.

## 🎯 Features

- **Real-time Data Ingestion**: Fetches papers from ArXiv, PubMed, and CrossRef APIs
- **Knowledge Graph Construction**: Automatically builds relationships between papers, authors, institutions, and concepts
- **Interactive Dashboard**: Real-time visualization of knowledge graph with insights and metrics
- **Sharding Analysis**: Implements and benchmarks different MongoDB sharding strategies
- **Streaming Pipeline**: Kafka-based data processing for scalable ingestion
- **Docker Integration**: Complete containerized setup for easy deployment

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   API       │    │    Kafka    │    │  Knowledge  │
│  Sources    │ -> │   Stream    │ -> │   Graph     │
│ (ArXiv,etc) │    │  Pipeline   │    │  Builder    │
└─────────────┘    └─────────────┘    └─────────────┘
                           │
                           ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Streamlit   │    │  MongoDB    │    │  Sharding   │
│ Dashboard   │ <- │   Atlas     │ <- │ Strategies  │
└─────────────┘    └─────────────┘    └─────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- MongoDB Atlas account (for production) or local MongoDB
- Python 3.8+ (for local development)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd NoSQL_Project
cp .env.example .env
```

### 2. Configure Environment

Edit `.env` file with your MongoDB Atlas credentials:

```env
MONGODB_USER=your_atlas_username
MONGODB_PASS=your_atlas_password
MONGODB_CLUSTER=your_atlas_cluster.mongodb.net
MONGODB_DB=NOSQL
```

For local development, you can use the provided MongoDB container by leaving the default settings.

### 3. Start the Complete System

```bash
./run.sh
```

This will start all services:
- MongoDB (local) or connect to Atlas
- Apache Kafka with Zookeeper
- Kafka Producer (API fetcher)
- Kafka Consumer (KG builder)
- Streamlit Dashboard
- Kafka UI
- MongoDB Express

### 4. Access the Dashboard

- **Streamlit Dashboard**: http://localhost:8501
- **Kafka UI**: http://localhost:8080
- **MongoDB Express**: http://localhost:8081

## 🧪 Testing and Benchmarking

### MongoDB Atlas Sharding

For production deployment with MongoDB Atlas:

```bash
python enable_atlas_sharding.py --connection-string "mongodb+srv://username:password@cluster.mongodb.net/NOSQL"
```

This script enables sharding for the database and collections with appropriate shard keys.

### Sharding Benchmarks

To benchmark different sharding strategies:

```bash
python benchmark_sharding.py --connection-string "mongodb+srv://username:password@cluster.mongodb.net/NOSQL"
```

This generates performance metrics for various operations across different collections and sharding configurations.

### Pipeline Integration Test

To test the complete pipeline integration:

```bash
python test_pipeline.py --connection-string "mongodb+srv://username:password@cluster.mongodb.net/NOSQL" --bootstrap-servers "localhost:9092"
```

This runs the producer and consumer, monitors the process, and verifies that data is flowing correctly through the system.

## 📊 Dashboard Features

### Real-time Knowledge Graph Visualization
- Interactive network graph showing papers, authors, institutions, and concepts
- Real-time updates as new data is ingested
- Configurable node limits, layout types, and display options
- Node filtering by type and relation filtering
- Dynamic node sizing based on connections
- Detailed hover information including abstracts and keywords

### Sharding Performance Analysis
- Comparison of different sharding strategies:
  - Modulo Hashing
  - Consistent Hashing
  - Range-based Partitioning
- Performance metrics and benchmarks
- Load balancing analysis

### Data Ingestion Control
- Manual API fetching controls
- Real-time pipeline monitoring
- System status indicators
- Manual refresh button for immediate data updates

### Database Insights
- Live statistics (papers, nodes, edges)
- Recent papers table
- Growth metrics

## 🔧 Manual Setup (Development)

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Start Individual Components

1. **Start Local Services**:
   ```bash
   docker-compose up -d mongo kafka zookeeper
   ```

2. **Start Kafka Producer**:
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python ingestion/kafka_producer.py
   ```

3. **Start Kafka Consumer**:
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python ingestion/kafka_consumer_kg.py
   ```

4. **Start Dashboard**:
   ```bash
   streamlit run ui/app.py
   ```

## 🗂️ Sharding Strategies

The project implements and benchmarks three sharding strategies:

### 1. Modulo Hashing
- Simple hash-based distribution
- Good for uniform data distribution
- Fast routing decisions

### 2. Consistent Hashing
- Virtual nodes for better load balancing
- Minimal data movement when adding/removing shards
- Better handling of hot spots

### 3. Range-based Partitioning
- Partitions based on document properties (e.g., publication year)
- Good for range queries
- Natural data organization

## 📈 Performance Benchmarks

Run sharding benchmarks:

```bash
python benchmarks/sharding_bench.py
```

Or use the dashboard's benchmark feature for interactive analysis.

## 🐳 Docker Services

| Service | Port | Description |
|---------|------|-------------|
| nosql-app | 8501 | Main Streamlit application |
| kafka | 9092 | Apache Kafka broker |
| kafka-ui | 8080 | Kafka management UI |
| mongo | 27017 | MongoDB database |
| mongo-express | 8081 | MongoDB web interface |
| zookeeper | 2181 | Kafka coordination |

## 📁 Project Structure

```
NoSQL_Project/
├── api/                    # Database connection and API routes
├── benchmarks/            # Sharding performance benchmarks
├── ingestion/             # Kafka producers and consumers
├── kg_builder/            # Knowledge graph construction
├── mongo-init-scripts/    # MongoDB initialization
├── ui/                    # Streamlit dashboard
├── docker-compose.yml     # Complete Docker setup
├── Dockerfile            # Application container
├── requirements.txt      # Python dependencies
└── run.sh               # Startup script
```

## 🔍 Monitoring

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f nosql-app
docker-compose logs -f kafka-producer
docker-compose logs -f kafka-consumer
```

### Check Service Status

```bash
docker-compose ps
```

### Stop Services

```bash
docker-compose down
```

## 🛠️ Configuration

### Kafka Configuration
- **Bootstrap servers (local host apps)**: `localhost:9092`
- **Bootstrap servers (inside Docker containers)**: `kafka:29092`
- **Topic**: `raw_papers`
- **Auto-commit**: enabled

Set `KAFKA_BOOTSTRAP_SERVERS` accordingly for producers/consumers. The compose file sets `kafka:29092` for in-container services; running locally uses `localhost:9092`.

### MongoDB Configuration
- Database: `NOSQL`
- Collections: `papers`, `nodes`, `edges`
- Connection: Atlas or local container

### Sharding Configuration
- Default shards: 3
- Strategies: modulo, consistent, range
- Benchmark iterations: 2000

## 🚨 Troubleshooting

### Common Issues

1. **Docker not starting**: Ensure Docker is running and has sufficient resources
2. **MongoDB connection failed**: Check Atlas credentials in `.env` file
3. **Kafka connection timeout**: Wait for Kafka to fully initialize (30-60 seconds)
4. **Port conflicts**: Check if ports 8501, 9092, 27017 are available

### Reset Everything

```bash
docker-compose down -v
docker system prune -f
./run.sh
```

## 📚 API Data Sources

- **ArXiv**: Academic preprints in physics, mathematics, computer science
- **PubMed**: Biomedical literature database
- **CrossRef**: Scholarly publication metadata

## 🎛️ Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `MONGODB_URI` | MongoDB connection string | Atlas or local |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers | localhost:9092 |
| `NUM_SHARDS` | Number of shards for benchmarking | 3 |
| `BATCH_SIZE` | API fetch batch size | 10 |

## 📊 Performance Metrics

The system tracks:
- Query response times
- Throughput (operations/second)
- Load balancing efficiency
- Data distribution patterns
- Real-time ingestion rates

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review Docker and service logs
3. Ensure all prerequisites are met
4. Verify environment configuration

---

**Built with**: Python, Streamlit, MongoDB Atlas, Apache Kafka, Docker, NetworkX, Plotly

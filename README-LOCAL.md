# 🏠 Spotify Pipeline - Local Development Guide

This guide helps you run the **complete Spotify data pipeline locally** on your Mac using Docker, with optional Databricks Community Edition integration.

## 🚀 Quick Start (5 minutes)

```bash
# 1. Setup local environment
make local-setup

# 2. Start services (PostgreSQL + Airflow + Kafka)
make local-up

# 3. Generate sample data and run pipeline
make local-dev
```

**That's it!** Your pipeline is running locally. Access Airflow at http://localhost:8080

## 📋 What You Get

### **Local Services**
- **PostgreSQL**: Operational database (localhost:5432)
- **Airflow**: Orchestration UI (http://localhost:8080)
- **Kafka**: Streaming platform (localhost:9092)
- **Scio Pipeline**: Scala/Beam data processing
- **DBT**: SQL transformations

### **Optional Cloud Integration**
- **Databricks Community Edition**: Free single-node cluster for testing
- **Azure resources**: Full cloud deployment when ready

## 🛠️ Detailed Setup

### Prerequisites
- Docker Desktop installed
- At least 8GB RAM available
- 5GB disk space

### Step 1: Environment Setup
```bash
# Clone and enter directory
git clone <your-repo>
cd spotify_pipeline

# Setup local environment
make local-setup

# Review/edit .env file (created from .env.local)
vim .env
```

### Step 2: Start Services
```bash
# Start all local services
make local-up

# Check status
make local-status

# View logs
make local-logs
```

### Step 3: Generate Test Data
```bash
# Generate sample Spotify data
make local-generate-data
```

### Step 4: Run Pipeline

**Option A: Full Pipeline via Airflow**
```bash
make local-pipeline
# Then go to http://localhost:8080 to monitor
```

**Option B: Individual Components**
```bash
# Run Scio processing only
make local-scio

# Run DBT transformations only  
make local-dbt
```

## 📊 Data Flow (Local)

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Sample Data    │───▶│  PostgreSQL      │───▶│  Scio Pipeline  │
│  Generator      │    │  (Raw Tables)    │    │  (Scala/Beam)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Analytics      │◀───│  DBT Transform   │◀───│ Parquet Files   │
│  Tables         │    │  (SQL)           │    │ (./data/processed)│
└─────────────────┘    └──────────────────┘    └─────────────────┘
         ▲
         │
┌─────────────────┐
│    Airflow      │
│ (Orchestrates)  │
└─────────────────┘
```

## 📁 File Structure

```
spotify_pipeline/
├── .env                          # Local environment variables
├── docker-compose.local.yml      # Local services definition
├── dags/local_spotify_etl_dag.py # Local Airflow DAG
├── data/                         # Local data storage
│   ├── raw/                      # Raw input files
│   ├── processed/                # Parquet output files  
│   └── analytics/                # Final analytics data
├── scripts/scio_pipelines/       # Scala/Scio processing
└── dbt/                          # SQL transformations
```

## 🎯 Available Commands

### Core Local Commands
```bash
make local-setup      # Initial setup
make local-up         # Start services
make local-down       # Stop services
make local-pipeline   # Run full pipeline
make local-test       # Test setup
```

### Component Commands
```bash
make local-scio       # Run Scio pipeline
make local-dbt        # Run DBT (PostgreSQL)
make local-generate-data  # Generate sample data
make local-clean      # Clean data/logs
```

### Monitoring Commands
```bash
make local-status     # Service status
make local-logs       # View logs
```

## 🔧 Configuration

### Database Connection
- **Host**: localhost:5432
- **Database**: spotify_data
- **User/Password**: defined in .env

### Airflow Access
- **URL**: http://localhost:8080
- **User**: admin
- **Password**: admin

### Data Locations
- **Input**: `./data/raw/`
- **Processing**: `./data/processed/`
- **Output**: PostgreSQL analytics schema

## 🐛 Troubleshooting

### Common Issues

**1. Port conflicts**
```bash
# Check what's using ports
lsof -i :8080  # Airflow
lsof -i :5432  # PostgreSQL

# Stop conflicting services or change ports in docker-compose.local.yml
```

**2. Memory issues**
```bash
# Increase Docker memory to 8GB in Docker Desktop settings
# Or reduce services by commenting out kafka in docker-compose.local.yml
```

**3. Permission issues**
```bash
# Fix data directory permissions
sudo chown -R $(whoami) data/ logs/
```

**4. Database connection failures**
```bash
# Check if postgres is ready
make local-status
docker-compose -f docker-compose.local.yml exec postgres pg_isready
```

### Logs and Debugging
```bash
# View specific service logs
docker-compose -f docker-compose.local.yml logs postgres
docker-compose -f docker-compose.local.yml logs airflow-webserver
docker-compose -f docker-compose.local.yml logs scio-runner

# Execute into containers for debugging
docker-compose -f docker-compose.local.yml exec postgres psql -U spotify_user -d spotify_data
docker-compose -f docker-compose.local.yml exec airflow-webserver bash
```

## ☁️ Databricks Community Edition (Optional)

### Setup
1. Sign up at https://community.cloud.databricks.com/
2. Create a cluster (free single-node)
3. Generate access token
4. Update .env with Databricks credentials

### Test DBT with Databricks
```bash
# Test connection
cd dbt && dbt debug --target databricks_community

# Run transformations on Databricks
make local-dbt-databricks
```

## 🚀 Next Steps

### Ready for Production?
When your local pipeline works perfectly:

```bash
# Deploy to Azure
make azure-setup

# Or deploy to Kubernetes
make k8s-deploy
```

### Need More Scale?
- **Local Spark**: Add Spark container for larger datasets
- **Databricks**: Upgrade to paid cluster
- **Azure**: Move to cloud for unlimited scale

## 💡 Pro Tips

1. **Fast iteration**: Use `make local-scio` to test processing changes quickly
2. **Data debugging**: Check `./data/processed/` for intermediate files
3. **SQL debugging**: Connect to PostgreSQL directly with any SQL client
4. **Airflow debugging**: Use the web UI to see task logs and retry failures
5. **Resource monitoring**: Use `docker stats` to monitor container resource usage

---

## 🆚 Local vs Cloud Comparison

| Feature | Local (Your Mac) | Azure Cloud |
|---------|------------------|-------------|
| **Setup Time** | 5 minutes | 30 minutes |
| **Cost** | Free | ~$50-200/month |
| **Data Limit** | ~10GB | Unlimited |
| **Processing Power** | Your Mac CPU | Scalable |
| **Storage** | Local SSD | Data Lake Gen2 |
| **Monitoring** | Local logs | Azure Monitor |
| **Collaboration** | Single user | Team access |
| **Production Ready** | Development only | Yes |

**Recommendation**: Start local, then move to Azure when you need scale! 🎯
# Databricks Community Edition Setup

## What is Databricks Community Edition?
- **Free tier** of Databricks with a single-node cluster
- **6GB RAM, 15GB storage** - perfect for development/testing
- **Supports Spark, SQL, Python, Scala, R**
- **Web-based notebooks** for interactive development
- **Can connect via JDBC** for DBT integration

## Setup Instructions

### 1. Sign Up (Free)
1. Go to https://community.cloud.databricks.com/
2. Click "Sign up for Community Edition"
3. Use your email to create account
4. Verify email and login

### 2. Create Cluster
1. In Databricks workspace, go to "Compute"
2. Click "Create Cluster"
3. Name: `spotify-dev-cluster`
4. Use defaults (single node, latest Spark version)
5. Click "Create Cluster" (takes 3-5 minutes)

### 3. Get Connection Details
1. Go to "SQL Warehouses" (or create one)
2. Click on your warehouse → "Connection details"
3. Copy the JDBC URL, looks like:
   ```
   jdbc:databricks://community.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=/sql/1.0/warehouses/your-warehouse-id;AuthMech=3;
   ```

### 4. Generate Access Token
1. Click your profile icon → "User Settings"
2. Go to "Access tokens" tab
3. Click "Generate new token"
4. Name: `spotify-pipeline-token`
5. Copy the token (save it securely!)

### 5. Update .env.local
```bash
DATABRICKS_WORKSPACE_URL=community.cloud.databricks.com
DATABRICKS_ACCESS_TOKEN=your_token_here
DATABRICKS_WAREHOUSE_ID=your_warehouse_id_here
```

## Testing Connection
```bash
# Test with DBT
cd dbt
dbt debug --target databricks_community
```

## Limitations
- **Single node**: Limited processing power
- **6GB RAM**: Good for small datasets (< 1M records)
- **15GB storage**: Store only essential data
- **4 hours max**: Cluster auto-terminates after inactivity
- **No premium features**: Basic Spark functionality only

## Alternative: Local Spark
If Databricks Community Edition is too limited:
```bash
# Install Spark locally with Docker
docker run -it --rm \
  -p 4040:4040 \
  -p 8080:8080 \
  -v $(pwd)/data:/data \
  apache/spark:3.5.0 \
  /opt/spark/bin/spark-shell
```
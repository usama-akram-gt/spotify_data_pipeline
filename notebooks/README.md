# Jupyter Notebooks Directory

This directory contains Jupyter notebooks for exploring, analyzing, and visualizing data from the Spotify data pipeline.

## Purpose

These notebooks serve multiple purposes:

- Exploratory data analysis
- Pipeline development and testing
- Ad-hoc investigations
- Visualization prototyping
- Algorithm development and testing
- Documentation and knowledge sharing

## Notebook Categories

### 1. Data Exploration

- `01_streaming_history_exploration.ipynb` - Analyze streaming history data
- `02_user_behavior_patterns.ipynb` - Identify user listening patterns
- `03_content_popularity_analysis.ipynb` - Explore track and artist popularity

### 2. Pipeline Development

- `pipeline_beam_prototype.ipynb` - Prototype Apache Beam transforms
- `pipeline_kafka_testing.ipynb` - Test Kafka producers and consumers
- `pipeline_data_quality_checks.ipynb` - Develop data quality validation

### 3. Visualization

- `viz_user_engagement_dashboard.ipynb` - Prototype user engagement visualizations
- `viz_content_trends.ipynb` - Visualize content popularity trends
- `viz_geographic_patterns.ipynb` - Map-based visualizations of listening patterns

### 4. Machine Learning

- `ml_recommendation_engine.ipynb` - Develop recommendation algorithms
- `ml_churn_prediction.ipynb` - Predict user churn
- `ml_content_clustering.ipynb` - Cluster tracks by audio features

## Usage

### Environment Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Register the virtual environment as a Jupyter kernel:
   ```bash
   python -m ipykernel install --user --name=spotify-pipeline
   ```

4. Start Jupyter:
   ```bash
   jupyter lab
   ```

### Connecting to Data

Notebooks connect to the Spotify pipeline database using the following methods:

1. Direct PostgreSQL connection:
   ```python
   import psycopg2
   conn = psycopg2.connect(
       host="localhost",
       database="spotify",
       user="postgres",
       password="postgres"
   )
   ```

2. Using SQLAlchemy:
   ```python
   from sqlalchemy import create_engine
   engine = create_engine('postgresql://postgres:postgres@localhost:5432/spotify')
   ```

3. Using the project's utility module:
   ```python
   import sys
   sys.path.append('../scripts')
   from utils.db_utils import get_db_connection
   
   conn = get_db_connection()
   ```

## Best Practices

1. **Documentation**: Include markdown cells explaining the purpose and methodology
2. **Code Organization**: Keep code modular and well-structured
3. **Parameter Isolation**: Define parameters at the beginning of the notebook
4. **Error Handling**: Include proper error handling for database operations
5. **Output Management**: Clear large outputs before committing to git
6. **Reproducibility**: Ensure notebooks can be run from top to bottom consistently
7. **Dependencies**: Document all external dependencies

## Contributing

When adding new notebooks:

1. Follow the naming convention: `{category}_{purpose}.ipynb`
2. Add a clear description at the top of the notebook
3. Update this README with the new notebook information
4. Ensure all database credentials are parameterized, not hardcoded 
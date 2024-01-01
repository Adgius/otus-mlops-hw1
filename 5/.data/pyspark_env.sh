python -m venv pyspark_venv
source pyspark_venv/bin/activate
pip install pyarrow mlflow==1.27.0 urllib3==1.25.8 pyyaml venv-pack
venv-pack -o pyspark_venv.tar.gz
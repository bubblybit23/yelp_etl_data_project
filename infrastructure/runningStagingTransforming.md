# Command 1
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/jobs/create_staging_layer.py

# Command 2  
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit /opt/bitnami/spark/jobs/create_transformation_layer.py
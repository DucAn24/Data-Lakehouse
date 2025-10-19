FROM apache/spark:3.4.0-python3

USER root

# Install prerequisites
RUN apt-get update && apt-get install -y curl python3-pip

# Install Python packages for ETL scripts
RUN pip3 install kafka-python==2.0.2 requests==2.31.0 boto3==1.26.137

# Download and install required JARs
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.2/spark-token-provider-kafka-0-10_2.12-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && mv s3-2.18.41.jar /opt/bitnami/spark/jars \
    && mv aws-java-sdk-1.12.367.jar /opt/bitnami/spark/jars \
    && mv delta-core_2.12-2.3.0.jar /opt/bitnami/spark/jars \
    && mv delta-storage-2.3.0.jar /opt/bitnami/spark/jars \
    && mv hadoop-aws-3.3.2.jar /opt/bitnami/spark/jars \
    && mv spark-sql-kafka-0-10_2.12-3.3.2.jar /opt/bitnami/spark/jars \
    && mv kafka-clients-3.3.2.jar /opt/bitnami/spark/jars \
    && mv spark-token-provider-kafka-0-10_2.12-3.3.2.jar /opt/bitnami/spark/jars \
    && mv commons-pool2-2.11.1.jar /opt/bitnami/spark/jars
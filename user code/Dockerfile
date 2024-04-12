FROM myartifactory/docker-local/reddagster-base:1.6.2 # name of my base image an version

ARG DATE=notset

WORKDIR /app
COPY requirements.txt ./ 
ENV DAGSTER_HOME /dagster_home
# Setup folders and write permissions for dagster_home and app
USER root
RUN mkdir -p /dagster_home && \
    mkdir -p /app 

# Here we copy our CodeLocation
COPY tutorial /app/tutorial
RUN chmod -R +x /app && \
    chmod -R +x  /dagster_home 

RUN pip install --no-cache-dir -r requirements.txt


# Run dagster gRPC server on port 3030
EXPOSE 3030

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3030", "-m", "tutorial" ]

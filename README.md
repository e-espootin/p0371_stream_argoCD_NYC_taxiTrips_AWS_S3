# stream generated data based on TLC trip data using Kafka, ArgoCD into AWS S3

## Project Summary

This project generates data based on TLC trip data using the Faker library within a built container. It includes two Docker images for the producer and consumer. These containers are pulled from Docker Hub based on ArgoCD configurations in the repository. A number of containers, based on the configuration file, will be created for producers and consumers. Data is generated continuously and saved in AWS S3. Additionally, the Kafka Server will be run on a local Ubuntu machine along with Minikube and ArgoCD.


![Project Architecture](path/to/your/image.png)
# Deploying FOCUS-converter on Kubernetes with Parallel Processing

###       * As-is reference architecture for deploying FOCUS converter on Kubernetes with parallel processing.

## Introduction

Deploying converter on Kubernetes can enhance scalability, reliability, and manageability. This document explores the
deployment of FOCUS converter tool on Kubernetes, focusing on leveraging data partitions for parallel processing to
achieve high performance and efficiency.

## Benefits of Kubernetes for converter tool

Kubernetes, an open-source platform for automating the deployment, scaling, and management of containerized
applications, offers several benefits for converter tool deployment, including the following:

- **Scalability:** Automatically scale your conversion jobs horizontally by increasing or decreasing the number of pods
  based
  on the workload.
- **Resource Efficiency:** Optimize the utilization of underlying resources by dynamically allocating them based on the
  demands of the conversion jobs.
- **Portability:** Kubernetes abstracts the underlying infrastructure, making your ETL workflows portable across cloud
  providers and on-premises environments.

## Parallel Processing with Data Partitions

Parallel processing is a method of dividing the data into smaller partitions and processing these partitions
simultaneously. This approach can significantly reduce the overall processing time of ETL tasks. Converter
tool can be designed to:

1. **Extract** data from various sources and partition it based on a predefined logic, such as date ranges or key
   values.
2. **Transform** the data in parallel, where each partition is processed by a separate instance of the tool running in
   its own container.
3. **Load** the transformed data into the destination system, which can also be done in parallel for each partition to
   optimize performance.

## Kubernetes Deployment Architecture

The deployment architecture on Kubernetes involves several components:

- **Pods:** Each instance of the converter runs in its own pod, allowing for parallel processing of data
  partitions.
- **Deployments:** A Kubernetes Deployment manages the pods, ensuring that the specified number of pods are always
  running and redeploying them in case of failures. [Example](deployment_docs/deployment.md)
- **Persistent Volumes:** Persistent storage is used to store the input and output data, as well as any intermediate
  state, ensuring data persistence across pod restarts.
- **Jobs:** For conversion tasks that need to be executed as a one-off job, Kubernetes
  Jobs can manage these execution patterns.

## Remote data read and write

- **Object storage:** The input and output data can be stored in object storage, such as Amazon S3 or Google Cloud
  Storage, which can be mounted as a persistent volume in the Kubernetes cluster.
- **Cloud sql:** The input and output data can be stored in cloud sql, such as Amazon RDS or Google Cloud SQL, which can
  be accessed by the converter tool using the appropriate database drivers.
- **GCP BigQuery:** The input and output data can be stored in GCP BigQuery, which can be accessed by the converter tool
  using the appropriate BigQuery client libraries.

## Cloud environment authentication

* Provider specific authentication guides here
* Use implicit credentials for the cloud provider for data access.

## Conclusion

Deploying FOCUS converter tool on Kubernetes and utilizing data partitioning for parallel processing can greatly
enhance the efficiency and reliability of data pipeline operations. This approach provides a scalable, fault-tolerant,
and resource-efficient solution that can meet the demanding needs of modern data workflows.


Indexing USGS Collection 2 Data
===============================

Generating Metadata and Converting to COGs
------------------------------------------
[This document is based on [internal documents](https://bitbucket.org/geoscienceaustralia/dea-internal-docs/src/master/procedures/indexing_collection2.rst?mode=edit) and some of the steps need to be modified for use outside of Geoscience Australia.]

Generating metadata and converting to COG's is done using [a custom branch of
eo-datasets3](https://github.com/GeoscienceAustralia/eo-datasets/tree/network-fs-support-newbase)
that still needs a little bit of cleanup before merging.


It includes support for:

> -   Reading and writing from S3
> -   Converting USGS Collection 2 Datasets into COGs with ODC eo3
>     metadata using the `eo3-prepare usgs-col2` command.

    $ eo3-prepare usgs-col2 --output-base ${OUTPUT_BASE} ${MESSAGE_BODY} && aws sqs delete-message --queue-url ${QUEUE_URL} --receipt-handle ${RECEIPT_HANDLE}

1.  Find all the datasets to process:

        $ s3-find s3://ga-africa-provisional/nigeria-2018-08-21/collection2/level2/standard/oli-tirs/2018/**/*MTL.txt > nigeria-datasets.txt

2.  Create an SQS queue with an associated DLQ using the AWS Console.
3.  Send each dataset to the SQS:

        $ QUEUE_URL=$(aws sqs create-queue --queue-name collection-2-nigeria | jq -r '.QueueUrl')
        $ cat nigeria-datasets.txt | xargs -n 1 -I'{}' aws sqs send-message --queue-url $QUEUE_URL --message-body '{}'

4.  Spin up a K8s Job to process them; 
``` {.sourceCode .console}
$ kubectl apply -f eo-datasets-job.yaml
```

Monitoring progress and logs of a K8s Job
-----------------------------------------

Find the job we\'re interested in.

``` {.sourceCode .console}
aws-box:~ $ kubectl get jobs
NAME                                                  COMPLETIONS   DURATION   AGE
cubedash-datacube-dashboard-update-index-1569434400   1/1           5s         5h48m
eo-datasets-cogging-test                              8/1 of 8      6h26m      9d
ls5-update-datacube-index                             1/1           11h        48d
ls7-update-datacube-index                             1/1           30h        48d
ls8-update-datacube-index                             1/1           13h        48d
```

List the pods within that job using a [label
selector](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)

``` {.sourceCode .console}
aws-box:~ $ kubectl describe job eo-datasets-cogging-test
Name:           eo-datasets-cogging-test
Namespace:      default
Selector:       controller-uid=6c4eb414-d84d-11e9-8ebd-069439c81b38
Labels:         app=eo-datasets
Annotations:    kubectl.kubernetes.io/last-applied-configuration:
                  {"apiVersion":"batch/v1","kind":"Job","metadata":{"annotations":{},"labels":{"app":"eo-datasets"},"name":"eo-datasets-cogging-test","names...
Parallelism:    8
Completions:    <unset>
Start Time:     Mon, 16 Sep 2019 16:44:25 +1000
Completed At:   Mon, 16 Sep 2019 23:10:29 +1000
Duration:       6h26m
Pods Statuses:  0 Running / 8 Succeeded / 1 Failed
Pod Template:
  Labels:       app=eo-datasets
                controller-uid=6c4eb414-d84d-11e9-8ebd-069439c81b38
                job-name=eo-datasets-cogging-test
  Annotations:  iam.amazonaws.com/role: deafrica-eks-c2-indexing
  Containers:
   eo-datasets-container:
    Image:      opendatacube/eo-datasets:latest
    Port:       <none>
    Host Port:  <none>
    Args:
      /usr/bin/env
      python3
      /opt/app/sqs-consume.py
      --queue-url
      $(QUEUE_URL)
      --message-timeout
      300
      --
      eo3-prepare
      usgs-col2
      --output-base
      $(OUTPUT_BASE)
    Limits:
      memory:  1548Mi
    Requests:
      cpu:     1
      memory:  1208Mi
    Environment:
      QUEUE_URL:           https://us-west-2.queue.amazonaws.com/565417506782/collection-2-nigeria
      OUTPUT_BASE:         s3://deafrica-collection2-testing/nigeria/
      AWS_DEFAULT_REGION:  us-west-2
    Mounts:                <none>
  Volumes:                 <none>
Events:                    <none>

aws-box:~ $ kubectl get pods -l app=eo-datasets
NAME                             READY   STATUS      RESTARTS   AGE
eo-datasets-cogging-test-2975f   0/1     Error       0          9d
eo-datasets-cogging-test-7f4gq   0/1     Completed   0          9d
eo-datasets-cogging-test-dkrhr   0/1     Completed   0          9d
eo-datasets-cogging-test-fgqm8   0/1     Completed   0          9d
eo-datasets-cogging-test-n2967   0/1     Completed   0          9d
eo-datasets-cogging-test-rv2hn   0/1     Completed   0          9d
eo-datasets-cogging-test-t49vb   0/1     Completed   0          9d
eo-datasets-cogging-test-xw2p5   0/1     Completed   0          9d
eo-datasets-cogging-test-z6hkj   0/1     Completed   0          9d
```

Output logs for the entire job:

    aws-box:~ $ kubectl logs -l app=eo-datasets

Save the logs from each pod in the job into a separate text file:

    aws-box:~ $ for i in `kubectl get pods -l app=eo-datasets | tail +2 | cut -d\  -f1`; do kubectl logs $i > $i.txt; echo $i; done

Indexing into an ODC Database
-----------------------------

1.  Find the PostgreSQL server:

        $ kubectl describe pod ows-datacube-dev-6ffd9fcb6c-zk62t
        ...
              DB_HOSTNAME:            database.local
        ...

2.  Launch a temporary Pod to run the indexing and connect to it:

        user@box:~$ wget https://github.com/GeoscienceAustralia/landsat-to-cog/raw/master/k8s/user-africa-dev-pod.yaml
        user@box:~$ kubectl apply -f user-africa-dev-pod.yaml
        user@box:~$ kubectl exec -it user-africa-dev-pod -- bash -l

3.  Install required tools (Inside the temporary pod):

        # apt-get update
        # apt-get install iputils-ping bind9-host postgresql-client

4.  Create and initialise a new ODC database

#### Note

There are [instructions and Helm configurations
available](https://github.com/opendatacube/datacube-k8s-eks/tree/master/jobs)
inside the [datacube-k8s-eks
repo](https://github.com/opendatacube/datacube-k8s-eks) which can also
be followed for creating and initialising databases. We should
consolidate our instructions and processes.


    root@user-africa-dev-pod:~# PGPASSWORD=$ADMIN_PASSWORD psql -h $DB_HOSTNAME -p $DB_PORT -U $ADMIN_USERNAME $DB_DATABASE

    africa=> create database usgs_collection2;
    CREATE DATABASE

    africa=> exit

    root@user-africa-dev-pod:~# cat > /opt/custom-config.conf
    [datacube]
    db_database: usgs_collection2
    db_hostname: database.local
    db_username: superuser
    db_password: **************
    db_port: 5432
    ^D

    root@user-africa-dev-pod:~# datacube system check
    Version:       1.7+89.gace03543.dirty
    Config files:  /opt/custom-config.conf
    Host:          database.local:5432
    Database:      usgs_collection2
    User:          superuser
    Environment:   None
    Index Driver:  default

    Valid connection:       Database not initialised:

    No DB schema exists. Have you run init?
            datacube system init

    root@user-africa-dev-pod:~# datacube system init --no-init-users
    Initialising database...
    Created.
    Checking indexes/views.
    Done.

5.  Install some extra tools for indexing:

        root@user-africa-dev-pod:/opt/odc# pip install --extra-index-url="https://packages.dea.gadevs.ga" \
        odc-apps-cloud      odc-apps-dc-tools

6.  Add the MetadataType and Product:

        root@user-africa-dev-pod:/opt/odc# datacube metadata add https://github.com/digitalearthafrica/config/raw/usgs-collection2/products/eo3_landsat_ard.odc-type.yaml
        root@user-africa-dev-pod:/opt/odc# datacube product add https://github.com/digitalearthafrica/config/raw/usgs-collection2/products/usgs-level2-collection2-sample.odc-product.yaml

7.  Find and index the datasets:

        $ s3-find s3://<s3 bucket path for a single product>/*/*.yaml | s3-to-tar | dc-index-from-tar --eo3

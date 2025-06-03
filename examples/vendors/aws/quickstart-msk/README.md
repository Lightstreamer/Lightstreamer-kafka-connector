# Quick Start with MSK

This folder contains a variant of the [_Quick Start SSL_](../../../quickstart-ssl/README.md#quick-start-ssl) app configured to use [_Amazon MSK_](https://aws.amazon.com/msk/) as the target Kafka cluster.

## Requirements

- AWS CLI installed and configured with appropriate permissions
- Basic knowledge of AWS services, especially MSK

## Set up the MSK Cluster

1. Create an MSK Provisioned cluster following the [_Getting started_](https://docs.aws.amazon.com/msk/latest/developerguide/getting-started.html) with these specific settings:

   - **Enabled public access** - Allows connections from outside the AWS network

   - **Enable IAM role-based authentication** - Secures access using AWS Identity and Access Management

   - **Custom cluster configuration:**:

     ```properties
     auto.create.topics.enable=true
     num.partitions=1
     allow.everyone.if.no.acl.found=false
     ```

2. Create an IAM policy for Kafka access permissions:

   - Create a file named `kafka-policy.json` with the following content:
    ```json
    {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Effect": "Allow",
              "Action": [
                  "kafka-cluster:Connect",
                  "kafka-cluster:AlterCluster",
                  "kafka-cluster:DescribeCluster"
              ],
              "Resource": [
                  "<MSK Cluster ARN>"
              ]
          },
          {
              "Effect": "Allow",
              "Action": [
                  "kafka-cluster:*Topic*",
                  "kafka-cluster:WriteData",
                  "kafka-cluster:ReadData"
              ],
              "Resource": [
                  "arn:aws:kafka:<region>:<Account-ID>:topic/<MSK Cluster Name>/*"
              ]
          },
          {
              "Effect": "Allow",
              "Action": [
                  "kafka-cluster:AlterGroup",
                  "kafka-cluster:DescribeGroup"
              ],
              "Resource": [
                  "arn:aws:kafka:<region>:<Account-ID>:group/<MSK Cluster Name>/*"
              ]
          }
      ]
    }
    ```

    where you have to replace the following placeholders with your actual values:
    - `<MSK Cluster ARN>` - The full ARN of your MSK cluster
    - `<region>` - Your AWS region code (e.g., us-east-1)
    - `<Account-ID>` - Your 12-digit AWS account ID
    - `<MSK Cluster Name>` -  The name of your MSK cluster

   - Create the policy:
     ```sh
     aws iam create-policy \
         --policy-name kafka-policy \
         --policy-document file://kafka-policy.json
     ```

3. Create an IAM role that can be assumed by your user:

   - Create a file named `trust-kafka-policy.json` with this content::

     ```json
     {
         "Version": "2012-10-17",
         "Statement": [
             {
                 "Effect": "Allow",
                 "Principal": {
                     "AWS": "arn:aws:iam::<Account-ID>:user/<Username"
                 },
                 "Action": "sts:AssumeRole"
             }
         ]
     }
     ```

     where you have to replace the `arn:aws:iam::<Account-ID>:user/<Username>` with your AWS user's ARN.

   - Create the role:
     ```sh
     aws iam create-role --role-name kafka-cluster-role  --assume-role-policy-document file://trust-kafka-policy.json
     ```

4. Attach the policy created at step 2 to the IAM role:

   ```sh
   aws iam attach-role-policy --role-name kafka-cluster-role --policy-arn arn:aws:iam::468819131509:policy/kafka-policy
   ```
   
## Set Up the Docker Compose File

With respect to the [_Quick Start SSL_](../../../quickstart-ssl/README.md#quick-start-ssl) app, the [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Amazon MSK_ as follows:

- Removal of the `broker` service, because replaced by the remote cluster.
- _kafka-connector_:
  - Definition of new environment variables to configure remote endpoint and credentials in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:
    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
    ...
    ```
  - Adaption of [`adapters.xml`](./adapters.xml) to include the following:
    - Update of the parameter `bootstrap.servers` to the environment variable `bootstrap_server`:
      ```xml
      <param name="bootstrap.servers">$env.bootstrap_server</param>
      ```

    - Minimal encryption settings:
      ```xml
      <param name="encryption.enable">true</param>
      <param name="encryption.protocol">TLSv1.2</param>
      ```

    - Configuration of the authentication settings, with usage of the `AWS_MSK_IAM` mechanism and reference to an AWS credential profile name:
      ```xml
      <param name="authentication.enable">true</param>
      <param name="authentication.mechanism">AWS_MSK_AWS</param>
      <param name="authentication.iam.credential.profile.name">$env.username</param>
      ```

- _producer_:
   - Update of the parameter `--bootstrap-servers` to the environment variable `bootstrap_server`
   - Provisioning of the `producer.properties` configuration file to enable `SASL/SCRAM` over TLS, with username, password, and trust store password retrieved from the environment variables `username`, `password`, and `truststore_password`:
    
   ```yaml
   # Configure SASL/SCRAM mechanism
   sasl.mechanism=SCRAM-SHA-512
   # Enable SSL encryption
   security.protocol=SASL_SSL
   # JAAS configuration
   sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${username}" password="${password}";
   ```  

## Run

From this directory, run follow the command:

```sh
$ bootstrap_server=<bootstrap_server> ./start.sh 
```

where:
- `bootstrap_server` is the public endpoint associated with the IAM Authentication Type.

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).

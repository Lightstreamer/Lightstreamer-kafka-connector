# Quick Start with MSK

This folder contains a variant of the [_Quick Start SSL_](../../../quickstart-ssl/README.md#quick-start-ssl) app configured to use [_Amazon MSK_](https://aws.amazon.com/msk/) as the target Kafka cluster.

## Requirements

- AWS CLI installed
- AWS access keys of a temporary non-production IAM user with no specific permissions

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
                     "AWS": "arn:aws:iam::<Account-ID>:user/<Username>"
                 },
                 "Action": "sts:AssumeRole"
             }
         ]
     }
     ```

     where you have to replace the `arn:aws:iam::<Account-ID>:user/<Username>` with the your AWS user's ARN.

   - Create the role:
     ```sh
     aws iam create-role \
         --role-name kafka-cluster-role \
         --assume-role-policy-document file://trust-kafka-policy.json
     ```

4. Attach the policy created at step 2 to the IAM role:

   ```sh
   aws iam attach-role-policy \
       --role-name kafka-cluster-role 
       --policy-arn arn:aws:iam::<Account-ID>:policy/kafka-policy
   ```

   where you have to replace `<Account-ID>` with your 12-digit AWS account ID.
   
## Set Up the Docker Compose File

With respect to the [_Quick Start SSL_](../../../quickstart-ssl/README.md#quick-start-ssl) app, the [docker-compose.yml](docker-compose.yml) file has been revised to realize the integration with _Amazon MSK_ as follows:

- Removal of the `broker` service, because replaced by the remote cluster.
- _kafka-connector_:
  - Definition of new environment variables to configure remote endpoint in the `adapters.xml` through the _variable-expansion_ feature of Lightstreamer:

    ```yaml
    ...
    environment:
      - bootstrap_server=${bootstrap_server}
      ...
    ...
    ```

  - Definition of new environment variables to reference an AWS shared credentials file:
  
    ```yaml
    environment:
      ...
      - AWS_SHARED_CREDENTIALS_FILE=/lightstreamer/aws_credentials
      
    ```

  - Configuration of the reference to the AWS shared credentials:

    ```yaml
    configs:
      - source: aws_credentials
        target: /lightstreamer/aws_credentials
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
      <param name="authentication.iam.credential.profile.name">msk_client</param>
      ```

- _producer_:
   - Update of the parameter `--bootstrap-servers` to the environment variable `bootstrap_server`
   - Reference to the AWS shared credential files for configuring the authentication to MSK
  
- _configs_:

   - Provisioning of the `producer.properties` configuration file to enable the AWS IAM for authentication for the _producer_ service:
    
     ```yaml
     # Configure AWS_MSK_IAM mechanism
     sasl.mechanism=SCRAM-SHA-512
     # Enable SSL encryption
     security.protocol=SASL_SSL
     # JAAS configuration
     sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="msk_client";
     sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
     ``` 

  - Provisioning of the AWS shared credentials file to store the credentials of the IAM user along with the AWS credential profile name used by the _kafka-connector_ and _producer_ services:

    ```yaml
    aws_credentials:
      content: |
        [default]
        aws_access_key_id = ${aws_access_key_id}
        aws_secret_access_key = ${aws_secret_access_key}

        [msk_client]
        role_arn = ${role_arn}
        source_profile = default
    ```

## Run

From this directory, run follow the command:

```sh
$ bootstrap_server=<bootstrap_server> \
  role_arn=<role_arn> \
  aws_access_key_id=<aws_access_key_id> \
  aws_secret_access_key=<aws_secret_access_key> \
  ./start.sh 
```

where:
- `<bootstrap_server>` - The public bootstrap server endpoint associated with the IAM Authentication Type
- `<role_arn>` - The ARN of the IAM role you created at step 3 of the section [`Set Up the MSK Cluster`](#set-up-the-msk-cluster)
- `<aws_access_key_id>` - The AWS access key ID of the test user
- `<aws_secret_access_key>` - The AWS secret access key of the test user

Then, point your browser to [http://localhost:8080/QuickStart](http://localhost:8080/QuickStart).

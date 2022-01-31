### Data Generation for Container-Based Benchmarks in CI

The automation in our Buildkite pipelines that executes benchmarks using
docker-compose depends on the source data being in a particular format. To that
end, the goal of this README is to detail the process, step by step, so we can
always reproduce an archive that should work on the first attempt.

### Prerequisites:

* Have an Amazon Linux 2 EC2 instance in AWS
  * Be sure to provision a system with enough disk IOPS and memory, otherwise
    the process could freeze or take forever to complete. The data generator is
    memory-bound.
    * Suggested EBS specs:
      * Storage capacity: `1024GiB`
      * Volume type: `gp3`
    * Suggested EC2 Instance Type:
      * `r5.xlarge` (preferred)
      * `c5.2xlarge`

* Have a Data Generator binary built, or preferably a container image with the
  generator binary built already. This guide assumes you have a prebuilt binary
  that can be copied to the Linux host you've provisioned.

### Generating Portable Archive for Data Generated Automatically

1. SSH into the Amazon Linux 2 instance using the private key you specified
   during instance launch:

    `ssh ec2-user@10.3.181.24 -i ~/.ssh/readyset-devops.pem`

2. Install Docker. Reboot/logout if `ec2-user` is unable to connect to Docker
   socket, even after running the commands below.

    ```
    sudo yum update -y
    sudo amazon-linux-extras install docker
    sudo service docker start
    sudo usermod -a -G docker ec2-user
    ```

3. Create a directory on the host to serve as the bind mounted volume path for
   MySQL to write its data directory to:

    `mkdir -p /home/ec2-user/mysql-datadir/`

4. Create a docker-compose file to run our temporary MySQL database:

    >  **VERY IMPORTANT**:
    >    * Do NOT change `lower-case-table-names` as it could render the DB archive
    >      useless in CI. The value must be 1.

    ```
        version: "3.8"
        services:
          mysql:
            image: mysql:8.0
            ports:
              - 3306:3306
            command: --authentication_policy=mysql_native_password --lower-case-table-names=1
            environment:
              MYSQL_DATABASE: test
              MYSQL_HOST: 127.0.0.1
              MYSQL_ROOT_PASSWORD: root
            volumes:
              - /home/ec2-user/mysql-datadir:/var/lib/mysql
    ```

5. Start the MySQL database as a daemon, and make sure it comes up as expected:

    ```
    /usr/local/bin/docker-compose up -d
    docker ps
    ```

6. Import the irl.db file. You may need to get this from a member of the team.
   SCP it to the Linux EC2 or use vi to write it to the file system.

    `mysql --host 127.0.0.1 -uroot -proot test < irl.db`

7. Now that the base schema is in place, we're ready to focus on pushing data
   into it. For the next steps, we'll be using a temporary container to run the
   data_generator binary.

#### Pumping Synthetic Data into MySQL Container

1. Change directories on the Linux host to wherever the data_generator binary is
   located. Presumably, this was SCPed to the host.

2. Start an `ubuntu:20.04` container to run the data generator within:

    `docker run -it -v $(pwd):/usr/src/app -w /usr/src/app ubuntu:20.04
    /bin/bash`

3. Some setup is needed within the container. Run the following commands inside
   it:

    ```
    apt-get update
    apt-get install build-essential -y
    ```

4. Now we can kick off the data generation process itself, inside the container.
   Be sure the `data_generator` binary is in the pwd.

    `./data_generator --schema ./irl.db --database-url
    mysql://root:root@172.17.0.1:3306/test`

    * Note:
      * The `--schema` parameter may seem redundant, but it's used by the
        generator to synthesize data that aligns with the schema of the DB.
      * This is the same file that we manually imported in step 6 of this
        document's previous section.

5. Hopefully now you're seeing the data generator displaying progress updates in
   the UI. This is good.

6. Once the generator completes, you may exit the data generator container with
   `ctrl + d`.

7. Exec into the MySQL container, and run `mysqladmin stop` to gracefully shut
   down the database. Exit the container if needed.

#### Building Compressed Datadir Archive

1. On the Linux host, CD to where the datadir raw files are located. Let's tar
   up the files.

    ```
    tar -C $(pwd) -zcvf benchmark-datadir-<semver-here>.tgz .
    ```

#### New Datadir Deployment to CI:

1. After compressing the archive, you can either:
    * SCP it to your machine, and then into the
      `readyset-devops-assets-build-us-east-2` bucket within the build account.
    * Upload from EC2 directly to the `readyset-devops-assets-build-us-east-2` S3
      bucket.
      * This option requires that you configure an appropriate IAM instance
        profile for the Linux EC2 instance.

2. Update `BENCHMARK_DATADIR_S3_KEY` references in this repository to reflect
   the new S3 object path as needed.

3. Cleanup your EC2 instance and attached EBS volume.

#!/usr/bin/env bash

aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --applications Name=Hadoop Name=Spark \
Name=Zeppelin --bootstrap-actions '[{"Path":"s3://bootstrapcity/clus_booter.sh","Name":"Custom action"}]' \
--ec2-attributes '{"KeyName":"keylab","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-26b72d1a", \
"EmrManagedSlaveSecurityGroup":"sg-35be524a","EmrManagedMasterSecurityGroup":"sg-35be524a"}' \
--service-role EMR_DefaultRole --release-label emr-5.5.0 --name 'filetmigcluster' --instance-groups \
'[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core - 2"},
{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master - 1"}]' \
--scale-down-behavior TERMINATE_AT_INSTANCE_HOUR --region us-east-1

sudo yum install gcc python27 python27-devel postgresql-devel
sudo curl https://bootstrap.pypa.io/ez_setup.py -o - | sudo python27
sudo /usr/bin/easy_install-2.7 pip
sudo pip2.7 install psycopg2
#And just in case the above bootstrap fails...
sudo pip install ipython jupyter boto Flask -U
export PYSPARK_PYTHON=`which python3`
export PYSPARK_DRIVER_PYTHON=`which jupyter`
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='*' --NotebookApp.port=8888"
source /home/hadoop/.bashrc



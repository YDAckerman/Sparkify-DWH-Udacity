import boto3
from botocore.exceptions import ClientError
import json


class Handler:

    def __init__(self, config):
        """
        - get key and secret from configuration
        - create an internal copy of the configuration
        - create internal boto3 resources
        """
        self.config = config
        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')

        self.ec2 = boto3.resource('ec2',
                                  region_name="us-west-2",
                                  aws_access_key_id=KEY,
                                  aws_secret_access_key=SECRET
                                  )

        self.s3 = boto3.resource('s3',
                                 region_name="us-west-2",
                                 aws_access_key_id=KEY,
                                 aws_secret_access_key=SECRET
                                 )

        self.iam = boto3.client('iam',
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET,
                                region_name='us-west-2'
                                )

        self.redshift = boto3.client('redshift',
                                     region_name="us-west-2",
                                     aws_access_key_id=KEY,
                                     aws_secret_access_key=SECRET
                                     )

    def print_s3_contents(self, bucket_name):
        """
        - print the contents of the given bucket
        """
        s3_bucket = self.s3.Bucket(bucket_name)
        for obj in s3_bucket.objects.all():
            print(obj)
        pass

    def print_s3_json_object(self, bucket_name, object_name):
        """
        - print json object with the given name in the given bucket
        """
        obj = self.s3.Object(bucket_name, object_name)
        contents = obj.get()['Body'].read().decode('utf-8')
        print(json.loads(contents))
        pass

    def create_iam_role(self):
        """
        - get configuration information
        - check if IAM role already exists
        - if not, create new IAM role
        - set internal configuration information with the new 
          role name and policy
        - return role arn
        """
        IAM_ROLE_NAME = self.config.get('DWH', 'IAM_ROLE_NAME')
        ARN = self.config.get('IAM_ROLE', 'ARN')

        print("Checking for IAM Role")
        createRole = False
        try:
            role = self.iam.get_role(RoleName=IAM_ROLE_NAME)
        except ClientError as e:
            print(e)
            createRole = True

        if createRole:
            print("Creating IAM Role")
            try:
                self.iam.create_role(
                    Path='/',
                    RoleName=IAM_ROLE_NAME,
                    Description="Allows Redshift clusters to " +
                    "call AWS services on your behalf.",
                    AssumeRolePolicyDocument=json.dumps(
                        {'Statement': [{'Action': 'sts:AssumeRole',
                                        'Effect': 'Allow',
                                        'Principal': {'Service':
                                                      'redshift.amazonaws.com'}}],
                         'Version': '2012-10-17'})
                )
                print("Attaching Policy")
                self.iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                                            PolicyArn=ARN)
                role = self.iam.get_role(RoleName=IAM_ROLE_NAME)
            except ClientError as e:
                print(e)

        self.config.set('DWH', 'DB_ROLE_ARN', role['Role']['Arn'])

        return role['Role']['Arn']

    def authorize_ingress(self, response):
        """
        - get configuration information
        - attempt to grant ingress authorization
        """
        DB_PORT = self.config.get('CLUSTER', 'DB_PORT')
        try:
            vpc = self.ec2.Vpc(id=response['Clusters'][0]['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print("Security Group: " + defaultSg.group_name)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(DB_PORT),
                ToPort=int(DB_PORT)
            )
        except ClientError as e:
            # this error is expected as the security group won't have changed
            # I don't know if it is a good idea to modify ingress rules for the
            # default group
            print(e)

    def start_redshift_cluster(self):
        """
        - get configuration information
        - create a redshift cluster
        - authorize ingress to cluster
        - print out the cluster endpoint and arn
        - return cluster description
        """
        # DWH
        CLUSTER_TYPE = self.config.get('DWH', 'CLUSTER_TYPE')
        NUM_NODES = self.config.get('DWH', 'NUM_NODES')
        NODE_TYPE = self.config.get('DWH', 'NODE_TYPE')
        CLUSTER_IDENTIFIER = self.config.get('DWH', 'CLUSTER_IDENTIFIER')
        DB_ROLE_ARN = self.config.get('DWH', 'DB_ROLE_ARN')

        # CLUSTER
        DB_NAME = self.config.get('CLUSTER', 'DB_NAME')
        DB_USER = self.config.get('CLUSTER', 'DB_USER')
        DB_PASSWORD = self.config.get('CLUSTER', 'DB_PASSWORD')

        print("Creating Redshift Cluster")
        try:
            self.redshift.create_cluster(
                # Cluster Specifications
                ClusterType=CLUSTER_TYPE,
                NodeType=NODE_TYPE,
                NumberOfNodes=int(NUM_NODES),
                # Identifiers & Credentials
                DBName=DB_NAME,
                ClusterIdentifier=CLUSTER_IDENTIFIER,
                MasterUsername=DB_USER,
                MasterUserPassword=DB_PASSWORD,
                # Roles (for s3 access)
                IamRoles=[DB_ROLE_ARN]
            )
        except ClientError as e:
            print(e)

        print("Waiting for Cluster to be Available")
        # code from here until return sourced from:
        # https://hevodata.com/learn/boto3-redshift/
        waiter = self.redshift.get_waiter('cluster_available')
        waiter.wait(
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MaxRecords=20,
            WaiterConfig={
                'Delay': 30,
                'MaxAttemps': 5
            }
        )
        response = self.redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)

        if 'errorType' in response.keys():
            print("Waiter has timed out")
        else:
            print("Cluster is now Available")
            self.authorize_ingress(response)
            print("Cluster Endpoint: \n")
            print(response['Clusters'][0]['Endpoint']['Address'])
            print("Cluster ARN: \n")
            print(response['Clusters'][0]['IamRoles'][0]['IamRoleArn'])

        return response

    def stop_redshift_cluster(self):
        """
        - get cluster identifier from configuration
        - remove cluster based on indentifier
        - remove cluster's iam role
        """
        CLUSTER_IDENTIFIER = self.config.get('DWH', 'CLUSTER_IDENTIFIER')
        # cluster may or may not already exist:
        try:
            self.redshift.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER,
                                         SkipFinalClusterSnapshot=True)
        except ClientError as e:
            print(e)
        print("Cluster Removed")
        self.remove_iam_role()
        pass

    def remove_iam_role(self):
        """
        - get configuration information
        - attemp to detach role policy and role, if they exist
        """
        IAM_ROLE_NAME = self.config.get('DWH', 'IAM_ROLE_NAME')
        ARN = self.config.get('IAM_ROLE', 'ARN')
        # role/policy may or may not already exist:
        try:
            self.iam.detach_role_policy(RoleName=IAM_ROLE_NAME,
                                        PolicyArn=ARN)
        except ClientError as e:
            print(e)
        print("IAM Policy removed from Role")
        try:
            self.iam.delete_role(RoleName=IAM_ROLE_NAME)
        except ClientError as e:
            print(e)
        print("IAM role removed")
        pass


def main():
    print("Test Handler class in dwh.py")


if __name__ == '__main__':
    main()

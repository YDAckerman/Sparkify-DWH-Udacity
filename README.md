

# Project Summary

This is an educational project for learning how to provision and
populate data resources using AWS. The project uses simulated
music-streaming data hosted by Udacity to create a data warehouse for
an imaginary company called Sparkify. The major steps in creating this
project were: 

    - learning how to provision the Redshift resources using
    the boto3 package
    - copying data from the appropriate Udacity sources
    on s3 into staging tables on Redshift
    - finally transforming data
    from the staging tables into a star schema useful for business
    analytics.

# Next Steps

    - Use Terraform to provision AWS resources.
    - Provide Data Quality Checks.
    - Provide Sample Queries.
    - Create a Dashboard (using Tableau, etc.) for Analytics
      on the Sparkify Database.

# Running the Scripts

First, create a config file like that shown in
sample_dwh.cfg, filling in all values other than `CLUSTER:DB_HOST` and
`DWH:ROLE_ARN`. 

Start a redshift cluster by running:

`python dwh.py start_redshift_cluster`

From the commandline. You will see output describing the script's
progress. When the script has finished the cluster endpoint and arn
will be printed. Copy and paste these to dwh.cfg under `CLUSTER:DB_HOST`
and `DWH:ROLE_ARN`, respectively.

Then, from the commandline run:

`python create_tables.py`

followed by:

`python etl.py`

Once these scripts have completed, the fact and dimension tables from
the star schema can be queried.

When the time comes to stop the cluster, run:

`python dwh.py stop_redshift_cluster`

# Repository Contents

- README.md
    - you're looking at it
- create_tables.py
    - run on the commandline to drop and create tables
- etl.py
    - run on the commandline to move data from s3 into staging tables
      on Redshift and then process staging data into the desired star
      schema for analysis.
- sql_queries.py
    - all the queries used in create_tables.py and etl.py
- dwh.py
    - provides commandline interface to interact with an aws handler
      class. The most important arguments are 'start_redshift_cluster'
      and 'stop_redshift_cluster'
- my_aws.py
    - provides Handler class used to create and interact with redshift
      cluster using the boto3 package.
- sample_dwh.cfg
    - a sample configuration file (will need to be renamed and filled
      in to reproduce this project).


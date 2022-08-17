#!/usr/bin/env python
# coding: utf-8

import sys
import configparser
from my_aws import Handler


def main():
    """
    - load config file
    - behavior then depends on arguments given:
       - test_iam_role: test iam role creation/deletion
       - test_redshift_cluster: test create/delete redshift
       - start_redshift_cluster: start a redshift cluster
       - stop_redshift_cluster: stop the redshift cluster
       - view_s3_bucket: print contents of a bucket
       - print_s3_json_object: print an object in an s3 bucket
    """
    config_path = 'dwh.cfg'
    config = configparser.ConfigParser()
    config.read_file(open(config_path))
    handler = Handler(config)

    args = sys.argv[1:]

    if args[0] == 'test_iam_role':
        response = handler.create_iam_role()
        print(response)
        handler.remove_iam_role()

    if args[0] == 'test_redshift_cluster':
        handler.create_iam_role()
        response = handler.start_redshift_cluster()
        print(response)
        handler.stop_redshift_cluster()

    if args[0] == 'start_redshift_cluster':
        handler.create_iam_role()
        handler.start_redshift_cluster()

    if args[0] == 'stop_redshift_cluster':
        handler.stop_redshift_cluster()

    if args[0] == 'view_s3_bucket':
        handler.print_s3_contents(args[1])

    if args[0] == 'print_s3_json_object':
        handler.print_s3_object(args[1], args[2])

if __name__ == '__main__':
    main()

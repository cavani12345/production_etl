import logging
import logging.config
import yaml


from production.xetra.common.s3 import S3BucketConnector
from production.xetra.transformers.xetra_transforms import XetraTransform


def main():
    """
        entry point to run the xetra ETL job.
    """

    # Parsing YML file
    config_path = 'production/config/xetra_report1_config.yml'
    config = yaml.safe_load(open(config_path))
    # read logging configuration
    log_config = config['logging']

    # configure logging module using configuration provided
    # in log_config dictionary.
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    # reading s3 configuration
    s3_config = config['s3']
    # creating the S3BucketConnector classes for source and target
    s3_bucket_src = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])
    s3_bucket_trg = S3BucketConnector(access_key=s3_config['access_key'],
                                      secret_key=s3_config['secret_key'],
                                      endpoint_url=s3_config['trg_endpoint_url'],
                                      bucket=s3_config['trg_bucket'])
    # creating XetraETL class
    logger.info('Xetra ETL job started.')
    xetra_etl = XetraTransform(s3_bucket_src, s3_bucket_trg)

    # running etl job for xetra report 1

    xetra_etl.full_etl()

    logger.info('Xetra ETL job finished.')


if __name__ == "__main__":
    main()

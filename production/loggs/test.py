import logging
import logging.config
import os
import yaml

config_path = 'production/config/xetra_report1_config.yml'
config = yaml.safe_load(open(config_path))

print(config['s3']['access_key'])
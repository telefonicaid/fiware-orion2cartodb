# -*- encoding: utf-8 -*-

import yaml
import logs
import utils

logs.config_log()

# Load properties
logs.logger.info("Loading properties from setup.yaml")
file = open("setup.yaml")
properties = yaml.load(file)
logs.logger.info("Loaded")

x_subject_token = utils.login()
table_exists = utils.checkcreatedtable()

if table_exists == False:
    #attributes = utils.get_all_attributes()
    attributes = utils.get_all_attributes(x_subject_token)
    utils.create_table(attributes)
else:
    logs.logger.error("Table already exist")

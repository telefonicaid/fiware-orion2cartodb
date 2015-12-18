# -*- encoding: utf-8 -*-

import logs
import requests
import yaml
import json
import urllib2

logs.config_log()

# Load properties
logs.logger.info("Loading properties from setup.yaml")
file = open("setup.yaml")
properties = yaml.load(file)
logs.logger.info("Loaded")

EPSG = "4326"
Non_encode_symbols = "&:/=(),'?!."
Mandatory_attributes = ["cartodb_id", "the_geom", "the_geom_webmercator", "created_at", "updated_at"]
new_attributes = []


def login():

    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'fiware-service': properties["fiware_service"]
    }

    payload = {
        "auth": {
            "identity": {
                "methods": [
                    "password"
                ],
                "password": {
                    "user": {
                        "domain": {
                            "name": properties["fiware_service"]
                        },
                        "name": properties["user"],
                        "password": properties["password"]
                    }
                }
            },
            "scope": {
                "domain": {
                    "name": properties["fiware_service"]
                }
            }
        }
    }

    response = requests.request("POST", properties["url_login"], data=json.dumps(payload), headers=headers)
    #print response.headers["x-subject-token"]
    return (response.headers["x-subject-token"])


def get_all_attributes(x_subject_token):
#def get_all_attributes():

    querystring = {"limit": "100"}

    payload = {
        "entities": [
        {
          "type": properties["entity_type"],
          "isPattern": "true",
          "id": ".*"
        } ]
    }
    headers = {
        'accept': "application/json",
        'fiware-service': properties["fiware_service"],
        'fiware-servicepath': "/",
        'content-type': "application/json",
        'x-auth-token': x_subject_token
    }

    response = requests.request("POST", properties["url_cb"], data=json.dumps(payload), headers=headers, params=querystring)
    entities = json.loads(response.text)["contextResponses"]

    for entity in entities:
        entity_attributes = entity["contextElement"]["attributes"]
        values = entity["contextElement"]["attributes"][0]["value"]

        for attribute in entity_attributes:

            if attribute["type"] == "compound":
                if not (attribute["name"] in new_attributes):
                    for subattribute in attribute["value"]:
                        new_attributes.append("%s_%s" % (attribute["name"],subattribute["name"]))

            else:
                if not (attribute["name"] in new_attributes):
                    new_attributes.append(attribute["name"])
    # print new_attributes
    global Mandatory_attributes

    Mandatory_attributes = Mandatory_attributes + new_attributes
    # print Mandatory_attributes
    return Mandatory_attributes


def checkcreatedtable():

        logs.logger.info("Checking if the table exists")
        url = properties["table_checker_url"] % properties["cartodb_apikey"]
        #print url
        response = requests.get(url)
        data=json.loads(response.text)["rows"]
        #print '{cdb_usertables: %s}' % layerproperties["table_name"]
        #TODO: Jose cuando veas esto (que lo verás...) no me mates, mi vida es muy dura y esto no va de otra manera
        exist = False
        tablename = properties["tablename"].lower()
        for element in data:
            print element
            if str(element) == "{u'cdb_usertables': u'%s'}" % tablename :
                exist = True
                logs.logger.info("The table already [ %s ] exists" % properties["tablename"])
        return exist


def create_table(attributes):

    column_names=""
    array_index=1

    for attribute_name in attributes:

            if array_index == len(attributes):
                column_names = ("%s"+"%s") % (column_names, attribute_name + " varchar")
                #print array_index
            else:
                column_names = ("%s"+"%s, ") % (column_names, attribute_name + " varchar")
                #print array_index
            array_index += 1

    logs.logger.info("Creating table [ %s ]" % properties["tablename"])

    # URL to create table
    url = "%s/api/v2/sql?q=CREATE TABLE %s(%s)&api_key=%s" % (properties["cartodb_base_endpoint"],
                                                              properties["tablename"],
                                                              column_names,
                                                              properties["cartodb_apikey"])

    # Send data and create table
    url = urllib2.quote(url, Non_encode_symbols)
    #print url
    logs.logger.info("Sending [ %s ]" % url + "' to CartoDB...")
    f = urllib2.urlopen(url)

    # URL to create table in cartodb's dashboard
    url = "%s/api/v2/sql?q=SELECT cdb_cartodbfytable('%s','%s')&api_key=%s" % (properties["cartodb_base_endpoint"],
                                                                                   properties["user_cartodb"],
                                                                                   properties["tablename"],
                                                                                   properties["cartodb_apikey"])
    logs.logger.info("Showing [ %s ]" % url + "' cartodb's dashboard ...")
    url = urllib2.quote(url, Non_encode_symbols)
    f = urllib2.urlopen(url)

    # Get response
    response = f.read()
    resp = json.loads(response)
    logs.logger.info("Response: '" + json.dumps(resp) + "'")
    logs.logger.info("Sent to CartoDB")

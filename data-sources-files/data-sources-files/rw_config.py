import json
import os
from datetime import datetime
import psutil

temp_sid = ""


def read_config(file):
    """This function read the json configutaion file and returns the json object

    Returns:
        dict -- returns a dictionary containing the configuration data
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(base_dir, file)
    with open(config_file, 'r') as file:
        config_dict = json.load(file)

    return config_dict


def write_config(final_config,sid):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    now = datetime.now()
    filename_time = now.strftime("%d%m")
    config_file = os.path.join(base_dir, sid + filename_time + ".json")
    with open(config_file, 'w') as file:
        formatted_json = json.dumps(final_config, indent=4, sort_keys=True)
        file.write(formatted_json)


def extract_config_details(src_system, file_list, namenode):
    all_tables = []
    url = "hdfs://"
    hadoop_port = "8020"
    emt_list = []
    config = {}
    final_config = {}
    for file in file_list:
        if file == "Pilot.json":
            config_dict = read_config(file)["sources"]
            for configuration in config_dict:
                if configuration["abbreviation"] == src_system:
                    config = configuration
                    sid = config.get("sid").lower()
                    port = config.get("port")
                    service_user = config.get("serviceuser")
                    if config is None:
                        return None
        else:
            config_dict = read_config(file)["tables"]
            source = read_config(file)["source"]
            db = source.get("schema").upper()
            for configuration in config_dict:
                if configuration["consumption"]:
                    config = configuration
                    all_tables.append(config)
                if configuration["tablename"] != "" and configuration["consumption"]:
                    var = configuration["tablename"]
                    emt_list.append(var)

            if all_tables is None:
                return None

    '''final_config["config"] = {}
    final_config["config"]["sources"] = config
    final_config["config"]["sources"]["schema"] = db
    if src_system == 'SYM':
        final_config["config"]["sources"]["dbServerAddress"] = "jdbc:oracle:thin:{0}{1}.atradiusnet.com".format('@',
                                                                                                                sid)
        final_config["config"]["sources"]["dbServerAdd"] = "{0}.atradiusnet.com".format(sid)
        final_config["config"]["sources"]["dbName"] = "{0}.atradiusnet.com".format(sid)
        final_config["config"]["sources"]["dbUser"] = service_user
        final_config["config"]["sources"]["dbPassword"] = ""
        final_config["config"]["sources"]["targetDirectory"] = "/DATA/NATIVE_ZONE/SYMPHONY/{0}/".format(db)
        final_config["config"]["sources"]["avscDir"] = "/DATA/NATIVE_ZONE/SYMPHONY/AVSOSCHEMA/{0}/".format(db)
        final_config["config"]["sources"]["hdfsAddress"] = namenode
        final_config["config"]["sources"]["subDBName"] = db
        final_config["config"]["sources"]["targetDBName"] = "SYMF_{0}".format(db)
        final_config["config"]["sources"]["outdir"] = "ScoopFiles"'''

    final_config["Configs"] = {}
    #    final_config["Configs"] = config
    final_config["Configs"]["schema"] = db
    if src_system == 'SYM':
        final_config["Configs"]["dbServerAddress"] = "jdbc:oracle:thin:{0}{1}.atradiusnet.com".format('@', sid)
        final_config["Configs"]["dbServerAdd"] = "{0}.atradiusnet.com".format(sid)
        final_config["Configs"]["dbName"] = "{0}.atradiusnet.com".format(sid)
        final_config["Configs"]["dbUser"] = service_user
        final_config["Configs"]["dbPassword"] = ""
        final_config["Configs"]["abbreviation"] = src_system
        final_config["Configs"]["serviceuser"] = service_user
        final_config["Configs"]["sid"] = sid
        final_config["Configs"]["dbServerPort"] = port
        final_config["Configs"]["targetDirectory"] = "/DATA/NATIVE_ZONE/SYMPHONY/{0}/".format(db)
        final_config["Configs"]["avscDir"] = "/DATA/NATIVE_ZONE/SYMPHONY/AVSOSCHEMA/{0}/".format(db)
        final_config["Configs"]["hdfsAddress"] = "{0}{1}:{2}".format(url, namenode, hadoop_port)
        final_config["Configs"]["subDBName"] = db
        final_config["Configs"]["targetDBName"] = "{0}_{1}".format(sid, db)
        final_config["Configs"]["outdir"] = "ScoopFiles"
    elif src_system == 'CYC':
        final_config["Configs"]["dbServerAddress"] = "jdbc:oracle:thin:{0}{1}.atradiusnet.com".format('@', sid)
        final_config["Configs"]["dbServerAdd"] = "{0}.atradiusnet.com".format(sid)
        final_config["Configs"]["dbName"] = "{0}.atradiusnet.com".format(sid)
        final_config["Configs"]["dbUser"] = service_user
        final_config["Configs"]["dbPassword"] = ""
        final_config["Configs"]["abbreviation"] = src_system
        final_config["Configs"]["serviceuser"] = service_user
        final_config["Configs"]["sid"] = sid
        final_config["Configs"]["dbServerPort"] = port
        final_config["Configs"]["targetDirectory"] = "/DATA/NATIVE_ZONE/CYC/{0}/".format(db)
        final_config["Configs"]["avscDir"] = "/DATA/NATIVE_ZONE/CYC/AVSOSCHEMA/{0}/".format(db)
        final_config["Configs"]["hdfsAddress"] = "{0}{1}:{2}".format(url, namenode, hadoop_port)
        final_config["Configs"]["subDBName"] = db
        final_config["Configs"]["targetDBName"] = "{0}_{1}".format(sid, db)
        final_config["Configs"]["outdir"] = "ScoopFiles"
    elif src_system == 'SYMR':
        final_config["Configs"]["dbServerAddress"] = "jdbc:oracle:thin:{0}{1}.atradiusnet.com".format('@', "sl42")
        final_config["Configs"]["dbServerAdd"] = "{0}.atradiusnet.com".format("sl42")
        final_config["Configs"]["dbName"] = "{0}.atradiusnet.com".format("sl42")
        final_config["Configs"]["dbUser"] = "SYMRREAD"
        final_config["Configs"]["dbPassword"] = "READ_0NLYQ"
        final_config["Configs"]["abbreviation"] = src_system
        final_config["Configs"]["serviceuser"] = service_user
        final_config["Configs"]["sid"] = sid
        final_config["Configs"]["dbServerPort"] = port
        final_config["Configs"]["targetDirectory"] = "/DATA/NATIVE_ZONE/SYMPHONYR/{0}/".format(db)
        final_config["Configs"]["avscDir"] = "/DATA/NATIVE_ZONE/SYMPHONYR/AVSOSCHEMA/{0}/".format(db)
        final_config["Configs"]["hdfsAddress"] = "{0}{1}:{2}".format(url,namenode,hadoop_port)
        final_config["Configs"]["subDBName"] = db
        final_config["Configs"]["targetDBName"] = "{0}_{1}".format(sid, db)
        final_config["Configs"]["outdir"] = "ScoopFiles"
    elif src_system == 'LCT':
        final_config["Configs"]["dbServerAddress"] = "jdbc:oracle:thin:{0}{1}.atradiusnet.com".format('@', sid)
        final_config["Configs"]["dbServerAdd"] = "{0}.atradiusnet.com".format(sid)
        final_config["Configs"]["dbName"] = "{0}.atradiusnet.com".format(sid)
        final_config["Configs"]["dbUser"] = service_user
        final_config["Configs"]["dbPassword"] = ""
        final_config["Configs"]["abbreviation"] = src_system
        final_config["Configs"]["serviceuser"] = service_user
        final_config["Configs"]["sid"] = sid
        final_config["Configs"]["dbServerPort"] = port
        final_config["Configs"]["targetDirectory"] = "/DATA/NATIVE_ZONE/LCT/{0}/".format(db)
        final_config["Configs"]["avscDir"] = "/DATA/NATIVE_ZONE/LCT/AVSOSCHEMA/{0}/".format(db)
        final_config["Configs"]["hdfsAddress"] = "{0}{1}:{2}".format(url,namenode,hadoop_port)
        final_config["Configs"]["subDBName"] = db
        final_config["Configs"]["targetDBName"] = "{0}_{1}".format(sid, db)
        final_config["Configs"]["outdir"] = "ScoopFiles"
    elif src_system == 'ORF':
        final_config["Configs"]["dbServerAddress"] = "jdbc:oracle:thin:{0}{1}.atradiusnet.com".format('@', sid)
        final_config["Configs"]["dbServerAdd"] = "{0}.atradiusnet.com".format(sid)
        final_config["Configs"]["dbName"] = "{0}.atradiusnet.com".format(sid)
        final_config["Configs"]["dbUser"] = service_user
        final_config["Configs"]["dbPassword"] = ""
        final_config["Configs"]["abbreviation"] = src_system
        final_config["Configs"]["serviceuser"] = service_user
        final_config["Configs"]["sid"] = sid
        final_config["Configs"]["dbServerPort"] = port
        final_config["Configs"]["targetDirectory"] = "/DATA/NATIVE_ZONE/ORF/{0}/".format(db)
        final_config["Configs"]["avscDir"] = "/DATA/NATIVE_ZONE/ORF/AVSOSCHEMA/{0}/".format(db)
        final_config["Configs"]["hdfsAddress"] = "{0}{1}:{2}".format(url,namenode,hadoop_port)
        final_config["Configs"]["subDBName"] = db
        final_config["Configs"]["targetDBName"] = "{0}_{1}".format(sid, db)
        final_config["Configs"]["outdir"] = "ScoopFiles"
    final_config["Configs"]["hiveHost"] = namenode
    final_config["Configs"]["hivePort"] = "10000"
    final_config["Configs"]["hiveUserName"] = "hive"
    final_config["Tables"] = emt_list
    write_config(final_config,sid)

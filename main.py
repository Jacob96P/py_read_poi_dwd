# -*- coding: utf-8 -*-
##################################
# <TITLE>
##################################
# Beschreibe das grundsaetzliche Ziel des Skriptes. Was wird gemacht?
#
##################################
#%%


###########
# IMPORTS #
###########
import pandas as pd
import sys
import configparser
import datetime
from pathlib import Path
import poi_processor_module
import psycopg2.extras
import shapely.geometry as sgeom
import logging

##########
# VARDEF #
##########
# get the current script directory path
# currentScriptDirectoryPath = Path(__file__).parent.resolve()
currentScriptDirectoryPath = '/Users/jacob/Documents/Dev/GIS_Programmierung_SS24/py_read_poi_dwd'

# get log file and config file paths
# logFilePath = Path.joinpath(currentScriptDirectoryPath, 'logs')
logFilePath = '/Users/jacob/Documents/Dev/GIS_Programmierung_SS24/py_read_poi_dwd/logs'
# configFilePath = Path.joinpath(currentScriptDirectoryPath, 'config.ini')
configFilePath = '/Users/jacob/Documents/Dev/GIS_Programmierung_SS24/py_read_poi_dwd/config.ini'

# check for config file
if not Path(configFilePath).is_file():
    print('config.ini file not found')
    sys.exit()

# init configParser and open file
config = configparser.ConfigParser()
# configFilePath = "config.ini"
config_result = config.read(configFilePath, encoding='utf8')

# SET Parameters
stationen_file = config.get('pfade', 'stationen_file')
parameter_file = config.get('pfade', 'parameter_file')
base_url = config.get('pfade', 'base_url')
save_dir = config.get('pfade', 'save_dir')
logfile_dir = config.get('pfade', 'logfile_dir')
db_conn = f"host={config.get('connection', 'DB_HOST')} \
    port={config.get('connection', 'DB_PORT')} \
    dbname={config.get('connection', 'DB_NAME')} \
    user={config.get('connection', 'DB_USER')} \
    password={config.get('connection', 'DB_PASS')}"
table_name = config.get('connection', 'table_name')
loesch_zeitpunkt = float(config.get('parameter', 'loesch_zeitpunkt'))

##########
# ABLAUF #
##########

logger = logging.getLogger(__name__)
logging.basicConfig(filename=f"{logfile_dir}/LOG_{datetime.datetime.now().strftime('%Y_%m_%d__%H_%M')}", encoding='utf-8', level=logging.DEBUG)

startTime = datetime.datetime.now()


##### 
# Es wird über die Stationen iteriert, jede Station wird dabei eine Instanz der Klasse poi_processor_module.PoiProcessor 
# Alle Variablen zur Konfiguration werden bei der Instanzierung übergeben und kommen aus der config.ini-Datei
# Dazu gehören auch die 2 csv-Dateiein aus denen die Stationen und Wetterparameter ausgelesen werden
#####

df_stationen = pd.read_csv(stationen_file)
for index, row in df_stationen.iterrows():
        PoiStation = poi_processor_module.PoiProcessor(
              db_conn = db_conn,
              stationsname = row['Stationsname'], 
              stationsid = row['id'], 
              shape = sgeom.Point(row['lon'], row['lat']),     # Stationslokation in WGS84 (4326)
              parameter = pd.read_csv(parameter_file).to_dict(orient='list'),   # Parameter aus csv werden als dictionary eingelesen
              tableName = table_name,
              baseUrl = base_url,
              saveDirectory = save_dir,
              loeschZeitpunkt = loesch_zeitpunkt,
              logger = logger
              )

        PoiStation.Poi_fc_aussortieren()

        PoiStation.download_file()

        PoiStation.Daten_auslesen()

        PoiStation.Poi_tabelle_updaten()

logger.info(f"#####\nFERTIG: Import- bzw. Updateprozess abgeschlossen!")

##### YOUR CODE HERE #####

# region - task runtime END
scriptDuration = str(datetime.datetime.now() - startTime).split('.', maxsplit=1)[0]
logger.info(f'Script duration {scriptDuration}')
# endregion





# %%

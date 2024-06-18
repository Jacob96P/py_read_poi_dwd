###########
# IMPORTS #
###########
import requests
import pandas as pd
import os
import sys
import datetime
import psycopg2
import psycopg2.extras
import shapely
import logging

class PoiProcessor:
    def __init__ (self, db_conn, stationsname, stationsid, shape, parameter, baseUrl, tableName, saveDirectory, loeschZeitpunkt, logger):
        """
        :param self:instanzvariable
        :param stationsName: Name der Messstation
        :param stationsid: dwd-stationsid der Messstation
        :param shape: Lokation der Messstation
        :param fileName: Dateiname der csv-Datei die für die Station galaden wird
        :param parameter: Dictionary der Parameter die abgespeichert werden sollen
        :param aktuelleDaten: Zwischenspeichervariable für die Daten die aus der csv ausgelesen wurden und im nächsten Schritt in die Datenbank geschrieben werden
        :param baseUrl: Basis-URL von der die poi-Daten vom dwd geladen werden
        :param saveDirectory: Ordner in dem die csv-Dateien abgespeichert werden sollen
        :param loeschZeitpunkt: Zeitpunkt ab dem Daten aus der Datenbank gelöscht werden sollen (soll: alles älter als 7 tage)
        """
        self.db_conn = db_conn
        self.stationsName = stationsname
        self.stationsid = stationsid
        self.shape = shape
        self.fileName = ""
        self.parameter = parameter
        self.aktuelleDaten = {}
        self.baseUrl = baseUrl
        self.tableName = tableName
        self.saveDirectory = saveDirectory
        self.loeschZeitpunkt = loeschZeitpunkt
        self.logger = logger
    
    def download_file(self):
        """
        Lädt die csv-Datei (file_name) der Station vom dwd von der angegebenen base_url runter und legt sie am angegebenen Ort (saveDir) ab.
        """
        self.logger.info(f"Die csv-Datei für Station {self.stationsName} wird vom Deutschen Wetterdienst geladen...")
        file_name = f"{str(self.stationsid)}_-BEOB.csv"
        url = f"{self.baseUrl}/{file_name}"
        save_path = f"{self.saveDirectory}/{file_name}"
        try:
            if(not os.path.exists(self.saveDirectory)):
                try:
                    os.mkdir(self.saveDirectory)
                except Exception:
                    sys.exit(f"directory {self.saveDirectory} not found")
            response = requests.get(url)
            if response.status_code == 200:
                with open(save_path, 'wb') as file:
                    file.write(response.content)
                self.fileName = file_name
                self.logger.info("Datei {file_name} wurde heruntergeladen und unter {save_path} gespeichert.")
            else:   # Weil im Dateinamen manchmal kein "_" ist: 
                file_name = f"{str(self.stationsid)}-BEOB.csv"
                url = f"{self.baseUrl}/{file_name}"
                save_path = f"{self.saveDirectory}/{file_name}"
                response = requests.get(url)
                if response.status_code == 200:
                    with open(save_path, 'wb') as file:
                        file.write(response.content)
                    self.fileName = file_name
                    self.logger.info(f"Datei {file_name} wurde erfolgreich heruntergeladen und unter {save_path} gespeichert.")
                else:
                    self.logger.warning(f"Fehler beim Herunterladen der Datei '{file_name}': {response.status_code} in Poi_Station.download_file() für Station {self.stationsName}")
        except Exception as e:
            self.logger.error(f"Fehler in Poi_Station.download_file() für Station {self.stationsName},", e)
    
    def Daten_auslesen(self):
        '''
        Die Wetterdaten aus den mit der Methode download_file() geladenen csv-Dateien werden ausgelesen und im Dictionary self.aktuelleDaten zwischengepeichert
        '''
        self.logger.info(f"Daten für Station {self.stationsName} werden aus csv-File ausgelesen....")
        try:
            file_path = f'{self.saveDirectory}/{self.fileName}'
            df = pd.read_csv(file_path, sep=';', header=2)
            data_dict = {}
            for index, row in df.iterrows():
                # Datum und Uhrzeit in Datetime-Objekt umwandeln
                datum = row['Datum'].split('.')
                datum = f"20{datum[2]}-{datum[1]}-{datum[0]}"
                datetime_str = f"{datum} {row['Uhrzeit (UTC)']}"
                datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                parameters = {self.parameter['parameter_tabelle'][i]: row[self.parameter['parameter_csv_dwd'][i]] for i in range(len(self.parameter['parameter_csv_dwd'])) if self.parameter['parameter_csv_dwd'][i] in row}
                # Zeitpunkt und Parameter dem Dict hinzufügen
                data_dict[datetime_obj] = parameters
                # Dict umdrehen damit das älteste am Anfang steht
                data_dict = {key: data_dict[key] for key in reversed(data_dict)}
            self.aktuelleDaten = data_dict
            self.logger.info(f"Daten für Station {self.stationsName} ausgelesen....")
        except Exception as e:
            self.logger.error(f"Fehler in Poi_Station.Daten_auslesen() für Station {self.stationsName}", e)

    def parameter_eigenschaft_liste(self, eigenschaft):
        '''
        Gibt eine Eigenschaft der Parameter als sortierte Liste zurück
        input die header der parameter.csv
        Input:
        eigenschaft -> "parameter" (zurück kommt Liste mit Parameterbezeichnung), "einheit" oder "surface_description"
        '''
        for key, value in self.parameter.items():
            if key == eigenschaft:
                return value
    
    def is_float(self, string):
        '''
        Prüft ob der ein String in ein Float umgewandelt werden kann.
        Notwendig um Daten aus csv-Datei richtig abzuspeichern
        Input: String
        '''
        try:
            float(string)
            return True
        except ValueError:
            return False
    
    def is_int(self, string):
        '''
        Prüft ob der ein String in ein Int umgewandelt werden kann.
        Notwendig um Daten aus csv-Datei richtig abzuspeichern
        Input: String
        '''
        try:
            int(string)
            return True
        except ValueError:
            return False

    def Poi_fc_aussortieren(self):
        '''
        Durchsucht die FeatureKlasse nach Datensätzen der Station die älter sind als das angegebene Löschdatum und löscht diese.
        '''
        self.logger.info(f"Datensätze für Station {self.stationsName} die älter als {int(self.loeschZeitpunkt)} Tage sind werden gelöscht...")
        try:
            time = datetime.datetime.now() - datetime.timedelta(days=self.loeschZeitpunkt)
            time = time.strftime('%Y-%m-%d %H:%M')
            with psycopg2.connect(self.db_conn) as con:
                cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute(f'''DELETE FROM dwd_poi WHERE stationsname = '{self.stationsName}' AND zeitpunkt < '{time}';''')
                self.logger.info(f"DATENSÄTZE GELÖSCHT: Es wurden {cur.rowcount} Datensätze der Station {self.stationsName} die älter als {int(self.loeschZeitpunkt)} Tage sind gelöscht!")
        except Exception as e:
            self.logger.error(f"Fehler in Poi_Station.Poi_fc_aussortieren() für Station {self.stationsName}, Exception: ", e)
                
    def Poi_tabelle_updaten(self):
        '''
        Fügt neue Daten in die Feature Klasse ein, wenn für die Station für den jeweiligen Zeitpunkt noch keinen Datensatz existiert.
        Außerdem wird eine Zelle, dessen Wert vorher null war mit einem neuen Wert überschrieben, wenn einer geliefert wird. Passiert in der Praxis aber wahrscheinlich nicht.
        '''
        self.logger.info(f"Neue Daten für Station {self.stationsName} werden eingefügt...")
        fields_list = []
        insertData_list = []
        for key, value in self.aktuelleDaten.items():
            try:
                fields = ['stationsname', 'stationsid', 'shape', 'zeitpunkt']
                insertData = [self.stationsName, self.stationsid, self.shape, key]
                # Testen ob INT oder STRING oder NIX, damit entsprechend eingetragen werden kann
                for key2, value2 in value.items():
                    if self.is_int(value2):
                        fields.append(key2)
                        insertData.append(int(value2))
                    elif self.is_float(value2.replace(',', '.')):
                        fields.append(key2)
                        insertData.append(float(value2.replace(',', '.')))
                    else:
                        fields.append(key2)
                        insertData.append(None) # Wenn keine Zahl drin, dann None (null)
                fields_list.append(fields)
                insertData_list.append(insertData)
            except Exception as e:
                self.logger.error(f"Fehler in Poi_tabelle_updaten für Station {self.stationsName}, Exception: ", e)

        try:
            for i in range(len(fields_list)):
                with psycopg2.connect(self.db_conn) as con:
                    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    cur.execute(f'''SELECT * FROM dwd_poi WHERE zeitpunkt = '{insertData_list[i][3]}' AND stationsname = '{self.stationsName}';''')
                    row = cur.fetchone()
                    if row != None:
                        update = False
                        for m in range(len(fields_list[i])):
                            if row[fields_list[i][m]] == None and insertData_list[i][m] != None:    # Wenn Wert der vorher leer war gefüllt wurde
                                update = True
                                cur.execute("""UPDATE dwd_poi 
                                            SET {fields_list[i][m]} = '{insertData_list[i][m]}'
                                            WHERE zeitpunkt = '{insertData_list[i][3]}' AND {station} = '{self.stationsName}'""")
                                self.logger.info(f"UPDATE: {fields_list[i][m]} wurde bei der Station {self.stationsName} zum Zeitpunkt {insertData_list[i][3]} geupdated!")
                        if not update:
                            # self.logger.info(f"KEINE VERÄNDERUNG: Bei der Station {self.stationsName} zum Zeitpunkt {insertData_list[i][3]} gab es keine Veränderung!")
                            a = 1
                    else:
                        try:
                            spalten = ','.join(fields_list[i])
                            values = ','.join(
                                f"'{str(element)}'" if isinstance(element, (str, datetime.datetime))
                                else 'NULL' if element is None 
                                else f"ST_GeomFromText('{element}', 4326)" if isinstance(element, shapely.geometry.point.Point)
                                else str(element)
                                for element in insertData_list[i]
                            )
                            cur.execute(f'''INSERT INTO dwd_poi ({spalten})
                                        VALUES({values})''')
                            self.logger.info(f'NEUER DATENSATZ: Bei der Station {insertData_list[i][0]} zum Zeitpunkt {insertData_list[i][3]} wurde ein neuer Datensatz eingetragen!')
                        except Exception as e:
                            self.logger.error('Fehler beim Einfügen neuer Daten', e)
        except Exception as e:
            self.logger.error(f"Fehler beim Einfügen in die DB in Poi_Station.Poi_tabelle_updaten() für Station {self.stationsName}\nException: ", e)

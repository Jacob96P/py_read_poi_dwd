# py_read_poi_dwd

Bibliothek zum Auslesen und Abspeichern von retrospektiven Wetterdaten aus csv-Dateien vom Deutschen Wetterdienst.

Diese Klasse Kann eine Tabelle mit Wetterdaten in einer GDB pflegen, sodass automatisch alte Daten gelöscht werden, neue Daten eingefügt werden, und wenn nötig fehlerhafte Daten überschrieben werden.

## Inputs
Notwendig sind:
* parameter.csv zur Definition der Wetterparameter aus den csv-Dateien
* stationen_poi.csv zur Definition der Stationen die genutzt werden sollen
* config.ini zum definieren weiterer Parameter (Pfade, URLs)
* diverse Python-Module, darunter das logger-Modul

## TO-DOs vor dem starten
* Pfade in config-ini anpassen
* postgreSQL-DB Verbindungsparameter in config-ini anpassen

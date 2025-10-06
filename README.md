# Projekt: Data Engineering

## 1. Docker installieren
1) Rufe die offizielle Installationsseite auf: https://docs.docker.com/desktop/setup/install/windows-install/
2) Lade die passende Version für dein Betriebssystem herunter.
3) Nach dem Download dne Installer ausführen.
4) Folge den Installationsschritten (bei Nachfragen einfach "Weiter" klicken und Zugriffe zulassen).
5) Docker Desktop starten

## 2. Repository klonen
1) Explorer öffnen und zum Ordner navigieren, in dem das Projekt gespeichert werden soll.  
2) Im Ordner "Rechtsklick" -> "Im Terminal öffnen"
3) Führe folgenden Befehl aus: "git lfs clone https://github.com/ManuelIU/ProjektDataEngineering.git"
4) Terminal schließen
5) Im Explorer in den Projektordner, der die Datei docker-compose.yml enthält.
6) Erneut "Rechtsklick" -> "Im Terminal öffnen"
7) Starte den Docker-Container mit: "docker-compose up --build -d"

## 3. Airflow aufrufen (wenn der Docker-Build fertig ist, etwas warten)
1) Browser öffnen
2) Rufe folgende Adresse auf: "localhost:8080".
3) Username und Password sind "admin".
4) In der linken Spalte (neben batch_weather_pipeline) gibt es einen Schalter, um den Workflow zu starten.
5) Nach dem aktivieren startet der Workflow automatisch.

## 4. Machine Learning App starten
1) Öffne den Ordner MLapp.
2) Starte das Programm MachineLearningApp.
3) Die App verbindet sich automatisch mit der Datenbank (dies kann einige Sekunden dauern).



Wo liegen die Daten?
    - Die csv-Dateien werden für diesen Workflow aus dem Ordner data/weather_data gelesen.
    - Im Ordner data/backup liegt eine Kopie der csv-Datei, da diese nach erfolgreichem Update lokal gelöscht wird.
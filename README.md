# ProjektDataEngineering

1) Docker installieren
    a) Website https://docs.docker.com/desktop/setup/install/windows-install/ aufrufen
    b) Abhängig vom eigenen Betriebssystem die entsprechende Version downloaden
    c) Nach dem Download Docker installieren
    d) Während des Installionsprozesses immer auf "Weiter" klicken und Zugriffe zulassen
    e) Docker Desktop starten

2) Repository klonen
    a) Explorer öffnen und zum Ordner navigieren in dem das Projekt landen soll
    b) Im Ordner "Rechtsklick" -> "Im Terminal öffnen"
    c) Befehl "git lfs clone https://github.com/ManuelIU/ProjektDataEngineering.git" eingeben und bestätigen
    d) Terminal schließen
    e) Im Explorer in den Projektordner mit der Datei "docker-compose" navigieren
    f) Erneut  "Rechtsklick" -> "Im Terminal öffnen"
    g) Befehl "docker-compose up --build -d" eingeben und bestätigen

3) Airflow aufrufen (wenn der Docker-Build fertig ist, etwas warten)
    a) Browser öffnen
    b) In die Adresszeile "localhost:8080" eingeben
    c) Username und Password sind "admin"
    d) Ganz Links (neben batch_weather_pipeline) kann der Workflow aktiviert werden.
    e) Nach dem aktivieren startet er automatisch.

4) Machine Learning App starten
    a) Im Ordner "MLapp" das Programm "MachineLearningApp" starten
    b) Programm verbindet sich mit Datenbank (Start kann etwas dauern)



Wo liegen die Daten?
    - Die csv-Dateien werden für diesen Workflow aus dem Ordner data/weather_data gelesen.
    - Im Ordner data/backup liegt eine Kopie der csv-Datei, da diese nach erfolgreichem Update lokal gelöscht wird.
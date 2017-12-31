# client-cemaden

**client-cemaden** is a client application for the Cemaden Web Service to retrieve rainfall measurements and water levels from physical sensors, storing them into PostgreSQL database. [Camden]((www.cemaden.gov.br/)) (National Center for Monitoring and Early Warning of Natural Disasters) is a Brazilian agency responsible for monitoring natural hazards. 

## Dependencies

* python 2.7 or greater
* libraries available in the `requirements.txt` file
* PostgreSQL 9.5 or greater
* PostGIS 2.0 or greater

## Configuration

* To create a database with the extension postgis. It is not necessary to create tables, **client-cemaden** will create the tables from the config file parameters (`segup.cfg`).
* To configure the connections in the `setup.cfg` file, as follow:

    * __Bounding box connection__

    ```
    [connection_name]
    connection.url=url_cemaden
    connection.station_type=sensor_type
    csv.folder=csv_folder       (import csv files when available)
    database.host=MyHost
    database.schema=MySchema
    database.name=MyDatabase
    database.table=MyTableName
    database.user=MyUserName
    database.password=MyPassowrd
    ```

**Note:** Cemaden Web Service documentation is available [here](https://trac.dpi.inpe.br/terrama2/raw-attachment/ticket/86/DOC01_webservice_cemaden.pdf).

## Running

    $ ./run.sh

## Contact

If you believe you have found a bug, or would like to ask for a feature or contribute to the project, please inform me at sidgleyandrade[at]utfpr[dot]edu[dot]br.

## License

This software is licensed under the GPLv3.

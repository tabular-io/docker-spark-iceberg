# Iceberg Flink Demo

This demo extends the Iceberg demo setup to include Flink. Included is a standalone Flink application that generates random Lord of the Rings
records and streams them into an Iceberg table using Flink.

Clone this repository, change into the `flink-example` directory, and start up the docker environment.
```sh
git clone git@github.com:tabular-io/docker-spark-iceberg.git
cd flink-example
docker compose up
```

Build the Flink application.
```
./gradlew clean shadowJar
```

Navigate to [http://localhost:8888](http://localhost:8888) and create a new `Python3` notebook. Using the `%%sql` magic, create an `lor` database.
```sql
%%sql

CREATE DATABASE lor
```

Navigate to the Flink UI at [http://localhost:8081/#/submit](http://localhost:8081/#/submit) and upload the shadow jar located at `build/libs/flink-example-0.0.1-all.jar`.

Submit the Flink application and provide the database and output table name as parameters. (The table will be created if it does not exist).
```
--database "lor" --table "character_sightings" 
```

Once the Flink application starts, data will begin streaming into the `lor.character_sightings` table. You can then run a spark query in the notebook to see the results!
```
%%sql

SELECT * FROM lor.character_sightings LIMIT 10
```
*output*:
```

```

Additional optional Flink arguments:
- `--checkpoint` - Set a checkpoint interval in milliseconds (default: 10000)
- `--event_interval` -  Set a time in milliseconds to sleep between each randomly generated record (default: 5000)

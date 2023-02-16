<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

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
```sql
%%sql

SELECT * FROM lor.character_sightings LIMIT 10
```
*output*:
```
+----------------+-------------------------+-------------------+
|character       |location                 |event_time         |
+----------------+-------------------------+-------------------+
|Grìma Wormtongue|Bridge of Khazad-dûm     |1931-08-01 09:02:00|
|Bilbo Baggins   |Ilmen                    |1693-08-01 03:06:28|
|Denethor        |Barad-dûr                |1576-01-04 17:01:59|
|Elrond          |East Road                |1738-09-04 08:07:24|
|Shadowfax       |Helm's Deep              |1977-06-10 00:28:44|
|Denethor        |Houses of Healing        |1998-02-08 12:09:05|
|Quickbeam       |Warning beacons of Gondor|1674-05-25 06:12:54|
|Faramir         |Utumno                   |1801-04-14 00:09:19|
|Legolas         |Warning beacons of Gondor|1923-02-21 10:24:55|
|Sauron          |Eithel Sirion            |1893-05-21 01:29:57|
|Gimli           |Black Gate               |1545-03-06 20:51:13|
+----------------+-------------------------+-------------------+
```

Additional optional Flink arguments:
- `--checkpoint` - Set a checkpoint interval in milliseconds (default: 10000)
- `--event_interval` -  Set a time in milliseconds to sleep between each randomly generated record (default: 5000)

```powershell
.\mongoimport.exe --host 127.0.0.1 --port 27017 --db training --collection grades --file "D:\mongodb\mongodb-sample-dataset-main\sample_training\grades.json"
.\mongoimport.exe --host 127.0.0.1 --port 27017 --db training --collection inspections --file "D:\mongodb\mongodb-sample-dataset-main\sample_training\inspections.json"
.\mongoimport.exe --host 127.0.0.1 --port 27017 --db training --collection posts --file "D:\mongodb\mongodb-sample-dataset-main\sample_training\posts.json"
.\mongoimport.exe --host 127.0.0.1 --port 27017 --db training --collection routes --file "D:\mongodb\mongodb-sample-dataset-main\sample_training\routes.json"
.\mongoimport.exe --host 127.0.0.1 --port 27017 --db training --collection stories --file "D:\mongodb\mongodb-sample-dataset-main\sample_training\stories.json"
.\mongoimport.exe --host 127.0.0.1 --port 27017 --db training --collection trips --file "D:\mongodb\mongodb-sample-dataset-main\sample_training\trips.json"
.\mongoimport.exe --host 127.0.0.1 --port 27017 --db training --collection tweets --file "D:\mongodb\mongodb-sample-dataset-main\sample_training\tweets.json"
.\mongoimport.exe --host 127.0.0.1 --port 27017 --db training --collection zips --file "D:\mongodb\mongodb-sample-dataset-main\sample_training\zips.json"

```


```powershell
.\mongodump.exe --host 127.0.0.1 --port 27017 --db training --out "D:\mongodb\backups\4.4\training_dump"
```

#### Run mongodb with config file

```bash
./mongod.exe --config "D:\mongodb\mongodb-4.4.29\bin\mongo-4.4.cfg"
./mongod.exe --config "D:\mongodb\mongodb-7.0.4\bin\mongo-7.0.cfg"
```

#### Compare the difference of two databases

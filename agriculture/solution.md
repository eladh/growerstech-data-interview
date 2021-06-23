## About the solution 

### DB choice
I decided to use a MySQL DB running in docker because the connection with python works well and is easy to set up. 
Although I think the optimal choice would be a NoSQL db, I choosed to go with a SQL one because of the lack of time and the complexity of the reprocessing. The advantage of the SQL way is constraint on types, so no error in processing will get trought to the DB. 

### ETL 
The basic mechanism of the ETL is based on the pandas capacity to insert DFs into the DB provided the connection. 
This is not optimal in general, the best in this case would be to define Tables using an ORM (SQLAlchemy) to enable migrations and process each row with more control. 
I used the bulk insert to simplify the process. The create_db.sql provides the overview of the schema despite the fact that the script itself is not executed.

In short, I implemented a fast solution, easy to execute but not an optimal one.

### Running the solution 


Go to agriculture folder and run:
```
docker-compose up --build
```

The ETL will execute automatically and the DB is accessible in the running container.
You can also run the docker container with only the db service, install the requirements and run the etl.py file.
It is recommended to use a virtual enviroment in this case.

```
$ docker-compose up --build
$ pip install -r requirements.txt
$ python etl.py
```

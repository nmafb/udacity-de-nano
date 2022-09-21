import psycopg2

!echo "alter user student postgres;" | sudo -u postgres psql


## Create a connection to the database

try:
	conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=postgres")
except psycopg2.Error as e:
	print("Error: Could not make connection to Postgres Database")
	print(e)

## Use the connection to get a cursor that can be used to execute queries.

try:
	cur = conn.cursor()
except psycopg2.Error as e:
	print("Error: Could not get cursor to the Database")
	print(e)

## TO-DO: Set automatic commit to be true so that each action is committed without having to call conn.commit() after each command.

conn.set_session(autocommit=True)

## TO-DO: Create a database to do the work in.

try:
	cur.execute("create database nanode")
except psycopg2.Error as e:
	print(e)


 ## TO-DO: Add the database name in the connect statement. Let's close our connection to the default database, reconnect to the Udacity database, and get a new cursor.

## TO-DO: Add the database name within the connect statement
try: 
    conn.close()
except psycopg2.Error as e:
    print(e)
    
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=nanode user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
    
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

conn.set_session(autocommit=True)

## Create a Song Library that contains a list of songs, including the song name, artist name, year, album it was from, and if it was a single.

song_title
artist_name
year
album_name
single


## TO-DO: Finish writing the CREATE TABLE statement with the correct arguments
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS songs (song_title varchar, artist_name varchar, year integer, album_name varchar, single varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)


## TO-DO: Insert the following two rows in the table
#First Row:  "Across The Universe", "The Beatles", "1970", "Let It Be", "False"
#Second Row: "Think For Yourself", "The Beatles", "1965", "Rubber Soul", "False"


## TO-DO: Finish the INSERT INTO statement with the correct arguments

try: 
    cur.execute("INSERT INTO songs (song_title, artist_name, year, album_name, single) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 ("Across The Universe", "The Beatles", "1970", "Let It Be", "False"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO songs (song_title, artist_name, year, album_name, single) \
                  VALUES (%s, %s, %s, %s, %s)",
                  ("Think For Yourself", "The Beatles", "1965", "Rubber Soul", "False"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)


## TO-DO: Finish the SELECT * Statement 
try: 
    cur.execute("SELECT * FROM songs;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)







    

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()





















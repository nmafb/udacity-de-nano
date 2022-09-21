
-- DeNormalized 
	create table if not exists music_library (album_id int, album_name varchar, artist_name varchar, year int, songs text[])

	insert into music_library (album_id, album_name, artist_name, year, songs) 
	values (1, 'Rubber Soul', 'The Beatles', 1965, ARRAY['Michelle', 'Think For Yourself', 'In My Life']);

	insert into music_library (album_id, album_name, artist_name, year, songs) 
	values (2, 'Let It Be', 'The Beatles', 1970, Array['Let It Be', 'Across The Universe']);

	select * from music_library;


-- 1NF
	create table if not exists music_library2 (album_id int, album_name varchar, artist_name varchar, year int, song_name varchar)


	INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name)	values (1, 'Rubber Soul', 'The Beatles', 1965, 'Michelle');
	INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name)	values (1, 'Rubber Soul', 'The Beatles', 1965, 'Think For Yourself');
	INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name)	values (1, 'Rubber Soul', 'The Beatles', 1965, 'In My Life');
	INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name)	values (2, 'Let It Be', 'The Beatles', 1970, 'Let It Be');
	INSERT INTO music_library2 (album_id, album_name, artist_name, year, song_name)	values (2, 'Let It Be', 'The Beatles', 1970, 'Across The Universe');

	select * from music_library2;


-- 2NF
	create table if not exists album_library (album_id int, album_name varchar, artist_name varchar, year int);
	create table if not exists song_library (song_id int, song_name varchar, album_id int);


	INSERT INTO album_library (album_id, album_name, artist_name, year)
	values (1, 'Rubber Soul', 'The Beatles', 1965);

	INSERT INTO album_library (album_id, album_name, artist_name, year)
	values (2, 'Let It Be', 'The Beatles', 1970);


	INSERT INTO song_library (song_id, album_id, song_name)	values (1, 1, 'Michelle');
	INSERT INTO song_library (song_id, album_id, song_name)	values (2, 1, 'Think For Yourself');
	INSERT INTO song_library (song_id, album_id, song_name)	values (3, 1, 'In My Life');
	INSERT INTO song_library (song_id, album_id, song_name)	values (4, 2, 'Let It Be');
	INSERT INTO song_library (song_id, album_id, song_name)	values (5, 2, 'Across the Universe');

	-- JOIN
	select * 
 	from album_library A
	inner join song_library B on A.album_id = B.album_id;

-- 3NF
	create table if not exists album_library2 (album_id int, album_name varchar, artist_id int, year int);
	-- Already Exists 
		-- 	create table if not exists song_library (song_id int, song_name varchar, album_id int);
	create table if not exists artist_library (artist_id int , artist_name varchar);


	INSERT INTO album_library2 (album_id, album_name, artist_id, year) values (1, 'Rubber Soul', 1, 1965);
	INSERT INTO album_library2 (album_id, album_name, artist_id, year) values (2, 'Let It Be', 1, 1970);

	INSERT INTO artist_library (artist_id, artist_name) values (1, 'The Beatles')

	-- JOIN
	select * 
	from album_library2 A
	inner join song_library B on A.album_id = B.album_id
	inner join artist_library C on A.artist_id = C.artist_id;


















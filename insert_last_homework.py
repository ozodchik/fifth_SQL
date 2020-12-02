import psycopg2
import sqlalchemy

engine = sqlalchemy.create_engine('postgresql://postgres:Ozodchik2712@localhost:5432/fourth_homework')
connection = engine.connect()
print(engine.table_names())

insert_musicians = connection.execute('''INSERT INTO executors(executors_name)
    VALUES ('musician_1'), ('musician_2'), ('musician_3'), ('musician_4'),
    ('musician_5'), ('musician_6'), ('musician_7'), ('musician_8');
''')
print(insert_musicians)


insert_genres = connection.execute('''INSERT INTO genres(Name_of_genre)
    VALUES ('genre_1'), ('genre_2'), ('genre_3'), ('genre_4'), ('genre_5');
''')
print(insert_genres)


insert_albums = connection.execute(f'''INSERT INTO albums(name, description, year_issue)
    VALUES ('Name_of_album_1', 'description_1', '2010-01-02 14:32:12'),
    ('Name_of_album_2',  'description_2', '2011-11-12 16:32:12'),
    ('Name_of_album_3',  'description_3', '2007-01-02 12:34:12'),
    ('Name_of_album_4',  'description_4', '2019-11-12 14:32:12'),
    ('Name_of_album_5',  'description_5', '2016-07-07 19:32:12'),
    ('Name_of_album_6',  'description_6', '2018-01-02 14:32:12'),
    ('Name_of_album_7',  'description_7', '2010-09-02 14:32:12'),
    ('Name_of_album_8',  'description_8', '2020-01-02 14:32:12');
''')
print(insert_albums)


insert_tracks = connection.execute('''INSERT INTO tracks(Name, Duration, albums_id)
    VALUES ('Track_1', 02.02, 17),
    ('Track_2', 03.02, 18), ('Track_3', 01.23, 19), ('Track_4', 01.13, 20),
    ('Track_5', 02.02, 21), ('Track_6', 01.43, 22), ('Track_7', 01.15, 23),
    ('Track_8', 01.02, 24), ('Track_9', 01.54, 17), ('Track_10', 01.46, 18),
    ('Track_11', 03.02, 19), ('Track_12', 01.34, 20), ('Track_13', 01.33, 21),
    ('Track_14', 01.02, 22), ('Track_15', 01.23, 23), ('Track_16', 01.27, 24);
''')


insert_collections = connection.execute('''INSERT INTO
    collection(name, collect_year)
    VALUES ('Name_of_collection_1', '2011-05-16 15:36:38'),
    ('Name_of_collection_2', '2019-03-23 15:36:38'),
    ('Name_of_collection_3', '2020-01-24 15:36:38'),
    ('Name_of_collection_4', '2019-05-25 15:36:38'),
    ('Name_of_collection_5', '2020-11-26 15:36:38'),
    ('Name_of_collection_6', '2019-01-28 15:36:38'),
    ('Name_of_collection_7', '2020-05-21 15:36:38'),
    ('Name_of_collection_8', '2019-11-21 15:36:38');
''')
print(insert_collections)


insert_executors_albums = connection.execute('''INSERT INTO
    executors_albums(executors_id, albums_id)
    VALUES (1, 17),
    (2, 18),
    (3, 19),
    (4, 20),
    (5, 21),
    (6, 22),
    (7, 23),
    (8, 24);
''')
print(insert_executors_albums)



insert_executors_albums = connection.execute('''INSERT INTO
    trscks_collection(tracks_id, collection_id)
    VALUES (33, 8), (34, 7), (35, 7), (36, 5), (37, 1), (38, 2), (39, 3), (40, 4),
    (41, 5), (42, 3), (43, 6), (44, 7), (45, 8), (46, 3), (47, 2), (48, 2);
''')
print(insert_executors_albums)



# Insert. Добавим сверху еще 2 альбома для задания №2 (SELECT)
insert_albums_additionally = connection.execute(f'''INSERT INTO Albums(name, description, year_issue)
    VALUES ('Name_of_album_1', 'my_regular_loren_ipsum_1', '2011-05-16 15:36:38'),
    ('Name_of_album_xx', 'my_regular_loren_ipsum_xx', '2018-03-09 15:36:38'),
    ('Name_of_album_yy', 'my_regular_loren_ipsum_yy', '2018-01-16 15:36:38');
''')
print(insert_albums_additionally)


# Insert. Добавим сверху еще 2 трека для задания №2 (SELECT)
insert_tracks_additionally = connection.execute('''INSERT INTO Tracks(Name, Duration, albums_id)
    VALUES ('Track_n', 05.01, 22), ('Track_m', 04.22, 22);
''')
print(insert_tracks_additionally)


# Insert. Добавим сверху еще 2 трека с 'my' для задания №2 (SELECT)
insert_tracks_additionally_my = connection.execute('''INSERT INTO Tracks(Name, Duration, albums_id)
    VALUES ('mymymymymymymymymy', 06.01, 23), ('Track_my', 04.52, 24);
''')
print(insert_tracks_additionally_my)
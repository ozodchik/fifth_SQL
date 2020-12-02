import psycopg2
import sqlalchemy

engine = sqlalchemy.create_engine('postgresql://postgres:Ozodchik2712@localhost:5432/fifty_homework')
connection = engine.connect()
print(engine.table_names())

# Найдем количество исполнителей в каждом жанре
sel_1 = connection.execute("""
select id_of_genre, count(id_of_executor) from executors_genres
group by id_of_genre
order by id_of_genre;
""").fetchall()
print(sel_1)


# Найдем количество треков, вошедших в альбомы 2019-2020 годов
sel_2 = connection.execute("""
select count(t.id), a.name from tracks t
join albums a on t.albums_id = a.id
group by a.id
having year_issue between '2019-01-01' and '2020-12-31';
""").fetchall()
print(sel_2)


# # Найдем среднюю продолжительность треков по каждому альбому;
sel_3 = connection.execute("""
select a.name, avg(t.duration) from albums a
join tracks t on a.id = t.albums_id
group by a.name
order by a.name
""").fetchall()
print(sel_3)


# Найдем всех исполнителей, которые не выпустили альбомы в 2020 году
sel_4 = connection.execute("""
select executors_name from executors e
join executors_albums e_a on e.id = e_a.executors_id
join albums a on e_a.albums_id = a.id
group by e.executors_name, a.year_issue
having a.year_issue not between '2020-01-01' and '2020-12-31';
""").fetchall()
print(sel_4)


# Найдем названия сборников, в которых присутствует исполнитель 'musician_3'
sel_5 = connection.execute("""
select col.name, e.executors_name from executors e
join executors_albums e_a on e.id = e_a.executors_id
join albums a on e_a.albums_id = a.id
join tracks t on a.id = t.albums_id
join trscks_collection t_c on t.id = t_c.tracks_id
join collection col on t_c.collection_id = col.id
group by col.name, e.executors_name
having e.executors_name like '%%musician_3%%';
""").fetchall()
print(sel_5)


# Найдем название альбомов, в которых присутствуют исполнители более 1 жанра
sel_6 = connection.execute("""
select a.name, e.executors_name, g.name_of_genre from albums a
left join executors_albums e_a on a.id = e_a.albums_id
left join executors e on e.id = e_a.executors_id
left join executors_genres e_g on e_g.id_of_executor = e.id
left join genres as g on g.id = e_g.id_of_genre
group by a.name, e.executors_name, g.name_of_genre
having count(g.id) > 1;
""").fetchall()
print(sel_6)


# Найдем названия альбомов, содержащих наименьшее количество треков
sel_7 = connection.execute("""
select distinct a.name from albums a
join tracks t on t.albums_id = a.id
where t.albums_id  in (
    select albums_id from tracks
    group by albums_id
    having count(tracks.id) = (
        select count(tracks.id) from tracks
        group by albums_id
        limit 1
    )
);
""").fetchall()
print(sel_7)


# Найдем исполнителя(-ей), написавшего самый короткий по продолжительности трек
sel_8 = connection.execute("""
select e.executors_name, min(t.duration) from tracks as t
left join albums a on a.id = t.albums_id
left join executors_albums e_a on e_a.albums_id = a.id
left join executors e on e.id = e_a.executors_id
group by e.executors_name, t.duration
having t.duration = (
    select min(duration) from tracks
    );
""").fetchall()
print(sel_8)


# Найдем наименование треков, которые не входят в сборники
sel_9 = connection.execute("""
select t.name from tracks t
left join trscks_collection t_c on t_c.tracks_id = t.id
where t_c.tracks_id is null;
""").fetchall()
print(sel_9)
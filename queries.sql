create table if not exists Executors (
	ID serial primary key,
	Executors_name varchar(60) not null
);

create table if not exists Albums (
	ID serial primary key,
	Name varchar(60),
	Description text,
	Year_issue date not null
);

create table if not exists Genres (
	ID serial primary key,
	Name_of_genre varchar(60) not null 
);

create table if not exists Collection (
	ID serial primary key, 
	Name varchar(50),
	Collect_year date not null,
	Tracks_ID integer 
);

create table if not exists Executors_Genres (
	ID serial primary key,
	ID_of_genre integer not null references Genres(ID),
	ID_of_executor integer not null references Executors(ID)
);

create table if not exists Executors_Albums (
	ID serial primary key,
	Executors_id integer not null references Executors(ID),
	Albums_ID integer not null references Albums(ID)
);

create table if not exists Tracks (
	ID serial primary key,
	Name varchar(60),
	Duration numeric(2,2),
	Albums_ID integer not null references Albums(ID)
);


create table if not exists Trscks_Collection (
	ID serial primary key,
	Tracks_ID integer not null references Tracks(ID),
	Collection_ID integer not null references Collection(ID)
);

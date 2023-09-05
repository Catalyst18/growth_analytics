-- # Create db
create database miro_db;
-- #createdb

-- # Create user
create user analyticsuser;

-- # Log in to postgres and set password for user

alter user analyticsuser with encrypted password 'miro';
-- # Grant all privileges to user;
grant all privileges on database miro_db to analyticsuser;

grant pg_read_server_files to analyticsuser;

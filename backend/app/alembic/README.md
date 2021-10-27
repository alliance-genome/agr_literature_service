## Alembic Notes

Alembic will check differences in a database schema and the database and produce files to help
update the database to the same schema. Also creates a down grade path too so that it can be reveres if necessary.
$ at the start indicates a command line rather than a comment.

## Done
# initialise the version (has been done, created the directory here), that the README is in)
$ cd backend/app
$ alembic init alembic
 - creates alembic directory and alembic.ini
 - edit alembic.ini and set the line sqlalchemy.url
   - i.e. sqlalchemy.url = postgresql://postgres:postgres@localhost/literature

# create a new revision
$ alembic revision -m "initialisation"
 - creates a alembic/versions/XXX_initialisation.py file

# Update database to this initial version (From none)
$ alembic upgrade head
- Will create the alembic_version table and initialise it with data.
- i.e. literature=# select * from alembic_version;
 version_num  
--------------
 62c09ba92b8f
(1 row)
- also process the upgrade() routine but this is blank so does nothing else to the databases


## Updating the schema and database
- edit the models
- alembic get changes.
  - $ alembic revision --autogenerate -m "Add some comment about the changes"
    - produces a file in versions/XXXXX_add_some_comment_about_the_changes.py
    - review the output.
    - ***edit/review the file***
- update the database
  - $ alembic upgrade head

***NOTE: for None persistent databases you will have to add the table alembic_version and add what ever the last release was in verson_num column.***

## General.
So at present we do not have a persistent database but we want to keep the alembic upgrades etc so we have a record
of what is done and can reverse if necessary. Also if we have test/production then the alembic migrations can be used on both.
The migrations are more valuable with persistent databases and enables us to test new schema and be sure we update
other database copies the same way.
***NOTE: Have not been able to get user plugin working as we get a cyclic import. So remove all user iterms in the upgrade method.***

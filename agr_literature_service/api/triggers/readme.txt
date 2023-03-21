To list functions 
   \df

To view the function 
   \df+ funcname

To view/edit in an easier way 
   \ef funcname

For using triggers to update a row in a table you must.

Add triggers to alembic by registering them in the alambic env.py file:-
i.e.
register_entities([trgfunc_author_update_citation, 
                   trg_author_update_citation,
                   trgfunc_reference_update_citation, 
                   trg_reference_update_citation])

Also in the alembic.ini file we add the following:- 
[logger_alembic_utils]
level = INFO
handlers =
qualname = alembic_utils

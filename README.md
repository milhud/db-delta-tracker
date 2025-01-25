### Database Delta Tracker and Restoration

This Go script comes in two parts: main.go and init.go.

## To Run

Edit init.go and main.go and provide the target database name, username, and password.

Then, run 

```
go run init.go
```

from the directory the go script is in.

This will backup the current database to json files, as well as create a "deltas" table to track changes and a [database]_restored database that will be used to restore the database later.

## To restore:

Run main.go using the following command:

```
    go run main.go
```

This will rebuild the database from the deltas table. Feel free to edit the deltas table to remove any commands that delete important data.


(The init.go script creates SQL triggers to track changes. Remove those triggers by running the below script.)

```
DO $$ 
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT event_object_table AS table_name,
               trigger_name
        FROM information_schema.triggers
    LOOP
        EXECUTE FORMAT('DROP TRIGGER IF EXISTS %I ON %I;', rec.trigger_name, rec.table_name);
    END LOOP;
END $$;

```

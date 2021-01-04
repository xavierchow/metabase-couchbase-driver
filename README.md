## Couchbase Metabase Driver

[Couchbase](https://www.couchbase.com/) database driver for the [Metabase](https://github.com/metabase/metabase).
This is still an experimental project, it provides the SQL(N1QL) support and limited features basing on MBQL.

## Usage

As a noSQL database, the coucbhase doesn't have the concept of table, this driver maps the coubhase bucket to the DB in the Metabase,
and it requires you to have a type field `_type` to define the documents in a table.

## Configuration

Go to the Metabase admin page and add a new database,


<img src="https://raw.githubusercontent.com/xavierchow/asset/master/metabase-couchbase-driver/metabase-admin.png" height="450">

The `Database name` needs to be the bucket name, `Host`, `Username` and `Password` are self-explanatory.
The `Table defintion` tells the Metabase the schema of the document in the bucket.

* `name`: the table name in Metabase
* `schema`: it needs to be the value of `doc._type`, it's used to identify which documents are in the table.
* `fields`
  * ``name``: column name
  * `type`: the JSON type of the field, optional.
  * `pk`?: a boolean whether it's primary key
  * `database-position`: column position
  * `base-type`:  the Metabase type defined in [types.clj](https://github.com/metabase/metabase/blob/master/src/metabase/types.clj) without `:type/` prefix, optional.

* Example
```json
{"tables":[{"name": "order", "schema": "Order", 
  "fields": [{ "name": "id", "type": "string","database-position": 0, "pk?": true},
             { "name": "state", "type": "string","database-position": 1 },
             { "name": "userPhoneNumber", "type": "string","database-position": 2 },
             { "name": "SKU code", "type": "string","database-position": 3 },
             { "name": "productName", "type": "string","database-position": 4 },
             { "name": "amount", "type": "number","database-position": 5 },
             { "name": "createdAt", "base-type": "Text","database-position": 6 }]}]}
```


## Building the driver

### Local build
#### Prereq: Install Metabase as a local maven dependency, compiled for building drivers

Clone the [Metabase repo](https://github.com/metabase/metabase) first if you haven't already done so.

```bash
cd /path/to/metabase_source
lein install-for-building-drivers
```

#### Build the Couchbase driver

```bash
# (In the Couchbase driver directory)
lein clean
DEBUG=1 LEIN_SNAPSHOTS_IN_RELEASE=true lein uberjar
```

#### Copy it to your plugins dir and restart Metabase

```bash
mkdir -p /path/to/metabase/plugins/
cp target/uberjar/couchbase.metabase-driver.jar /path/to/metabase/plugins/
jar -jar /path/to/metabase/metabase.jar
```

### Build with docker

```
docker build -t xavchow/metabase-with-cb .
```

```
docker run --rm -p 3000:3000 --name metabase xavchow/metabase-with-cb
```
## Licene

Licensed under [MIT](https://github.com/xavierchow/metabase-couchbase-driver/blob/master/LICENSE)

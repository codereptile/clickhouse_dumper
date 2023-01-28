# clickhouse_dumper

In order to delete old dates from clickhouse, use:
```SQL
ALTER TABLE dAtAbAsE.tAbLe DELETE WHERE date < 'yyyy-mm-dd'
```
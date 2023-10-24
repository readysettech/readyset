## Utilities for parsing VStream events

Specific features in no particular order:
* Extracting table schema from FIELD events
* Maintaining an in-memory schema cache of all previously processed FIELD events
* Parsing data from ROW events and converting it to Noria-formatted rows (vectors of `DfValue`)
* Classifying ROW events into different DML operations (insert, update, delete).

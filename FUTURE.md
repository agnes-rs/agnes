# Future Work

`agnes` is currently at a very early stage, and many improvements and enhancements are planned / under development.

Some currently planned features / changes (not a complete list) are:
* Splitting CSV loading code into separate crate and improve documentation so it can be used as an example of a source adapter (for future source type development).
* Additional source types:
  * Structed text data (XML, JSON, etc.)
  * Serialization formats (Protobuf, BSON, HDF5)
  * Databases
* Additional documentation and examples.
* Interface with matrix / machine learning libraries ([matrix](https://github.com/jblondin/matrix), [tensorflow](https://github.com/tensorflow/rust), etc.)
* Data visualization through [rhubarb](https://github.com/jblondin/rhubarb).
* `DataView` reshaping and aggregation (see R's `reshape` package / SQL GROUP BY)
* Hash joins (currently only uses sort-merge joins)
* Outer joins
* Replacement of partial functions with specializations once specializations stabilize.
* Expanded test suite.
* Integration of [Apache Arrow](https://github.com/apache/arrow) data structures.

Example source code and notes for my talk on using data in off-heap data with idomatic Clojure.



## Source code notes

The code includes examples and benchmarks of loading Divvy bikeshare ride data (https://www.divvybikes.com/data)

## Prerequisites

This set of examples demonstrates working with large collections of data in memory. As configured in project.clj, it expects to use 8gb of heap space. If you do not have 8gb of physical memory available on your computer, you will want to modify the project.clj. Note that performance of the examples and benchmarks will be affected. 

## To get started

This repo includes the raw Divvy ride data as distributed by Divvy. It works with the data in 2 formats: the raw data (parsed into Clojure maps), and a seriealized binary format (loaded directly into memory). This repo does not include the serialized data. To generate it, run:

> lein make-record-cache

## License

Copyright © 2016 David Altenburg

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

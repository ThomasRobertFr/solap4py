solap4py
========

A simple Python module for SOLAP applications

## What is solap4py ?

solap4py is a binding python module you can use to submit requests to [GeoMondrian](http://www.spatialytics.org/fr/projets/geomondrian/), a Spatial [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) (SOLAP) server with Python.

## Where is it used ?

solap4py is used to retrieve spatial data from GeoMondrian for a custom [GeoNode](http://geonode.org/) to display geographic business intelligence data. Check it out at [LogAnalysis/GeoNode](https://github.com/loganalysis/geonode/) !

## How to use it ?

solap4py is a Python module you have to integrate into our custom GeoNode. There's no standalone version yet.

## Dependencies

You first have to launch a [solap4py-java](https://github.com/loganalysis/solap4py-java) server. Then solap4py will just act as a binding layer to use the java server with Python.

Of course you also need a GeoMondrian server to get your data from.

Finally, you also need a custom GeoNode and integrate solap4py into it.

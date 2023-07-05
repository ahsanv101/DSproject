# DSproject

The goal of the project is to develop a software that enables one to process data stored in different formats and to upload them into two distinct databases to query these databases simultaneously according to predefined operations. This document describes the data to process (their main characteristics and possible issues) and how the software has been organised (name of the files, where have been defined the various Python classes, etc.).

## Data
The exemplar data for testing the project are:
-for creating the relational database, there are two files, a CSV file containing data about publications and a JSON file containing additional information including the authors of each publication, the identifiers of the venue of each publication, and the identifier and name of each publisher publishing the venues;

-for creating the RDF triplestore, there are two files, a CSV file containing data about publications and a JSON file containing additional information including the authors of each publication, the identifiers of the venue of each publication, and the identifier and name of each publisher publishing the venues.


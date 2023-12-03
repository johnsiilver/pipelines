# Distributed Stagedpipe

## Introduction

This package provides a distributed stagedpipe that works similar to the stand alone version.

Each stagedpipe to be run is distributed as a plugin to a set of nodes. The plugin framework is located in the `plugin/` directory.

The major difference in operation of the standalone and the distributed version from a client's perspective is the including of a pipeline configuration file. This allows setup of a pipeline for a particular use that would normally be done inside `main()`. An example is passing the credentials for a database to use so that the plugin can construct a connection to a database.

There is one other minor difference, the pipeline has a maximum of 100MiB per data message sent.

The distributed pipeline consists of 3 types of Nodes:

* The Overseer node
* The Coordinator nodes
* The Worker nodes

The Overseer is responsible for knowing all Coordinator and Work nodes in the cluster. It also handles pushing of all plugins and plugin updates to the Worker nodes. Finally it is the Overseer that assigns a Coordinator for a client to use in streaming data. Currently there is only one Overseer node with no primary/secondary backup.

The Coordinator node is responsible for taking connections from clients and funneling the traffic to various Worker nodes.

The Worker nodes run all the plugins and do all the processing. Since Worker nodes multiplex all the pipelines, they monitor their memory footprint and pause when memory is low to prevent out of memory problems.  

## Notes

The `distrib/` package has no compatibility promises at this time. While this works, it hasn't had major real life testing yet. 

Also, right now things like node discovery is very static and has an overseer that can never go down. That will need to be corrected.

Communication between nodes is currently `gRPC`, but in the long run I'm going to change that to use my `Claw` format and transport.  I'll probably leave it `gRPC` from the client to the Overseer and client to Coordinator, as grpc has a ton of language support and I want to allow other languages to connect to the system.


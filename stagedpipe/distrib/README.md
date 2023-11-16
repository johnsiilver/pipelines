# Distributed Stagepipe

## Introduction

TBW

## Notes

The `distrib/` package has no compatibility promises at this time. While this works, it hasn't had major real life testing yet. 

Also, right now things like node discovery is very static and has an overseer that can never go down. That will need to be corrected.

Communication between nodes is currently grpc, but in the long run I'm going to change that to use my claw format and transport.  I'll probably leave it grpc from the client to the overseer and client to coordinator, as grpc has a ton of language support.


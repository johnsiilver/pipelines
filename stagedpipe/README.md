# Introduction

Golang standard Pipelines work using the concurrency model layed out in Rob Pike's talk on "Concurrency in not Parallelism".  

In this version, each pipeline has stages, each stage runs in parallel, and channels pass data from one stage to the next. In a single pipeline with X stages, you can have X stages running. This is called a concurrent pipeline for the purposes of this doc.

You can run multiple pipelines in parallel. So if you run Y pipelines of X stages, you can have X * Y stages running at the same time.
```
        -> stage0 -> stage1 -> stage2 -> stage3 -> 
        -> stage0 -> stage1 -> stage2 -> stage3 -> 
in ->->                                            ->-> out
        -> stage0 -> stage1 -> stage2 -> stage3 -> 
        -> stage0 -> stage1 -> stage2 -> stage3 -> 
```

## The First Problem

In simple pipelines with few stages, this is pretty easy. In complex pipelines, you end up with a bunch of channels and go routines. When you add to the pipeline after you haven't looked at the code in a few months, you forget to call `make(chan X)` in your constructor, which causes a deadlock. You fix that, but you forgot to call `go p.stage()`, which caused another deadlock. You have lots of ugly channels and the code is brittle.

You try to deal with that by making each stage its own file with its own `struct` and piecing them all together with a `Register(stage)` that takes care of the setup for you.

That works, but it lacks the beauty of just scrolling from stage to state in a single file.

## The Second Problem

The standard type of pipelining also works great in two scenarios:
* Everything that goes through the Pipeline is related
* Each request in the Pipeline is a promise that responds to a single request

In the first scenario, no matter how many things enter the pipeline, you know know they are all related to a single call. When input ends, you shut down the input channel and the output channel shuts down when a `sync.WaitGroup` says all pipelines are done.

In the second scenario, you can keep open the pipeline for the life of the program as requests come in. Each request is a promise, which once it comes out the exit channel is sent on the included promise channel. This is costly because you have to create a channel for every request, but it also keeps open the pipelines for future use.

But what if you want to keep your pipelines running and have multiple ingress streams that each need to be routed to their individual output streams?  The pipeline models above break down, either requiring each stream to have its own pipelines (which wastes resources), bulk requests that eat a lot of memory, or other unsavory methods.

## This Solution

I hate to say "the solution", because there are many ways you can solve this. But I was looking to create a framework that was elegant in how it handled this.

What I've done here is combine a state machine with a pipeline. For each stage in the state machine, we spin off 1 goroutine that is running the state machine. We receive input on a single channel and send output on a single channel.  The input gets sent to any state machine that is available to process and each state machine sends to the `out` channel.

```
Four Pipelines processing
        -> Pipeline ->
        -> Pipeline -> 
in ->->                ->-> out
        -> Pipeline -> 
        -> Pipeline ->

Each Pipeline looks like:
        -> stages ->
        -> stages ->
in ->->               ->-> out
        -> stages ->
        -> stages ->
```

You can than concurrently run multiple pipelines. This differs from the standard model in that a full pipeline might not have all stages running, but it will have the same number of stages running. Mathmatically, we still end up in a X * Y number of concurrent actions.

`Stage`s are constructed inside a type that implements our `StateMachine` interface. Any method on that object that is `Public` and implements `Stage` becomes a valid stage to be run. You pass the `StateMachine` to our `New()` constructor with the number of parallel pipelines (all running concurrently) that you wish to run. A good starting number is `runtime.NumCPU()`.

Accessing the pipeline for happens by creating a `RequestGroup`. You can simply stream values in and out of the pipeline separate from other `RequestGroup`s using the the `Submit()` method and `Out` channel.  
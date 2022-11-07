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

## The Problem

In simple pipelines with few stages, this is pretty easy. In complex pipelines, you end up with a bunch of channels and go routines. When you add to the pipeline after you haven't looked at the code in a few months, you forget to call `make(chan X)` in your constructor, which causes a deadlock. You fix that, but you forgot to call `go p.stage()`, which caused another deadlock. You have lots of ugly channels and the code is brittle.

You try to deal with that by making each stage its own file with its own `struct` and piecing them all together with a `Register(stage)` that takes care of the setup for you.

That works, but it lacks the beauty of just scrolling from stage to state in a single file.

## The Other Problem

Pipelines are great when you want to stream either single entries or bulk entries that will be processed at each stage immediately.

But what if it makes sense to stream individual entries, but at certain times bulk up data in the entries to send to a service that wants bulk requests?  I mean bulk network requests are faster than individual requests for the most part by several multiples.

Well in the standard processing model, this breaks down because you can't stop the pipeline or you have to make huge channels at different points that keep things from blocking.

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

Adding a stage simply requires adding a method that is `Public` and implements our `StateFn`.

In addition we add an output queue that sits between final stage and the `out` channel. This channel can have a size limit or not, depending on your needs. As items in the queue signal they have finished work, they are pushed out into the `out` channel. 

This prevents having to size channels at each stage, though it does have some head of line blocking issues. I have not found this to be a problem.
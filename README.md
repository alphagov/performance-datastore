[![Build Status](https://travis-ci.org/alphagov/performance-datastore.svg?branch=master)](https://travis-ci.org/alphagov/performance-datastore)

# Overview

Basic sketches around doing an API in Go for the Performance Platform
datastore.

This is a work in progress to think in code and explore what the new,
supported API for the Performance Platform should look like. It is
not production code that we currently rely on.

# Usage

`make` will build the project.

There are various environment variables used to control what the
application will try to use for things like Mongo connection,
backend APIs etc.

# TODO

- [ ] Look at using http://godoc.org/code.google.com/p/go.net/context
- [ ] Implement Write API as per https://github.com/alphagov/backdrop
- [ ] Implement Read API as per new ideas
- [ ] Add Write API support for fanning out data (RabbitMQ, notifications etc)
- [ ] Add support for PostgreSQL

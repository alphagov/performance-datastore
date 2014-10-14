[![Build Status](https://travis-ci.org/jabley/performance-datastore.svg?branch=master)](https://travis-ci.org/jabley/performance-datastore)

# Overview

Basic sketches around doing an API in Go for the Performance Platform
datastore.

Used for me exploring Go as an implementation language for servers
and sketching out what I'd like the API to look like.

# Usage

`make release` will build the project with concrete versions of the
dependencies.

`make` will build the project using the latest versions of all the
dependencies.

There are various environment variables used to control what the
application will try to use for things like Mongo connection,
backend APIs etc.

# TODO

- [ ] Look at using http://godoc.org/code.google.com/p/go.net/context
- [ ] Implement Write API as per https://github.com/alphagov/backdrop
- [ ] Implement Read API as per new ideas
- [ ] Add Write API support for fanning out data (RabbitMQ, notifications etc)
- [ ] Add support for PostgreSQL

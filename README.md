## Lab instructions

https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

## Docker

### Create docker image:

docker build -t 5840-docker .

### Run docker image:

docker run -it -v ~/Desktop/6.5840/6.5840:/mit-6.824-labs 5840-docker

## Paper Questions

### Lecture 2

https://go.dev/tour/concurrency/10

### Lecture 4

With a linearizable key/value storage system, could two clients who issue get() requests for the same key at the same time receive different values? Explain why not, or how it could occur.

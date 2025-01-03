###Create docker image:

docker build -t 5840-docker .

###Run docker image:

docker run -it -v ~/Desktop/6.5840/6.5840:/mit-6.824-labs 5840-docker
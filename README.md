# Narwhal

Narwhal is a YARN application for Docker container orchestration for YARN.

## Features

## Roadmap

- [x] Simple short-lived Docker container scheduling
- [x] Retreive meta data of Docker container
- [ ] Secured Docker container
- [ ] Volume mount and port mapping of Docker container
- [ ] Task re-run when failure
- [ ] Application master fault tolerance
- [ ] Client and AM RPC
- [ ] Dynamically scale tasks
- [x] Remove appname submission to AM
- [ ] Use MD5 hash to check if image has already been loaded
- [ ] Support "YARN" containers

## Setting up and running Narwhal
1. A LinuxContainerExecutor enabled hadoop cluster
2. Enable YARN Registry for the cluster
3. Prepare your Docker image. It should have the same user (and uid) in it which is also used when run Narwhal
4. Clone, build and run Narwhal with the Docker image
```sh
git clone https://github.com/intel-hadoop/Narwhal.git
cd Narwhal
mvn clean package -DskipTests
```
```sh
docker pull centos:latest
cat <<'EOF' > artifact.json
{
    'name': 'hello-docker',
    'cpus': 2.0,
    'mem': 32.0,
    'instances': 2,
    'cmd': 'echo \"hello docker\"',
    'engine': {
        'type': 'DOCKER',
        'image': 'centos',
        'localImage': true
    }
}
EOF
```
```sh
yarn jar target/narwhal-1.0-SNAPSHOT.jar run -configFile artifact.json -jar target/narwhal-1.0-SNAPSHOT.jar
```
## Narwhal client
To list Docker container status of an applicatoin
```sh
yarn jar target/narwhal-1.0-SNAPSHOT.jar resolve -applicationId <applicationId>
```
To list all applications history
```sh
yarn jar target/narwhal-1.0-SNAPSHOT.jar registry
```

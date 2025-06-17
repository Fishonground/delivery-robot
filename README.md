# Secure update

## Disclaimer 

This is a demo project and shall not be used in production.
The code is distributed under MIT license (see the LICENSE file).

## Purpose

This is an example of simple delivery-robot hardening by using "Security monitor" pattern: all cross-services requests go through the Monitor service.
The Monitor checks whether particular request is authorized and valid, then delivers it to destination service or drops it without further processing.

## Running the demo

There is the main options for running the demo:
- containerized (using docker containers)

There shall be _docker-compose_ and _make_  packages locally available

### Running complete demo in containerized mode

execute in VS Code terminal window either
- if running first time execute _make prepare_
- _make run_
- or _docker-compose up_

then open the requests.rest file in Visual Studio Code editor. If you have the REST client extension installed, you will see 'Send request' active text above GET or POST requests, you can click on it.

If you click on the application send POST link with comment "Create new order", you will start working process and shall see response in the Response window (will open automatically).

After you will see something like "[central] event {id}, robot is in destination point, ready for PIN!" in logs, you can sent request with PIN. You can make it and earlier, but checking program should be blocked by security aims.

### Tests
- Tests execution integrated in _make all_ scenario, if you want to run it manually use _make test_ when system is running

#### Troubleshooting

- if kafka or zookeeper containers don't start, make sure you don't have containers with the same name. If you do, remove the old containers and run the demo again.
- if you use a package manager other than apt or you have troubles with make prepare, you need install packages in _sys packages_ scenario manually then clear _sys packages_ scenario in makefile
- if after _make all_ you see output like this: [Building wheel for confluent-kafka (pyproject.toml): started
  Building wheel for confluent-kafka (pyproject.toml): finished with status 'error'
Failed to build confluent-kafka] try to install librdkafka package. Then try _make all_ again
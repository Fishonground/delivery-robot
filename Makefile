PATH_PREFIX := $(CURDIR)

MODULES := monitor \
           communication \
		   crypto \
		   central \
		   complex \
		   gnss \
		   internal \
		   hmi \
		   motion \
		   positioning \
		   sensors \

SLEEP_TIME := 20

run:
	docker-compose up -d
	sleep ${SLEEP_TIME}

	for MODULE in ${MODULES}; do \
		echo Creating $${MODULE} topic; \
		docker exec broker \
			kafka-topics --create --if-not-exists \
			--topic $${MODULE} \
			--bootstrap-server localhost:9092 \
			--replication-factor 1 \
			--partitions 1; \
	done

all: clean build run-broker delay30s run delay60s test

delay30s:
	sleep 30

delay60s:
	sleep 60

sys-packages:
	sudo apt install -y docker-compose
	sudo apt install python3-pip -y
	sudo pip install pipenv

broker:
	docker-compose -f kafka/docker-compose.yaml up -d

permissions:
	chmod u+x $(PATH_PREFIX)/monitor/monitor.py
	chmod u+x $(PATH_PREFIX)/central/central.py
	chmod u+x $(PATH_PREFIX)/communication/communication.py
	chmod u+x $(PATH_PREFIX)/fleet/server.py
	chmod u+x $(PATH_PREFIX)/hmi/hmi.py
	chmod u+x $(PATH_PREFIX)/motion/motion.py
	chmod u+x $(PATH_PREFIX)/positioning/positioning.py
	chmod u+x $(PATH_PREFIX)/sensors/sensors.py
	chmod u+x $(PATH_PREFIX)/crypto/crypto.py
	chmod u+x $(PATH_PREFIX)/complex/complex.py
	chmod u+x $(PATH_PREFIX)/internal/internal.py
	chmod u+x $(PATH_PREFIX)/gnss/gnss.py

pipenv:
	pipenv install -r requirements.txt

prepare: sys-packages permissions pipenv build run-broker

logs:
	docker-compose logs -f --tail 100

build:
	docker-compose build

run-broker:
	docker-compose up -d zookeeper broker

clean:
	docker-compose down; pipenv --rm; rm -f Pipfile*; echo cleanup complete

check_broker:
	# message broker needs to be reachable by name su-broker - it is expected so in tests
	ping -c 1 su-broker; if [ $$? -eq 0 ]; then echo "broker check ok" ; else \
	echo "adding su-broker to your /etc/hosts"; sudo bash -c "echo '127.0.0.1       su-broker' >> /etc/hosts" ; \
	fi

test: check_broker
	pipenv run pytest -sv

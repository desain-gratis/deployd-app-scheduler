ifneq (,$(wildcard .env))
include .env
export
endif

clean-wsl: 
	docker compose down -v # important for wsl
	sudo rm -rf ./tmp*           

clean:
	rm -rf ./tmp*

test:
	go test -v ./src/...
	
build: test
	CGO_ENABLED=0 GOOS=linux go build -o dist/scheduler cmd/scheduler/*.go

build-docker: build
	docker build --pull=false --network=host -t scheduler .

run: build-docker clean clean-wsl
	docker compose up --force-recreate

sync:
	go run ./cmd/sync/*.go

get:
	curl -H 'X-Namespace: *' "$(API_ENDPOINT)/user-profile"

load-test:
	k6 run -e RPS=100 --out csv=test/raw_results.csv test/k6-user-profile-test.js

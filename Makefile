include env.mk

all:
	@echo "make build|build-collect|build-sign"
	@echo "make docker|docker-collect|docker-sign"
	@echo "make push"
build: build-collect build-sign
build-collect:
	cd cmd/collect/ && go install

build-sign:
	make -C usdt_sign

docker: docker-dir docker-collect docker-sign
docker-dir:
	mkdir -p build

docker-collect:
	rm -rf build/collect
	cp -r images/collect build/
	cp $(GOBIN)/collect build/collect/
	$(DOCKER_BUILD) build/collect/ -t $(DOCKER_NS)/$(PROJECT_NAME)-hsn-collect:$(HSN_MYBOX_VERSION)

docker-sign:
	rm -rf build/sign
	cp -r images/sign build/
	cp -r usdt_sign/node_modules usdt_sign/index.js usdt_sign/package.json build/sign/
	$(DOCKER_BUILD) build/sign/ -t $(DOCKER_NS)/$(PROJECT_NAME)-hsn-sign:$(HSN_SIGN_VERSION)

push:
	sudo docker push $(DOCKER_NS)/$(PROJECT_NAME)-hsn-collect:$(HSN_MYBOX_VERSION)
	sudo docker push $(DOCKER_NS)/$(PROJECT_NAME)-hsn-sign:$(HSN_SIGN_VERSION)

check:
	goimports -w .
	golangci-lint run --no-config --issues-exit-code=0 --deadline=30m \
      --disable-all --enable=deadcode  --enable=gocyclo --enable=golint --enable=varcheck \
      --enable=structcheck --enable=errcheck --enable=ineffassign \
      --enable=unconvert --enable=goconst --enable=gosec --enable=megacheck #--enable=maligned --enable=dupl --enable=interfacer

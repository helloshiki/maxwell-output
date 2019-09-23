XDIR=$(shell cd ../../../..;pwd)
GOPATH=$(XDIR)
GOBIN=$(GOPATH)/bin
GO111MODULE=on


DOCKER_NS=registry.cn-hangzhou.aliyuncs.com/tenbayblockchain
PROJECT_NAME=hsn
DOCKER_BUILD=sudo docker build
HSN_MYBOX_VERSION=1.1
HSN_SIGN_VERSION=1.1
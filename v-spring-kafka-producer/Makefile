KUBECTL=minikube kubectl --
K8S_BASE=k8s
K8S_DEPLOY=$(K8S_BASE)/deployment.yaml

# all: generate-k8s-deployment
all:

describe:
	$(KUBECTL) cluster-info

forward-port:
	$(KUBECTL) port-forward service/v-spring-kafka-producer -n v-labs 9000:9000 --address='0.0.0.0'

generate-k8s-deployment:
	mkdir -p k8s
	$(KUBECTL) create deployment v-spring-kafka-producer \
		--image=v-labs/v-spring-kafka-producer \
		--dry-run=client -o=yaml \
		> $(K8S_DEPLOY)
	echo --- >> $(K8S_DEPLOY)
	$(KUBECTL) create service clusterip v-spring-kafka-producer \
		--tcp=8080:8080 --dry-run=client -o=yaml \
		>> $(K8S_DEPLOY)


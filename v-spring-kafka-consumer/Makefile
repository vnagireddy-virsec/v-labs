KUBECTL=minikube kubectl --
K8S_BASE=k8s
K8S_DEPLOY=$(K8S_BASE)/deployment.yaml
V_K8S_NAMESPACE=v-labs

all: generate-k8s-deployment apply-k8s-deployment

apply-k8s-deployment: generate-k8s-deployment
	$(KUBECTL) create namespace $(V_K8S_NAMESPACE) \
		--dry-run=client -o yaml \
		| $(KUBECTL) apply -f -
	$(KUBECTL) apply -f $(K8S_DEPLOY) -n $(V_K8S_NAMESPACE)

create-image:
	eval $(minikube docker-env)
	docker build -t v-spring-kafka-consumer .

describe:
	$(KUBECTL) cluster-info

generate-k8s-deployment:
	mkdir -p k8s
	$(KUBECTL) create deployment v-spring-kafka-consumer \
		--image=v-labs/v-spring-kafka-consumer \
		--dry-run=client -o=yaml > $(K8S_DEPLOY)
	echo --- >> $(K8S_DEPLOY)
	$(KUBECTL) create service clusterip v-spring-kafka-consumer \
		--tcp=8080:8080 --dry-run=client -o=yaml >> $(K8S_DEPLOY)


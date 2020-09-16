### Prometheus

* Install the Prometheus addon `kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.7/samples/addons/prometheus.yaml`.
* Verify it is running `kubectl get svc prometheus -n istio-system`.

To access the Prometheus dashboard run the following command:

`kubectl -n istio-system port-forward $(kubectl -n istio-system get pod -l app=prometheus -o jsonpath='{.items[0].metadata.name}') 9090:9090`

and go to localhost:9090.

### Grafana

* Install the Grafana addon `kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.7/samples/addons/grafana.yaml`.
* Verify it is running `kubectl get svc grafana -n istio-system`.


To access the Grafana dashboard run the following command:

`kubectl -n istio-system port-forward $(kubectl -n istio-system get pod -l app=grafana -o jsonpath='{.items[0].metadata.name}') 3000:3000`

and go to localhost:3000.

### Jaeger


* Install the Jaeger addon `kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.7/samples/addons/jaeger.yaml`.
* Verify it is running `kubectl get svc jaeger -n istio-system`.


To access the Jaeger dashboard run the following command:

`kubectl port-forward -n istio-system $(kubectl get pod -n istio-system -l app=jaeger -o jsonpath='{.items[0].metadata.name}') 16686:16686`

and go to localhost:16686.

### Kiali


* Install the Kiali addon `kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.7/samples/addons/kiali.yaml`.
* Verify it is running `kubectl get svc kiali -n istio-system`.


To access the Kiali dashboard run the following command:

`kubectl port-forward -n istio-system $(kubectl get pod -n istio-system -l app=kiali -o jsonpath='{.items[0].metadata.name}') 20001:20001`

and go to localhost:20001.

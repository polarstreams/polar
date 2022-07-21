package discovery

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/barcostreams/barco/internal/conf"
	"github.com/barcostreams/barco/internal/utils"
	"github.com/rs/zerolog/log"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const appNameLabel = "app.kubernetes.io/name"
const k8sBackoffDelayMs = 200
const k8sMaxBackoffDelayMs = 10_000

// Represents a wrapper around k8s api calls
type k8sClient interface {
	init(config conf.DiscovererConfig) error
	getDesiredReplicas() (int, error)
	startWatching(replicas int)
	replicasChangeChan() <-chan int
}

type k8sClientImpl struct {
	client       *kubernetes.Clientset
	appName      string
	namespace    string
	replicasChan chan int
}

func newK8sClient() k8sClient {
	return &k8sClientImpl{
		replicasChan: make(chan int),
	}
}

func (c *k8sClientImpl) init(config conf.DiscovererConfig) error {
	k8sRestConfig, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	log.Debug().Msgf("Creating new k8s config")
	client, err := kubernetes.NewForConfig(k8sRestConfig)
	if err != nil {
		return err
	}

	podName := config.PodName()
	c.namespace = config.PodNamespace()
	log.Debug().Str("pod", podName).Str("namespace", c.namespace).Msgf("Querying k8s api")

	if c.namespace == "" || podName == "" {
		return fmt.Errorf("K8s discovery requires BARCO_POD_NAME and BARCO_POD_NAMESPACE to be set")
	}

	pod, err := client.CoreV1().
		Pods(c.namespace).
		Get(context.TODO(), podName, metav1.GetOptions{})

	if err != nil {
		return err
	}

	if pod == nil {
		return fmt.Errorf("Could not find pod '%s' in namespace '%s'", podName, c.namespace)
	}

	c.client = client
	c.appName = pod.Labels[appNameLabel]
	return nil
}

func (c *k8sClientImpl) getDesiredReplicas() (int, error) {
	labelSelector := fmt.Sprintf("%s=%s", appNameLabel, c.appName)
	stsInfo := fmt.Sprintf("label selector '%s' in namespace '%s'", labelSelector, c.namespace)
	log.Debug().Msgf("Querying statefulset with %s", stsInfo)
	list, err := c.client.AppsV1().StatefulSets(c.namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return 0, err
	}
	if len(list.Items) == 0 {
		return 0, fmt.Errorf("No statefulset found with %s", stsInfo)
	}
	sts := list.Items[0]
	replicas := 1
	if sts.Spec.Replicas != nil {
		replicas = int(*sts.Spec.Replicas)
	}

	log.Debug().Msgf("Found %d replicas in statefulset with %s", replicas, stsInfo)
	if replicas == 0 {
		return 0, fmt.Errorf("Stateful has zero replicas (%s)", stsInfo)
	}
	return int(replicas), nil
}

func (c *k8sClientImpl) startWatching(replicas int) {
	labelSelector := fmt.Sprintf("%s=%s", appNameLabel, c.appName)
	retryCounter := 0
	for {
		log.Debug().Msgf("Creating a k8s watch")
		watcher, err := c.client.AppsV1().StatefulSets(c.namespace).Watch(context.TODO(), metav1.ListOptions{
			LabelSelector: labelSelector,
		})

		if err != nil {
			log.Warn().Err(err).Msgf("There was an error trying to watch k8s resources")
			retryCounter++
			delayMs := math.Pow(2, float64(retryCounter)) * k8sBackoffDelayMs
			if delayMs > k8sMaxBackoffDelayMs {
				delayMs = k8sMaxBackoffDelayMs
			} else {
				retryCounter++
			}
			time.Sleep(utils.Jitter(time.Duration(delayMs) * time.Millisecond))
			continue
		}

		log.Info().Msgf("K8s watch added")
		retryCounter = 0

		for event := range watcher.ResultChan() {
			if event.Type == watch.Error {
				log.Warn().Interface("event", event.Object).Msgf("K8s watcher received an error")
				break
			}

			log.Debug().Str("type", string(event.Type)).Msgf("K8s Received")
			sts, ok := event.Object.(*v1.StatefulSet)
			if !ok {
				log.Error().Interface("event", event.Object).Msgf("Unexpected event object type")
				break
			}

			eventReplicas := 1
			if sts.Spec.Replicas != nil {
				eventReplicas = int(*sts.Spec.Replicas)
			}

			if eventReplicas != replicas {
				replicas = eventReplicas
				c.replicasChan <- replicas
			}
		}

		log.Info().Msgf("k8s watcher channel closed")
		// Can be called multiple times
		watcher.Stop()
	}
}

func (c *k8sClientImpl) replicasChangeChan() <-chan int {
	return c.replicasChan
}

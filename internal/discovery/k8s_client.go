package discovery

import (
	"context"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	envPodName      = "POD_NAME"
	envPodNamespace = "POD_NAMESPACE"
)

// Represents a wrapper around k8s api calls
type k8sClient interface {
	init() error
	getDesiredReplicas() (int, error)
}

type k8sClientImpl struct {
	client    *kubernetes.Clientset
	appName   string
	namespace string
}

func newK8sClient() k8sClient {
	namespace := os.Getenv(envPodNamespace)
	return &k8sClientImpl{
		namespace: namespace,
	}
}

func (c *k8sClientImpl) init() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	log.Debug().Msgf("Creating new k8s config")
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	podName := os.Getenv(envPodName)
	namespace := os.Getenv(envPodNamespace)
	log.Debug().Msgf("Querying k8s api")

	pod, err := client.CoreV1().
		Pods(namespace).
		Get(context.TODO(), podName, metav1.GetOptions{})

	if err != nil {
		return err
	}

	if pod == nil {
		return fmt.Errorf("Could not find pod '%s' in namespace '%s'", podName, namespace)
	}

	c.client = client
	c.appName = pod.Labels["app.kubernetes.io/name"]
	return nil
}

func (c *k8sClientImpl) getDesiredReplicas() (int, error) {
	labelSelector := fmt.Sprintf("app.kubernetes.io/name=%s", c.appName)
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
	replicas := sts.Status.Replicas
	log.Debug().Msgf("Found %d replicas in statefulset with %s", replicas, stsInfo)
	if replicas == 0 {
		return 0, fmt.Errorf("Stateful has zero replicas (%s)", stsInfo)
	}
	return int(replicas), nil
}

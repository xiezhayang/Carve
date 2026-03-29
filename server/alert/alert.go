package alert

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Runner 创建/删除告警器 Deployment（无 Service，告警器通过 Carve 的 Service 建 WebSocket）
type Runner struct {
	CarveURL        string
	AlerterImage    string
	DeployNamespace string
	getClientset    func() (kubernetes.Interface, error)
}

var sanitizeRe = regexp.MustCompile(`[^a-z0-9-]`)

func sanitizeTarget(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "_", "-")
	s = sanitizeRe.ReplaceAllString(s, "")
	if len(s) > 40 {
		s = s[:40]
	}
	return strings.Trim(s, "-")
}

func shortID() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// DeploymentName 生成告警器 Deployment 名：alerter-<target>-<id>
func DeploymentName(target, modelName string) (string, error) {
	id, err := shortID()
	if err != nil {
		return "", err
	}
	base := "alerter-" + sanitizeTarget(target)
	if base == "alerter-" {
		base = "alerter-default"
	}
	return base + "-" + id, nil
}

// NewRunnerInCluster 使用 in-cluster 配置
func NewRunnerInCluster(carveURL, alerterImage, deployNamespace string) (*Runner, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Runner{
		CarveURL:        carveURL,
		AlerterImage:    alerterImage,
		DeployNamespace: deployNamespace,
		getClientset:    func() (kubernetes.Interface, error) { return cs, nil },
	}, nil
}

// NewRunnerFromKubeconfig 使用 kubeconfig
func NewRunnerFromKubeconfig(kubeconfig, carveURL, alerterImage, deployNamespace string) (*Runner, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Runner{
		CarveURL:        carveURL,
		AlerterImage:    alerterImage,
		DeployNamespace: deployNamespace,
		getClientset:    func() (kubernetes.Interface, error) { return cs, nil },
	}, nil
}

func ptr[T any](v T) *T { return &v }

// Deploy 创建告警器 Deployment；告警器启动后会连 Carve WebSocket、拉模型并运行
func (r *Runner) Deploy(ctx context.Context, target, modelName string) (deploymentName string, err error) {
	cs, err := r.getClientset()
	if err != nil {
		return "", err
	}
	deploymentName, err = DeploymentName(target, modelName)
	if err != nil {
		return "", fmt.Errorf("deployment name: %w", err)
	}
	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.DeployNamespace,
			Name:      deploymentName,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": deploymentName},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": deploymentName},
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "9092",
						"prometheus.io/path":   "/metrics",
						"prometheus.io/scheme": "http",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "alerter",
							Image:           r.AlerterImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{Name: "CARVE_URL", Value: r.CarveURL},
								{Name: "TARGET", Value: target},
								{Name: "MODEL_NAME", Value: modelName},
							},
							Ports: []corev1.ContainerPort{{ContainerPort: 9092, Name: "metrics"}},
						},
					},
				},
			},
		},
	}
	_, err = cs.AppsV1().Deployments(r.DeployNamespace).Create(ctx, dep, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create deployment: %w", err)
	}
	return deploymentName, nil
}

// Delete 删除指定名称的告警器 Deployment
func (r *Runner) Delete(ctx context.Context, deploymentName string) error {
	cs, err := r.getClientset()
	if err != nil {
		return err
	}
	return cs.AppsV1().Deployments(r.DeployNamespace).Delete(ctx, deploymentName, metav1.DeleteOptions{})
}

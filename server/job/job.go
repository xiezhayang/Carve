package job

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Runner 根据配置创建训练 Job
type Runner struct {
	CarveURL     string
	Image        string
	Namespace    string
	getClientset func() (kubernetes.Interface, error)
}

func SanitizeJobName(modelName string) string {
	re := regexp.MustCompile(`[^a-z0-9-]`)
	s := strings.ToLower(modelName)
	s = strings.ReplaceAll(s, "_", "-")
	s = re.ReplaceAllString(s, "")
	if len(s) > 52 {
		s = s[:52]
	}
	return strings.Trim(s, "-")
}

// JobName 返回该 model 对应的 Job 名
func JobName(modelName string) string {
	return "train-" + SanitizeJobName(modelName)
}

// NewRunner 使用 in-cluster 配置（Carve 跑在 K8s 里时用）
func NewRunnerInCluster(carveURL, image, namespace string) (*Runner, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Runner{
		CarveURL:     carveURL,
		Image:        image,
		Namespace:    namespace,
		getClientset: func() (kubernetes.Interface, error) { return cs, nil },
	}, nil
}

// NewRunnerFromKubeconfig 使用 kubeconfig（本地或 Carve 不在集群内时用）
func NewRunnerFromKubeconfig(kubeconfig, carveURL, trainerImage, jobNamespace string) (*Runner, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Runner{
		CarveURL:     carveURL,
		Image:        trainerImage,
		Namespace:    jobNamespace,
		getClientset: func() (kubernetes.Interface, error) { return cs, nil },
	}, nil
}

func ptr[T any](v T) *T { return &v }

// Run 创建并提交一个训练 Job
func (r *Runner) Run(ctx context.Context, csvFilename, modelName string) error {
	cs, err := r.getClientset()
	if err != nil {
		return err
	}
	jobName := JobName(modelName)
	activeDeadline := int64(3600)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.Namespace,
			Name:      jobName,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          ptr(int32(3)),
			ActiveDeadlineSeconds: &activeDeadline,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "trainer",
							Image: r.Image,
							Env: []corev1.EnvVar{
								{Name: "CARVE_URL", Value: r.CarveURL},
								{Name: "CSV_FILENAME", Value: csvFilename},
								{Name: "MODEL_NAME", Value: modelName},
							},
						},
					},
				},
			},
		},
	}
	_, err = cs.BatchV1().Jobs(r.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	return nil
}

// DeleteJob 删除指定名称的 Job（用于收到模型上传后清理）
func (r *Runner) DeleteJob(ctx context.Context, jobName string) error {
	cs, err := r.getClientset()
	if err != nil {
		return err
	}
	return cs.BatchV1().Jobs(r.Namespace).Delete(ctx, jobName, metav1.DeleteOptions{})
}

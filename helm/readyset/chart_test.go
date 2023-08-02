package test

import (
	// "fmt"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	// networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/gruntwork-io/terratest/modules/k8s"
	"github.com/gruntwork-io/terratest/modules/logger"
	"github.com/gruntwork-io/terratest/modules/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
)

func loadChartYaml(path string) (*chart.Chart, error) {
	chart, err := loader.Load(".")
	return chart, err
}

func cliValues() map[string]string {
	// provide the ability to define a default and allow for overrides
	values := make(map[string]string)

	values["readyset.deployment"] = "helm-test-readyset"

	return values
}

func defaultOptions(namespace string, values map[string]string) *helm.Options {
	return &helm.Options{
		ValuesFiles:    []string{"values.yaml"},
		SetValues:      values,
		KubectlOptions: k8s.NewKubectlOptions("", "", namespace),
		Version:        "readyset-0.9.1",
		Logger:         logger.Discard,
		// ExtraArgs:         map[string][]string{"repoAdd": []string{"--repository-config", "testdata/test-repositories.yaml"}},
		BuildDependencies: true,
	}
}

func generateNamespaceName() string {
	return "readyset-" + strings.ToLower(random.UniqueId())
}

func TestChart(t *testing.T) {
	_, err := loadChartYaml(".")
	require.NoError(t, err)
}

func TestAdapterDeploymentDefault(t *testing.T) {
	assert := assert.New(t)

	chart, err := loadChartYaml(".")
	require.NoError(t, err)

	namespace := generateNamespaceName()
	chartValues := cliValues()

	options := defaultOptions(namespace, chartValues)

	helmChartPath, err := filepath.Abs(".")
	require.NoError(t, err)

	helm.AddRepo(t, options, "readyset", "https://helm.releases.readyset.io")
	helm.AddRepo(t, options, "hashicorp", "https://helm.releases.hashicorp.com")

	deploymentName := "readyset-adapter"

	var adapterDeployment appsv1.Deployment

	renderedDeploymentTemplate, err := helm.RenderTemplateE(t, options, helmChartPath, "readyset", []string{"templates/readyset-adapter-deployment.yaml"})
	require.NoError(t, err)

	helm.UnmarshalK8SYaml(t, renderedDeploymentTemplate, &adapterDeployment)

	assert.Equal(deploymentName, adapterDeployment.Name, "Deployments should be equal")
	assert.Equal(namespace, adapterDeployment.ObjectMeta.Namespace, "Namespaces should be equal")
	assert.Equal(options.Version, adapterDeployment.ObjectMeta.Labels["helm.sh/chart"], "Versions should be equal")
	assert.Equal(chart.Metadata.AppVersion, adapterDeployment.ObjectMeta.Labels["app.kubernetes.io/version"], "Versions should be equal")
	// Containers[1].Env[4] in this case is the container "readyset-adapter" and the env var "QUERY_CACHING"
	assert.Equal("explicit", adapterDeployment.Spec.Template.Spec.Containers[1].Env[4].Value, "Query caching mode should equal 'explicit'")
}

func TestAdapterDeploymentCachingModeInRequestPath(t *testing.T) {
	assert := assert.New(t)

	namespace := generateNamespaceName()
	chartValues := cliValues()

	// Set values as though they are passed via the CLI
	chartValues["readyset.queryCachingMode"] = "in-request-path"

	options := defaultOptions(namespace, chartValues)

	helmChartPath, err := filepath.Abs(".")
	require.NoError(t, err)

	helm.AddRepo(t, options, "readyset", "https://helm.releases.readyset.io")
	helm.AddRepo(t, options, "hashicorp", "https://helm.releases.hashicorp.com")

	deploymentName := "readyset-adapter"

	var adapterDeployment appsv1.Deployment

	renderedDeploymentTemplate, err := helm.RenderTemplateE(t, options, helmChartPath, "readyset", []string{"templates/readyset-adapter-deployment.yaml"})
	require.NoError(t, err)

	helm.UnmarshalK8SYaml(t, renderedDeploymentTemplate, &adapterDeployment)

	// Standard tests
	assert.Equal(deploymentName, adapterDeployment.Name, "Deployments should be equal")
	assert.Equal(namespace, adapterDeployment.ObjectMeta.Namespace, "Namespaces should be equal")
	assert.Equal(options.Version, adapterDeployment.ObjectMeta.Labels["helm.sh/chart"], "Versions should be equal")
	assert.Equal(options.SetValues["readyset.deployment"], adapterDeployment.ObjectMeta.Labels["app.kubernetes.io/instance"], "app.kubernetes.io/instance should be equal")

	adapterContainer := adapterDeployment.Spec.Template.Spec.Containers[1]

	// Containers[1].Env[4] in this case is the container "readyset-adapter" and the env var "QUERY_CACHING"
	assert.Equal(options.SetValues["readyset.queryCachingMode"], adapterContainer.Env[4].Value, "Query caching mode should equal 'in-request-path'")
}

func TestServerStatefulSetDefault(t *testing.T) {
	assert := assert.New(t)

	namespace := generateNamespaceName()
	chartValues := cliValues()

	options := defaultOptions(namespace, chartValues)

	helmChartPath, err := filepath.Abs(".")
	require.NoError(t, err)

	helm.AddRepo(t, options, "readyset", "https://helm.releases.readyset.io")
	helm.AddRepo(t, options, "hashicorp", "https://helm.releases.hashicorp.com")

	deploymentName := "readyset-server"

	var serverStatefulSet appsv1.StatefulSet

	renderedDeploymentTemplate, err := helm.RenderTemplateE(t, options, helmChartPath, "readyset", []string{"templates/readyset-server-statefulset.yaml"})
	require.NoError(t, err)

	helm.UnmarshalK8SYaml(t, renderedDeploymentTemplate, &serverStatefulSet)

	containers := serverStatefulSet.Spec.Template.Spec.Containers
	containersExpected := 2
	containersActual := len(containers)

	assert.Equal(containersExpected, containersActual, fmt.Sprintf("Expected number of containers: %d, actual: %d", containersExpected, containersActual))

	assert.Equal(deploymentName, serverStatefulSet.Name, "Deployments should be equal")
	assert.Equal(namespace, serverStatefulSet.ObjectMeta.Namespace, fmt.Sprintf("Namespaces should be equal: %v\n", serverStatefulSet.ObjectMeta))
	assert.Equal(options.Version, serverStatefulSet.ObjectMeta.Labels["helm.sh/chart"], "Versions should be equal")
	assert.Equal(options.SetValues["readyset.deployment"], serverStatefulSet.ObjectMeta.Labels["app.kubernetes.io/instance"], "app.kubernetes.io/instance should be equal")

	// The default values should yield an environment with 15 elemnents for readyset-server
	arrayLen := 15
	assert.Equal(arrayLen, len(containers[1].Env), fmt.Sprintf("Length of environment variable array should be %d", arrayLen))

	// Ensure none of the env vars enable replication tables
	for _, v := range containers[1].Env {
		assert.NotEqual("REPLICATION_TABLES", v.Name)
	}
}

func TestServerStatefulSetWithReplicationTables(t *testing.T) {
	assert := assert.New(t)

	namespace := generateNamespaceName()
	chartValues := cliValues()

	// Set values as though they are passed via the CLI
	chartValues["readyset.server.replication_tables"] = "public.foo"

	options := defaultOptions(namespace, chartValues)

	helmChartPath, err := filepath.Abs(".")
	require.NoError(t, err)

	helm.AddRepo(t, options, "readyset", "https://helm.releases.readyset.io")
	helm.AddRepo(t, options, "hashicorp", "https://helm.releases.hashicorp.com")

	deploymentName := "readyset-server"

	var serverStatefulSet appsv1.StatefulSet

	renderedDeploymentTemplate, err := helm.RenderTemplateE(t, options, helmChartPath, "readyset", []string{"templates/readyset-server-statefulset.yaml"})
	require.NoError(t, err)

	helm.UnmarshalK8SYaml(t, renderedDeploymentTemplate, &serverStatefulSet)

	assert.Equal(deploymentName, serverStatefulSet.Name, "Deployments should be equal")
	assert.Equal(namespace, serverStatefulSet.ObjectMeta.Namespace, fmt.Sprintf("Namespaces should be equal: %v\n", serverStatefulSet.ObjectMeta))
	assert.Equal(options.Version, serverStatefulSet.ObjectMeta.Labels["helm.sh/chart"], "Versions should be equal")
	assert.Equal(options.SetValues["readyset.deployment"], serverStatefulSet.ObjectMeta.Labels["app.kubernetes.io/instance"], "app.kubernetes.io/instance should be equal")
	assert.Equal(options.SetValues["readyset.server.replication_tables"], serverStatefulSet.Spec.Template.Spec.Containers[1].Env[15].Value, "REPLICATION_TABLES should be 'public.foo'")
}

func TestAdapterRoles(t *testing.T) {
	namespace := generateNamespaceName()
	chartValues := cliValues()

	options := defaultOptions(namespace, chartValues)

	helmChartPath, err := filepath.Abs(".")
	require.NoError(t, err)

	helm.AddRepo(t, options, "readyset", "https://helm.releases.readyset.io")
	helm.AddRepo(t, options, "hashicorp", "https://helm.releases.hashicorp.com")

	var adapterRole rbacv1.Role
	renderedRbacTemplate, err := helm.RenderTemplateE(t, options, helmChartPath, "readyset", []string{"templates/readyset-adapter-role.yaml"})
	require.NoError(t, err)
	helm.UnmarshalK8SYaml(t, renderedRbacTemplate, &adapterRole)
}

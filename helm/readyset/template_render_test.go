package test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"

	"github.com/gruntwork-io/terratest/modules/helm"
	"github.com/gruntwork-io/terratest/modules/k8s"
	"github.com/gruntwork-io/terratest/modules/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Repo struct {
	Name string `json:"name"`
	Url  string `json:"url"`
}

func TestTemplateRender(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	helmChartPath, err := filepath.Abs(".")
	require.NoError(t, err)

	deploymentName := "readyset-adapter"
	namespaceName := "readyset-" + strings.ToLower(random.UniqueId())

	options := &helm.Options{
		ValuesFiles:       []string{},
		SetValues:         map[string]string{"readyset.databaseUri": "mysql://user:pass@host/database", "consul.enabled": "true"},
		SetStrValues:      map[string]string{},
		SetJsonValues:     map[string]string{},
		SetFiles:          map[string]string{},
		KubectlOptions:    k8s.NewKubectlOptions("", "", namespaceName),
		HomePath:          "",
		EnvVars:           map[string]string{},
		Version:           "readyset-0.4.0-rc3",
		ExtraArgs:         map[string][]string{},
		BuildDependencies: true,
	}

	helm.AddRepo(t, options, "readyset", "https://helm.releases.readyset.io")
	defer helm.RemoveRepo(t, options, "readyset")

	helm.AddRepo(t, options, "hashicorp", "https://helm.releases.hashicorp.com")
	defer helm.RemoveRepo(t, options, "hashicorp")

	cmd, err := helm.RunHelmCommandAndGetStdOutE(t, options, "repo", "list", "-o", "json")
	require.NoError(t, err)

	var r []Repo
	json.Unmarshal([]byte(cmd), &r)

	assert.Equal("readyset", r[0].Name)

	renderedTemplate := helm.RenderTemplate(t, options, helmChartPath, "readyset", []string{"templates/readyset-adapter-deployment.yaml"})

	var deployment appsv1.Deployment
	helm.UnmarshalK8SYaml(t, renderedTemplate, &deployment)

	assert.Equal(deploymentName, deployment.Name, "Deployments should be equal")
	assert.Equal(namespaceName, deployment.ObjectMeta.Namespace, "Namespaces should be equal")
	assert.Equal(options.Version, deployment.ObjectMeta.Labels["helm.sh/chart"], "Versions should be equal")
}

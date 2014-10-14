package config_api_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestConfigApi(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ConfigApi Suite")
}

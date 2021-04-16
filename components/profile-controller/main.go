/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"unicode"

	settingsapi "github.com/kubeflow/kubeflow/components/profile-controller/api/settings/v1alpha1"
	profilev1 "github.com/kubeflow/kubeflow/components/profile-controller/api/v1"
	"github.com/kubeflow/kubeflow/components/profile-controller/controllers"
	istioSecurityClient "istio.io/client-go/pkg/apis/security/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	// +kubebuilder:scaffold:imports
)

const USERIDHEADER = "userid-header"
const USERIDPREFIX = "userid-prefix"
const WORKLOADIDENTITY = "workload-identity"
const PODDEFAULTS = "pd"

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func removeUnquotedSpace(s string) (string, error) {
	rs := make([]rune, 0, len(s))
	const out = rune(0)
	var quote rune = out
	var escape = false
	for _, r := range s {
		if !escape {
			if r == '`' || r == '"' || r == '\'' {
				if quote == out {
					// start unescaped quote
					quote = r
				} else if quote == r {
					// end (matching) unescaped quote
					quote = out
				}
			}
		}
		// backslash (\) is the escape character
		// except when it is the second backslash of a pair
		escape = !escape && r == '\\'
		if quote != out || !unicode.IsSpace(r) {
			// between matching unescaped quotes
			// or not whitespace
			rs = append(rs, r)
		}
	}
	if quote != out {
		err := fmt.Errorf("unmatched unescaped quote: %q", quote)
		return "", err
	}
	return string(rs), nil
}

func sliceUniqMap(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	j := 0
	for _, v := range s {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		s[j] = v
		j++
	}
	return s[:j]
}

func SplitNotInQuotes(s string, sep string) []string {
	res := []string{}
	var beg int
	var inString string

	for i := 0; i < len(s); i++ {
		if s[i] == sep[0] && inString == "" {
			res = append(res, s[beg:i])
			beg = i + 1
		} else if s[i] == '"' || s[i] == '\'' {
			if inString == "" {
				inString = string(s[i])
			} else if i > 0 && s[i-1] != '\\' {
				inString = ""
			}
		}
	}
	return append(res, s[beg:])
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func parsePodDefaults(podDefaults string) map[string]interface{} {
	csv, err := removeUnquotedSpace(strings.Replace(podDefaults, "\n", "", -1))
	if err != nil {
		setupLog.Error(err, "unable to trim spaces")
		os.Exit(1)
	}

	entries := SplitNotInQuotes(csv, ",")
	var selectors []string
	for _, e := range entries {
		selectors = append(selectors, SplitNotInQuotes(e, ".")[0])
	}
	selectorsFiltered := sliceUniqMap(selectors)
	validFields := map[string]bool{
		"labels": true,
	}
	selectorsMap := make(map[string]interface{})

	for _, s := range selectorsFiltered {
		fieldsMap := make(map[string][]string)
		for _, e := range entries {
			parts := SplitNotInQuotes(e, ".")
			selector := strings.ToLower(parts[0])
			field := strings.ToLower(parts[1])
			kvs := strings.Join(parts[2:], ".")
			// kv := strings.Split(kvs, "=")

			if selector == s && validFields[field] {
				fieldsMap[field] = append(fieldsMap[field], kvs)
			}
		}
		selectorsMap[s] = fieldsMap
	}

	return selectorsMap
}

func init() {
	_ = clientgoscheme.AddToScheme(scheme)

	_ = profilev1.AddToScheme(scheme)
	_ = istioSecurityClient.AddToScheme(scheme)
	_ = settingsapi.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr, leaderElectionNamespace string
	var enableLeaderElection bool
	var userIdHeader string
	var userIdPrefix string
	var workloadIdentity string
	var podDefaults string
	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&leaderElectionNamespace, "leader-election-namespace", "",
		"Determines the namespace in which the leader election configmap will be created.")
	flag.StringVar(&userIdHeader, USERIDHEADER, "x-goog-authenticated-user-email", "Key of request header containing user id")
	flag.StringVar(&userIdPrefix, USERIDPREFIX, "accounts.google.com:", "Request header user id common prefix")
	flag.StringVar(&workloadIdentity, WORKLOADIDENTITY, "", "Default identity (GCP service account) for workload_identity plugin")
	flag.StringVar(&podDefaults, PODDEFAULTS, "", "Comma separated list of PodDefaults Spec Fields")
	// os.Args = []string{"main", "-pd=ddpd-pod-labels.Labels.project=shared,ddpd-pod-labels.Labels.sub-project=ddpd,whitespace-pod-labels.Labels.project=shared,whitespace-pod-labels.Labels.sub-project=whitespace,whitespace-pod-labels.VolumeMounts.name=tmp-volume"}
	flag.Parse()

	ctrl.SetLogger(zap.Logger(true))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		MetricsBindAddress:      metricsAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionNamespace: leaderElectionNamespace,
		LeaderElectionID:        "kubeflow-profile-controller",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	pd := make(map[string]interface{})

	if len(podDefaults) > 0 {
		pd = parsePodDefaults(podDefaults)
	}

	if err = (&controllers.ProfileReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		Log:              ctrl.Log.WithName("controllers").WithName("Profile"),
		UserIdHeader:     userIdHeader,
		UserIdPrefix:     userIdPrefix,
		WorkloadIdentity: workloadIdentity,
		PodDefaults:      pd,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Profile")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

package detector

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/ThuatPhan/PodSleuth/internal/domain"
)

// fingerprint derives the stable episode identity for a failure. It
// incorporates cluster, namespace, workload UID, Pod UID, container and
// failure type; the Pod name is never used on its own, so a Pod
// recreated with the same name but a new UID yields a new identity.
func fingerprint(clusterID string, pod *domain.PodView, container, failureType string) string {
	workloadUID := ""
	if o := pod.ControllerOwner(); o != nil {
		workloadUID = o.UID
	}
	parts := []string{clusterID, pod.Metadata.Namespace, workloadUID, pod.Metadata.UID, container, failureType}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

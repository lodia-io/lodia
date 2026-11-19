// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import "testing"

const servedBy135 = `apiserver_authentication_config_controller_last_config_info{apiserver_id_hash="sha256:5333b840aaee430c4c16054212b29b756157a6a08b811d3746aea5245088bef7",hash="sha256:99a9052851e2c23867f82eb4c038761a1b9a5a3238a501d6f19536f37990453c"} 1
`

func TestTheLoadedConfigurationsHashIsItsLabelHash(t *testing.T) {
	got, err := loadedConfigHash(servedBy135)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sha256:99a9052851e2c23867f82eb4c038761a1b9a5a3238a501d6f19536f37990453c"; got != want {
		t.Errorf("the hash is %s, want %s", got, want)
	}
	for _, metrics := range []string{
		"",
		`apiserver_authentication_config_controller_last_config_info{apiserver_id_hash="sha256:5333b840aaee430c4c16054212b29b756157a6a08b811d3746aea5245088bef7"} 1` + "\n",
	} {
		if hash, err := loadedConfigHash(metrics); err == nil {
			t.Errorf("%q gives the hash %s", metrics, hash)
		}
	}
}

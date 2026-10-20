package main

import (
	"strings"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	c := Config{Host: "h", Database: "d", Username: "u"}
	c.applyDefaults()
	assert.Equal(t, 1433, c.Port)
	assert.Equal(t, "dbo", c.Schema)
	assert.Equal(t, sslTrust, c.SSLMethod.SSLMethod)
	require.NoError(t, c.validate())
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		cfg  Config
		want string
	}{
		{Config{Database: "d", Username: "u"}, "host is required"},
		{Config{Host: "h", Username: "u"}, "database is required"},
		{Config{Host: "h", Database: "d"}, "username is required"},
		{Config{Host: "h", Database: "d", Username: `CORP\u`}, "Windows account"},
		{Config{Host: "h", Database: "d", Username: "u", Port: 70000}, "out of range"},
		{Config{Host: "h", Database: "d", Username: "u", Schema: strings.Repeat("s", 129)}, "longer than 128 characters"},
		{Config{Host: "h", Database: "d", Username: "u", Schema: "raw "}, "ends with a space"},
		{Config{Host: "h", Database: "d", Username: "u", SSLMethod: SSLMethod{SSLMethod: "bogus"}}, `ssl_method "bogus"`},
		{Config{Host: "h", Database: "d", Username: "u", SSLMethod: SSLMethod{SSLMethod: sslTrust, Certificate: "x"}}, "takes no certificate"},
	}
	for _, c := range cases {
		c.cfg.applyDefaults()
		err := c.cfg.validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), c.want)
	}
}

func TestConfigDriver(t *testing.T) {
	base := Config{Host: "db.internal", Database: "d", Username: "u", Password: "p"}
	base.applyDefaults()

	mc, err := msConfig(base, base.Database)
	require.NoError(t, err)
	assert.Equal(t, "db.internal", mc.Host)
	assert.Equal(t, uint64(1433), mc.Port)
	assert.Equal(t, "d", mc.Database)
	assert.Equal(t, msdsn.Encryption(msdsn.EncryptionRequired), mc.Encryption)
	assert.True(t, mc.TLSConfig.InsecureSkipVerify)

	plain := base
	plain.SSLMethod.SSLMethod = sslUnencrypted
	mc, err = msConfig(plain, plain.Database)
	require.NoError(t, err)
	assert.Equal(t, msdsn.Encryption(msdsn.EncryptionOff), mc.Encryption)

	verify := base
	verify.SSLMethod = SSLMethod{SSLMethod: sslVerify, HostNameInCertificate: "sql.example.com"}
	mc, err = msConfig(verify, verify.Database)
	require.NoError(t, err)
	assert.False(t, mc.TLSConfig.InsecureSkipVerify)
	assert.Equal(t, "sql.example.com", mc.TLSConfig.ServerName)
	assert.True(t, mc.HostInCertificateProvided)

	bad := base
	bad.SSLMethod = SSLMethod{SSLMethod: sslVerify, Certificate: "not pem"}
	_, err = msConfig(bad, bad.Database)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "holds no PEM certificate")
}

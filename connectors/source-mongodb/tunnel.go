package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"golang.org/x/crypto/ssh"
)

func dialSSH(ctx context.Context, t TunnelMethod) (*ssh.Client, error) {
	var auth ssh.AuthMethod
	if t.TunnelMethod == tunnelKey {
		signer, err := ssh.ParsePrivateKey([]byte(t.SSHKey))
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("ssh_key: %w", err))
		}
		auth = ssh.PublicKeys(signer)
	} else {
		auth = ssh.Password(t.TunnelUserPassword)
	}
	hostKey := ssh.InsecureIgnoreHostKey() //nolint:gosec // no tunnel_host_key configured: the host key is not verified
	var hostKeyAlgos []string
	if t.TunnelHostKey != "" {
		keys, err := parseHostKeys(t.TunnelHostKey)
		if err != nil {
			return nil, synclet.ConfigError(err)
		}
		hostKey = pinnedHostKeys(keys)
		hostKeyAlgos = hostKeyAlgorithms(keys)
	}
	addr := net.JoinHostPort(t.TunnelHost, strconv.Itoa(t.TunnelPort))
	sc := &ssh.ClientConfig{
		User:              t.TunnelUser,
		Auth:              []ssh.AuthMethod{auth},
		HostKeyCallback:   hostKey,
		HostKeyAlgorithms: hostKeyAlgos,
		Timeout:           connectTimeout,
	}
	var d net.Dialer
	dctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	nc, err := d.DialContext(dctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("cannot reach SSH tunnel host %s: %w", addr, err)
	}
	conn, chans, reqs, err := ssh.NewClientConn(nc, addr, sc)
	if err != nil {
		_ = nc.Close()
		return nil, synclet.ConfigError(fmt.Errorf("cannot open SSH tunnel as %q at %s: %w", t.TunnelUser, addr, err))
	}
	return ssh.NewClient(conn, chans, reqs), nil
}

// parseHostKeys reads the keys of tunnel_host_key, one per line, each in
// authorized_keys format ("ssh-ed25519 AAAA... comment") or as a known_hosts
// line ("host ssh-ed25519 AAAA...") as ssh-keyscan prints it. Blank lines and
// # comments are skipped; marked known_hosts lines (@revoked,
// @cert-authority) are refused.
func parseHostKeys(s string) ([]ssh.PublicKey, error) {
	var keys []ssh.PublicKey
	for i, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "" || strings.HasPrefix(line, "#"):
			continue
		case strings.HasPrefix(line, "@"):
			marker, _, _ := strings.Cut(line, " ")
			return nil, fmt.Errorf("tunnel_host_key line %d is marked %s; only plain host keys can be pinned", i+1, marker)
		}
		if key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line)); err == nil {
			keys = append(keys, key)
			continue
		}
		_, _, key, _, _, err := ssh.ParseKnownHosts([]byte(line))
		if err != nil {
			return nil, fmt.Errorf("tunnel_host_key line %d is not a public key in authorized_keys or known_hosts format", i+1)
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, errors.New("tunnel_host_key holds no public key")
	}
	return keys, nil
}

// pinnedHostKeys accepts a server that presents one of keys.
func pinnedHostKeys(keys []ssh.PublicKey) ssh.HostKeyCallback {
	return func(_ string, _ net.Addr, presented ssh.PublicKey) error {
		for _, k := range keys {
			if bytes.Equal(k.Marshal(), presented.Marshal()) {
				return nil
			}
		}
		return errors.New("ssh: host key mismatch")
	}
}

// hostKeyAlgorithms are the host key algorithms the client offers, so the
// server presents a pinned key rather than another of its keys.
func hostKeyAlgorithms(keys []ssh.PublicKey) []string {
	var algos []string
	for _, k := range keys {
		names := []string{k.Type()}
		if k.Type() == ssh.KeyAlgoRSA {
			names = []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA}
		}
		for _, n := range names {
			if !slices.Contains(algos, n) {
				algos = append(algos, n)
			}
		}
	}
	return algos
}

// tunnelDialer opens connections through the SSH client. SSH channels have
// no deadlines, which the driver sets on every connection, so each channel
// is joined to one end of a net.Pipe and the other end is returned.
func tunnelDialer(sc *ssh.Client) dialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		remote, err := sc.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		local, inner := net.Pipe()
		go func() {
			_, _ = io.Copy(inner, remote)
			_ = inner.Close()
		}()
		go func() {
			_, _ = io.Copy(remote, inner)
			_ = remote.Close()
		}()
		return local, nil
	}
}

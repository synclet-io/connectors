package main

import (
	"context"
	"fmt"
	"io"
	"net"

	"golang.org/x/crypto/ssh"
)

// SSHTunnel provides an SSH tunnel to a MySQL server.
type SSHTunnel struct {
	listener  net.Listener
	sshClient *ssh.Client
}

// newSSHTunnel creates an SSH tunnel that forwards a local port to the remote MySQL server.
func newSSHTunnel(cfg TunnelConfig, mysqlHost string, mysqlPort int) (*SSHTunnel, error) {
	var authMethod ssh.AuthMethod
	if cfg.SSHKey != "" {
		signer, err := ssh.ParsePrivateKey([]byte(cfg.SSHKey))
		if err != nil {
			return nil, fmt.Errorf("failed to parse SSH private key: %w", err)
		}
		authMethod = ssh.PublicKeys(signer)
	} else {
		authMethod = ssh.Password(cfg.Password)
	}

	sshConfig := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{authMethod},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	sshClient, err := ssh.Dial("tcp", sshAddr, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SSH server %s: %w", sshAddr, err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		sshClient.Close()
		return nil, fmt.Errorf("failed to start local listener: %w", err)
	}

	remoteAddr := fmt.Sprintf("%s:%d", mysqlHost, mysqlPort)

	go func() {
		for {
			localConn, err := listener.Accept()
			if err != nil {
				return
			}

			remoteConn, err := sshClient.Dial("tcp", remoteAddr)
			if err != nil {
				localConn.Close()
				continue
			}

			go forward(localConn, remoteConn)
		}
	}()

	return &SSHTunnel{
		listener:  listener,
		sshClient: sshClient,
	}, nil
}

func forward(local, remote net.Conn) {
	defer local.Close()
	defer remote.Close()

	done := make(chan struct{}, 1)
	go func() {
		io.Copy(local, remote)
		done <- struct{}{}
	}()
	go func() {
		io.Copy(remote, local)
		done <- struct{}{}
	}()
	<-done
}

// LocalAddr returns the local address of the tunnel listener (127.0.0.1:PORT).
func (t *SSHTunnel) LocalAddr() string {
	return t.listener.Addr().String()
}

// Close shuts down the tunnel listener and SSH client.
func (t *SSHTunnel) Close() error {
	t.listener.Close()
	return t.sshClient.Close()
}

// Dialer returns a function that dials through the SSH tunnel.
func (t *SSHTunnel) Dialer() func(ctx context.Context, addr string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		return t.sshClient.Dial("tcp", addr)
	}
}

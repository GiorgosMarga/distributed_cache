package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type managedServer struct {
	addr string
	cmd  *exec.Cmd
}

type clusterManager struct {
	mu            sync.Mutex
	serverBin     string
	bootstrapAddr string
	processes     map[string]*managedServer
}

func newClusterManager(serverBin string, bootstrapAddr string) *clusterManager {
	return &clusterManager{
		serverBin:     serverBin,
		bootstrapAddr: bootstrapAddr,
		processes:     make(map[string]*managedServer),
	}
}
func (m *clusterManager) ensureBootstrap() error {
	m.mu.Lock()
	if _, ok := m.processes[m.bootstrapAddr]; ok {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	return m.startServer(m.bootstrapAddr, true)
}

func (m *clusterManager) removeLast() (string, error) {
	m.mu.Lock()
	if len(m.processes) <= 1 {
		m.mu.Unlock()
		return "", errors.New("bootstrap server cannot be removed")
	}
	var proc *managedServer
	for addr, s := range m.processes {
		if addr != m.bootstrapAddr {
			proc = s
			break
		}
	}
	delete(m.processes, proc.addr)
	m.mu.Unlock()

	if proc == nil || proc.cmd == nil || proc.cmd.Process == nil {
		return "", fmt.Errorf("server process for %s was not found", proc.addr)
	}

	if err := proc.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = proc.cmd.Process.Kill()
	}
	done := make(chan error, 1)
	go func() {
		done <- proc.cmd.Wait()
	}()

	select {
	case <-time.After(5 * time.Second):
		_ = proc.cmd.Process.Kill()
		<-done
	case <-done:

	}

	return proc.addr, nil
}

func (m *clusterManager) reset() error {
	m.mu.Lock()
	addresses := make([]string, 0, len(m.processes))
	for a := range m.processes {
		addresses = append(addresses, a)
	}
	m.mu.Unlock()
	for _, addr := range addresses {
		if addr == m.bootstrapAddr {
			continue
		}
		if _, err := m.stopSpecific(addr); err != nil {
			return err
		}
	}
	return m.ensureBootstrap()
}

func (m *clusterManager) stopSpecific(addr string) (bool, error) {
	m.mu.Lock()
	proc, ok := m.processes[addr]
	if ok {
		delete(m.processes, addr)
	}
	m.mu.Unlock()
	if !ok || proc == nil || proc.cmd == nil || proc.cmd.Process == nil {
		return false, nil
	}

	if err := proc.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = proc.cmd.Process.Kill()
	}
	done := make(chan error, 1)
	go func() {
		done <- proc.cmd.Wait()
	}()
	select {
	case <-time.After(5 * time.Second):
		_ = proc.cmd.Process.Kill()
		<-done
	case <-done:
	}
	return true, nil
}

func (m *clusterManager) getServers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := make([]string, 0, len(m.processes))
	for addr := range m.processes {
		s = append(s, addr)
	}
	sort.Slice(s, func(i, j int) bool {
		return portFromAddress(s[i]) < portFromAddress(s[j])
	})
	return s
}
func (m *clusterManager) startNext() (string, error) {
	sPort := strings.Split(m.bootstrapAddr, ":")[1]
	port, _ := strconv.Atoi(sPort)
	newPort := fmt.Sprintf(":%d", port+len(m.processes))
	if err := m.startServer(newPort, false); err != nil {
		return "", err
	}

	return newPort, nil
}
func (m *clusterManager) startServer(listenAddr string, isBootstrap bool) error {
	cmd, err := m.commandForServer(listenAddr, isBootstrap)
	if err != nil {
		return err
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	m.mu.Lock()
	m.processes[listenAddr] = &managedServer{addr: listenAddr, cmd: cmd}

	m.mu.Unlock()

	return nil
}

func (m *clusterManager) commandForServer(listenAddr string, isBootstrap bool) (*exec.Cmd, error) {
	args := []string{"-address", listenAddr}
	if !isBootstrap {
		args = append(args, "-connectWith", m.bootstrapAddr)
	}
	if info, err := os.Stat(m.serverBin); err == nil && !info.IsDir() {
		return exec.Command(m.serverBin, args...), nil
	}
	return exec.Command("./bin/cache-server", args...), nil

}

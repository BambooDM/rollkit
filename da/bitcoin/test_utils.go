package bitcoin

import (
	"fmt"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

const (
	MockBtcHost   = "localhost:18443"
	MockBtcUser   = "regtest"
	MockBtcPass   = "regtest"
	MockBlockTime = 3 * time.Second
)

// managing bitcoin regtest process
type RegBitcoinProcess struct {
	Cmd *exec.Cmd
}

func (reg *RegBitcoinProcess) RunBitcoinProcess(t *testing.T) {
	// setup bitcoin node running in regtest mode
	rpcUser := fmt.Sprintf("-rpcuser=%s", MockBtcUser)
	rpcPass := fmt.Sprintf("-rpcpassword=%s", MockBtcPass)
	reg.Cmd = exec.Command("bitcoind", "-regtest", "-fallbackfee=0.0000001", "-listen", "-server", "-rpcport=18443", rpcUser, rpcPass)
	// set child process group id to the same as parent process id, so that KILL signal can kill both parent and child processes
	reg.Cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	err := reg.Cmd.Start()
	if err != nil {
		panic(err)
	}

	// wait for bitcoin node to start
	time.Sleep(5 * time.Second)

	// generate blocks
	go func() {
		for {
			t.Log("generating bitcoin block")
			err := exec.Command("bitcoin-cli", "-regtest", "-generate").Run()
			if err != nil {
				panic(err)
			}
			time.Sleep(MockBlockTime)
		}
	}()
}

func (reg *RegBitcoinProcess) StopBitcoinProcess(t *testing.T) {
	t.Logf("cmd = %v, process = %v\n", reg.Cmd, reg.Cmd.Process)

	if reg.Cmd != nil && reg.Cmd.Process != nil {
		t.Log("killing bitcoin regtest process")

		err := reg.Cmd.Process.Kill()
		if err != nil {
			panic(err)
		}
	}
}

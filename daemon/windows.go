package main

import (
	"fmt"
	"indexer"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
)

type fsIndexService struct{}

func (m *fsIndexService) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (ssec bool, errno uint32) {
	const cmdsAccepted = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending}

	ticker := time.NewTicker(168 * time.Hour)
	defer ticker.Stop()

	changes <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}

serviceLoop:
	for {
		select {
		case <-ticker.C:
			indexer.ScanLoop()
		case c := <-r:
			switch c.Cmd {
			case svc.Interrogate:
				changes <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				changes <- svc.Status{State: svc.StopPending}
				break serviceLoop
			default:
				continue
			}
		}
	}

	changes <- svc.Status{State: svc.Stopped}
	return
}

func runService() error {
	handle := windows.CurrentProcess()
	windows.SetPriorityClass(handle, windows.IDLE_PRIORITY_CLASS)
	return svc.Run(indexer.ServiceName, &fsIndexService{})
}

func main() {
	isService, err := svc.IsWindowsService()
	if err != nil {
		return
	}

	if isService {
		runService()
		return
	}

	fmt.Printf("Detected launch as a standalone executable; %s is only intended to run as a Windows service", indexer.ServiceName)
	fmt.Println("Press Ctrl + C to exit, else this window will close in ten seconds.")
	time.Sleep(10 * time.Second)
}

package main

import (
	"context"
	"fmt"
	"indexer"
	"log/slog"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
)

type fsIndexService struct{}

func (m *fsIndexService) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (ssec bool, errno uint32) {
	const cmdsAccepted = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending}
	ctx, cancel := context.WithCancel(context.Background())

	err := indexer.EnsurePersistentFiles()
	if err != nil {
		cancel()
		return false, 1
	}

	logger, file, err := indexer.InitLogging()
	if err != nil {
		cancel()
		return false, 1
	}

	defer file.Close()

	go runWorker(ctx, logger)

	changes <- svc.Status{State: svc.Running, Accepts: cmdsAccepted}

changeLoop:
	for req := range r {
		switch req.Cmd {
		case svc.Interrogate:
			changes <- req.CurrentStatus
		case svc.Stop, svc.Shutdown:
			changes <- svc.Status{State: svc.StopPending}
			cancel()
			break changeLoop
		default:
			logger.Error(fmt.Sprintf("Recieved unexpected control request: %d", req.Cmd))
			continue
		}
	}

	cancel()
	changes <- svc.Status{State: svc.Stopped}
	return
}

func runWorker(ctx context.Context, logger *slog.Logger) error {
	//Scan once every 7 days, TODO: make this configurable
	ticker := time.NewTicker(168 * time.Hour)
	defer ticker.Stop()

	logger.Info(fmt.Sprintf("%s started successfully.", indexer.ServiceName))

	for {
		select {
		case <-ticker.C:
			indexer.Scan(ctx, logger)
		case <-ctx.Done():
			fmt.Println("Worker stopping gracefully.")
			return nil
		}
	}
}

func main() {
	isService, err := svc.IsWindowsService()
	if err != nil {
		return
	}

	if isService {
		handle := windows.CurrentProcess()
		windows.SetPriorityClass(handle, windows.IDLE_PRIORITY_CLASS)
		svc.Run(indexer.ServiceName, &fsIndexService{})
		return
	}

	fmt.Printf("Detected launch as a standalone executable; %s is only intended to run as a Windows service\n", indexer.ServiceName)
	fmt.Println("Press Ctrl + C to exit, else this window will close in ten seconds.")
	time.Sleep(10 * time.Second)
}

package main

import (
	"context"
	"embed"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/logflow/logflow/pkg/config"
	"github.com/logflow/logflow/pkg/server"
)

//go:embed web/*
var webFS embed.FS

var (
	servePort    int
	serveHost    string
	serveOpen    bool
	serveNoBrowser bool
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the web UI server",
	Long: `Start a local HTTP server with the LogFlow web interface.

The server provides:
  - Web-based file upload and conversion
  - Real-time progress streaming
  - REST API for programmatic access
  - Job history and management

Examples:
  logflow serve                    # Start on default port (8080)
  logflow serve --port 3000        # Start on custom port
  logflow serve --host 0.0.0.0     # Listen on all interfaces
  logflow serve --no-browser       # Don't open browser automatically`,
	RunE: runServe,
}

func init() {
	cfg := config.Global().Get()

	serveCmd.Flags().IntVarP(&servePort, "port", "p", cfg.Server.Port, "Port to listen on")
	serveCmd.Flags().StringVar(&serveHost, "host", cfg.Server.Host, "Host to bind to")
	serveCmd.Flags().BoolVar(&serveOpen, "open", true, "Open browser automatically")
	serveCmd.Flags().BoolVar(&serveNoBrowser, "no-browser", false, "Don't open browser")

	rootCmd.AddCommand(serveCmd)
}

func runServe(cmd *cobra.Command, args []string) error {
	// Create server
	srv, err := server.NewServer(webFS)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	defer srv.Close()

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", serveHost, servePort)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      srv,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // Disable for SSE
		IdleTimeout:  120 * time.Second,
	}

	// Start listening
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// Print startup message
	url := fmt.Sprintf("http://%s:%d", serveHost, servePort)
	if serveHost == "0.0.0.0" || serveHost == "" {
		url = fmt.Sprintf("http://localhost:%d", servePort)
	}

	fmt.Println()
	fmt.Println("  ╭─────────────────────────────────────╮")
	fmt.Println("  │         LOGFLOW SERVER              │")
	fmt.Println("  ├─────────────────────────────────────┤")
	fmt.Printf("  │  Local:   %-25s │\n", url)
	if serveHost == "0.0.0.0" {
		if ip := getOutboundIP(); ip != "" {
			fmt.Printf("  │  Network: http://%-17s │\n", fmt.Sprintf("%s:%d", ip, servePort))
		}
	}
	fmt.Println("  │                                     │")
	fmt.Println("  │  Press Ctrl+C to stop               │")
	fmt.Println("  ╰─────────────────────────────────────╯")
	fmt.Println()

	// Open browser
	if serveOpen && !serveNoBrowser {
		go func() {
			time.Sleep(500 * time.Millisecond)
			openBrowser(url)
		}()
	}

	// Handle shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
		httpServer.Shutdown(context.Background())
	}()

	// Start server
	errChan := make(chan error, 1)
	go func() {
		if err := httpServer.Serve(listener); err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		return nil
	}
}

// openBrowser opens URL in the default browser.
func openBrowser(url string) {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		return
	}

	cmd.Start()
}

// getOutboundIP gets the preferred outbound IP.
func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

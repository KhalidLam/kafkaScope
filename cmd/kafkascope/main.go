package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/KhalidLam/kafkaScope/internal/kafka"
	"github.com/KhalidLam/kafkaScope/internal/ui"
)

const version = "0.1.0"

func main() {
	brokers := flag.String("brokers",
		envOr("KAFKA_BROKERS", "localhost:9092"),
		"Comma-separated list of broker addresses")
	refresh := flag.Duration("refresh", 3*time.Second, "Polling interval (e.g. 2s, 5s)")
	ver := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *ver {
		fmt.Printf("kafkascope %s\n", version)
		os.Exit(0)
	}

	brokerList := strings.Split(*brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	client, err := kafka.NewClient(brokerList)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer func() { _ = client.Close() }()

	// refreshCh is shared between the AppModel (send) and the Poller (receive).
	refreshCh := make(chan struct{}, 1)

	model := ui.NewApp(*brokers, *refresh, refreshCh)
	p := tea.NewProgram(model, tea.WithAltScreen(), tea.WithMouseCellMotion())

	poller := kafka.NewPoller(client, *refresh, p, refreshCh)
	go poller.Start()

	if _, err := p.Run(); err != nil {
		log.Fatalf("ui: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

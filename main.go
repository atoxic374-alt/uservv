package main

import (
	"fmt"
	"os"
	"users/database"
	"users/globals"
	"users/logger"
	"users/proxy"
	"users/web"
)

func main() {
	logger.Title("Discord Checker", "cyan")

	if err := globals.LoadConfig(); err != nil {
		logger.Warn(fmt.Sprintf("Config load warning: %v  using defaults", err))
	}

	if err := globals.LoadVanityConfig(); err != nil {
		logger.Warn(fmt.Sprintf("Vanity config load warning: %v  using defaults", err))
	}

	if err := database.Init("data/checker.db"); err != nil {
		logger.Error(fmt.Sprintf("Database init failed: %v", err))
		os.Exit(1)
	}
	logger.Success("Database initialized")

	if err := globals.LoadBlackList(); err != nil {
		logger.Warn(fmt.Sprintf("Blacklist: %v", err))
	} else {
		logger.Info(fmt.Sprintf("Blacklist: %d entries", len(globals.BlackList)))
	}

	if err := proxy.Default.Reload(); err != nil {
		logger.Warn(fmt.Sprintf("Proxy manager: %v", err))
	} else {
		logger.Info(fmt.Sprintf("Proxy manager: %d healthy proxies", proxy.Default.Count()))
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "5000"
	}
	addr := "0.0.0.0:" + port
	logger.Success(fmt.Sprintf("Dashboard  http://%s", addr))

	if err := web.StartServer(addr); err != nil {
		logger.Error(fmt.Sprintf("Server error: %v", err))
		os.Exit(1)
	}
}

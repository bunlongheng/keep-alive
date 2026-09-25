BIN := keep-alive

build:
	go build -trimpath -ldflags="-s -w" -o $(BIN) .

run: build
	./$(BIN) -once

# Live terminal dashboard read from stats.json + keep-alive.log (works over ssh).
tui: build
	./$(BIN) -tui

# One-shot all-time report to stdout.
report: build
	./$(BIN) -report

# Cross-compile for a Raspberry Pi 5 (64-bit Raspberry Pi OS) from any machine.
pi:
	GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o $(BIN)-linux-arm64 .

# On the Pi: build, install a systemd service that runs from this directory, start it.
install: build
	sed "s|__DIR__|$(CURDIR)|g; s|__USER__|$(USER)|g" keep-alive.service | sudo tee /etc/systemd/system/keep-alive.service > /dev/null
	sudo systemctl daemon-reload
	sudo systemctl enable --now keep-alive
	systemctl status keep-alive --no-pager | head -5

uninstall:
	sudo systemctl disable --now keep-alive
	sudo rm -f /etc/systemd/system/keep-alive.service
	sudo systemctl daemon-reload

clean:
	rm -f $(BIN) $(BIN)-linux-arm64 keep-alive.log status.json stats.json

.PHONY: build run tui report pi install uninstall clean

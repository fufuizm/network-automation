#!/usr/bin/env python3
"""
Network Device Configuration Backup Script

Connects to network devices via SSH, retrieves running configurations,
and saves timestamped backups to the local filesystem.

Supports: Cisco IOS/IOS-XE, Arista EOS, Juniper JunOS
"""

import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
from netmiko import ConnectHandler, NetmikoTimeoutException, NetmikoAuthenticationException
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

# --------------- Configuration ---------------
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "./backups"))
INVENTORY_FILE = Path(os.getenv("INVENTORY_FILE", "./inventory/hosts.yaml"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

console = Console()
logger = logging.getLogger(__name__)

# --------------- Command Map ---------------
SHOW_COMMANDS = {
    "cisco_ios": "show running-config",
    "cisco_xe": "show running-config",
    "cisco_nxos": "show running-config",
    "arista_eos": "show running-config",
    "juniper_junos": "show configuration | display set",
}


def load_inventory(inventory_path: Path) -> list[dict]:
    """Load device inventory from YAML file."""
    with open(inventory_path) as f:
        inventory = yaml.safe_load(f)

    devices = []
    for group_name, group in inventory.get("groups", {}).items():
        defaults = group.get("defaults", {})
        for device in group.get("devices", []):
            merged = {**defaults, **device, "group": group_name}
            devices.append(merged)
    return devices


def backup_device(device: dict) -> dict:
    """Backup a single device configuration."""
    hostname = device["hostname"]
    device_type = device["device_type"]
    result = {"hostname": hostname, "status": "unknown", "file": None, "error": None}

    try:
        connection = ConnectHandler(
            device_type=device_type,
            host=device["host"],
            username=device.get("username", os.getenv("NET_USER")),
            password=device.get("password", os.getenv("NET_PASS")),
            port=device.get("port", 22),
            timeout=device.get("timeout", 30),
        )

        command = SHOW_COMMANDS.get(device_type, "show running-config")
        config = connection.send_command(command)
        connection.disconnect()

        # Save backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / device.get("group", "ungrouped") / hostname
        backup_path.mkdir(parents=True, exist_ok=True)

        filename = backup_path / f"{hostname}_{timestamp}.cfg"
        filename.write_text(config)

        # Also maintain a "latest" symlink
        latest = backup_path / f"{hostname}_latest.cfg"
        if latest.is_symlink():
            latest.unlink()
        latest.symlink_to(filename.name)

        result["status"] = "success"
        result["file"] = str(filename)
        logger.info(f"Backup successful: {hostname} -> {filename}")

    except NetmikoTimeoutException:
        result["status"] = "timeout"
        result["error"] = f"Connection timed out to {hostname}"
        logger.error(result["error"])

    except NetmikoAuthenticationException:
        result["status"] = "auth_failed"
        result["error"] = f"Authentication failed for {hostname}"
        logger.error(result["error"])

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        logger.error(f"Error backing up {hostname}: {e}")

    return result


def run_backups(devices: list[dict]) -> list[dict]:
    """Run backups concurrently across all devices."""
    results = []

    with Progress() as progress:
        task = progress.add_task("[cyan]Backing up devices...", total=len(devices))

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(backup_device, d): d for d in devices}

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                progress.advance(task)

    return results


def print_summary(results: list[dict]):
    """Print a summary table of backup results."""
    table = Table(title="Backup Results")
    table.add_column("Hostname", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("File / Error")

    for r in sorted(results, key=lambda x: x["hostname"]):
        status_style = "green" if r["status"] == "success" else "red"
        detail = r["file"] if r["status"] == "success" else (r["error"] or "")
        table.add_row(r["hostname"], f"[{status_style}]{r['status']}[/]", detail)

    console.print(table)

    success = sum(1 for r in results if r["status"] == "success")
    console.print(f"\n[bold]{success}/{len(results)} devices backed up successfully[/bold]")


def main():
    parser = argparse.ArgumentParser(description="Network device configuration backup")
    parser.add_argument("-i", "--inventory", default=str(INVENTORY_FILE), help="Inventory file path")
    parser.add_argument("-o", "--output", default=str(BACKUP_DIR), help="Backup output directory")
    parser.add_argument("-w", "--workers", type=int, default=MAX_WORKERS, help="Max concurrent connections")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    global BACKUP_DIR, MAX_WORKERS
    BACKUP_DIR = Path(args.output)
    MAX_WORKERS = args.workers

    console.print("[bold blue]Network Configuration Backup[/bold blue]\n")

    devices = load_inventory(Path(args.inventory))
    console.print(f"Loaded [cyan]{len(devices)}[/cyan] devices from inventory\n")

    results = run_backups(devices)
    print_summary(results)

    failed = [r for r in results if r["status"] != "success"]
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

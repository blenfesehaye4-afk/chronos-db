"""Rich CLI interface for ChronosDB server, benchmarks, and interactive REPL."""

import argparse
import asyncio
import time
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .engine import ChronosEngine
from .server import ChronosClient, ChronosServer

console = Console()


def run_benchmark(data_dir: Path, total_ops: int = 5000):
    """Run sequential write and read throughput benchmarks."""
    console.print(Panel(f"[bold cyan]ChronosDB Benchmark Suite[/bold cyan]\nOperations: [yellow]{total_ops:,}[/yellow] | Directory: [dim]{data_dir}[/dim]"))

    engine = ChronosEngine(data_dir=data_dir, memtable_max_bytes=256 * 1024)

    # 1. Write Benchmark
    start_w = time.perf_counter()
    for i in range(total_ops):
        engine.put(f"user:{i:06d}", f'{{"id": {i}, "active": true, "score": {i * 1.5}}}')
    dur_w = time.perf_counter() - start_w
    w_ops = total_ops / dur_w

    # 2. Read Benchmark
    start_r = time.perf_counter()
    hits = 0
    for i in range(total_ops):
        val = engine.get(f"user:{i:06d}")
        if val is not None:
            hits += 1
    dur_r = time.perf_counter() - start_r
    r_ops = total_ops / dur_r

    # 3. Compaction
    start_c = time.perf_counter()
    engine.compact()
    dur_c = time.perf_counter() - start_c

    stats = engine.stats()
    engine.close()

    table = Table(title="⚡ Benchmark Performance Results")
    table.add_column("Phase", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Throughput", justify="right", style="green")

    table.add_row("Sequential Writes (PUT)", f"{total_ops:,}", f"{dur_w:.3f} s", f"{w_ops:,.0f} ops/sec")
    table.add_row("Key Queries (GET)", f"{hits:,}", f"{dur_r:.3f} s", f"{r_ops:,.0f} ops/sec")
    table.add_row("SSTable Compaction", f"{stats['sstable_count']} SSTables", f"{dur_c:.3f} s", "Complete")

    console.print(table)
    console.print(f"[bold green]✓[/bold green] Active SSTables: [yellow]{stats['sstable_count']}[/yellow] | Disk Usage: [yellow]{stats['total_sstable_disk_bytes']:,}[/yellow] bytes")


async def run_server(host: str, port: int, data_dir: Path):
    engine = ChronosEngine(data_dir=data_dir)
    server = ChronosServer(engine, host=host, port=port)
    await server.start()
    console.print(Panel(f"[bold green]ChronosDB Server Running[/bold green]\nHost: [cyan]{host}:{port}[/cyan]\nStorage: [dim]{data_dir.resolve()}[/dim]"))
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        console.print("\n[yellow]Stopping server...[/yellow]")
        await server.stop()
        engine.close()


async def run_repl(host: str, port: int):
    client = ChronosClient(host=host, port=port)
    try:
        await client.connect()
    except Exception as e:
        console.print(f"[bold red]Failed to connect to ChronosDB at {host}:{port}:[/bold red] {e}")
        return

    console.print(f"[bold cyan]Connected to ChronosDB on {host}:{port}[/bold cyan] (type 'exit' to quit)")
    while True:
        try:
            line = input(f"chronos:{port}> ").strip()
            if not line:
                continue
            if line.lower() in ("exit", "quit"):
                break

            parts = line.split(" ", 2)
            cmd = parts[0].upper()
            if cmd == "PING":
                res = await client.ping()
                console.print(f"[green]{res}[/green]")
            elif cmd == "SET" and len(parts) >= 3:
                ok = await client.set(parts[1], parts[2])
                console.print("[green]OK[/green]" if ok else "[red]ERR[/red]")
            elif cmd == "GET" and len(parts) >= 2:
                val = await client.get(parts[1])
                console.print(f"[yellow]\"{val}\"[/yellow]" if val is not None else "[dim](nil)[/dim]")
            elif cmd == "DEL" and len(parts) >= 2:
                deleted = await client.delete(parts[1])
                console.print(f"(integer) {1 if deleted else 0}")
            elif cmd == "SCAN":
                prefix = parts[1] if len(parts) > 1 else "*"
                items = await client.scan(prefix=prefix)
                for it in items:
                    console.print(f"• [cyan]{it['key']}[/cyan] -> [yellow]{it['value']}[/yellow]")
            elif cmd == "STATS":
                st = await client.stats()
                console.print(st)
            elif cmd == "COMPACT":
                ok = await client.compact()
                console.print("[green]OK[/green]" if ok else "[red]ERR[/red]")
            else:
                res = await client._send_cmd(line)
                console.print(res)
        except (KeyboardInterrupt, EOFError):
            break
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")

    await client.close()


def print_stats(data_dir: Path):
    engine = ChronosEngine(data_dir=data_dir)
    stats = engine.stats()
    engine.close()

    table = Table(title="🗄️ ChronosDB Storage Engine Status")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="yellow")

    for k, v in stats.items():
        table.add_row(k.replace("_", " ").title(), str(v))

    console.print(table)


def main():
    parser = argparse.ArgumentParser(
        prog="chronos-db",
        description="🗄️ ChronosDB: High-performance Log-Structured Merge (LSM) Tree Storage Engine CLI.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # Server
    srv = subparsers.add_parser("server", help="Start ChronosDB network server")
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, default=6380)
    srv.add_argument("--dir", default="./data")

    # Bench
    bench = subparsers.add_parser("bench", help="Run write & read throughput benchmark")
    bench.add_argument("--ops", type=int, default=5000)
    bench.add_argument("--dir", default="./bench_data")

    # REPL
    repl = subparsers.add_parser("repl", help="Connect interactive terminal client")
    repl.add_argument("--host", default="127.0.0.1")
    repl.add_argument("--port", type=int, default=6380)

    # Stats
    st = subparsers.add_parser("stats", help="Inspect storage files and statistics")
    st.add_argument("--dir", default="./data")

    args = parser.parse_args()

    if args.command == "server":
        asyncio.run(run_server(args.host, args.port, Path(args.dir)))
    elif args.command == "bench":
        run_benchmark(Path(args.dir), total_ops=args.ops)
    elif args.command == "repl":
        asyncio.run(run_repl(args.host, args.port))
    elif args.command == "stats" or args.command is None:
        print_stats(Path(getattr(args, "dir", "./data")))


if __name__ == "__main__":
    main()

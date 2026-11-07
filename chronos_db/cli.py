"""Rich CLI interface for ChronosDB server, benchmarks, and interactive REPL."""

import argparse
import asyncio
import tempfile
import time
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .engine import ChronosEngine
from .server import ChronosClient, ChronosServer

console = Console()


def run_benchmark(data_dir: Path, total_ops: int = 5000):
    """Run sequential write and read throughput benchmarks with cache monitoring."""
    console.print(Panel(f"[bold cyan]ChronosDB Benchmark Suite[/bold cyan]\nOperations: [yellow]{total_ops:,}[/yellow] | Directory: [dim]{data_dir}[/dim]"))

    engine = ChronosEngine(data_dir=data_dir, memtable_max_bytes=256 * 1024)

    # 1. Write Benchmark
    start_w = time.perf_counter()
    for i in range(total_ops):
        engine.put(f"user:{i:06d}", f'{{"id": {i}, "active": true, "score": {i * 1.5}}}')
    dur_w = time.perf_counter() - start_w
    w_ops = total_ops / dur_w

    # 2. Read Benchmark (MemTable & Cache)
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
    cache_info = stats.get("cache", {})
    console.print(f"[bold green]✓[/bold green] Cache Hit Ratio: [cyan]{cache_info.get('hit_ratio', 0.0) * 100:.1f}%[/cyan] | Disk Usage: [yellow]{stats['total_sstable_disk_bytes']:,}[/yellow] bytes")


def run_demo():
    """Run an interactive feature showcase of ChronosDB v0.3.0 capabilities."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        console.print(Panel("[bold green]ChronosDB v0.3.0 Feature Showcase[/bold green]\nDemonstrating TTL expiration, LRU Block Cache, Redis Hashes/Lists/Sets, and Point-in-time Snapshots."))

        engine = ChronosEngine(data_dir=tmp_path)

        # 1. TTL Demo
        console.print("\n[bold cyan]1. Time-To-Live (TTL) & Auto-Expiration:[/bold cyan]")
        engine.put("session:auth_token_temp", "secret_jwt_xyz", ttl=1.0)
        console.print(f"  • Inserted key with 1.0s TTL. Initial TTL: [yellow]{engine.ttl_seconds('session:auth_token_temp'):.2f}s[/yellow]")
        console.print(f"  • Immediate GET: [green]{engine.get('session:auth_token_temp').decode()}[/green]")
        console.print("  • Waiting 1.1s for expiration...")
        time.sleep(1.1)
        expired_val = engine.get("session:auth_token_temp")
        console.print(f"  • Post-expiration GET: [dim]{expired_val}[/dim] (Expired automatically)")

        # 2. Redis Data Structures
        console.print("\n[bold cyan]2. Redis Data Structures (Hashes, Lists, Sets):[/bold cyan]")
        # Hashes
        engine.structures.hset("user:100", "username", "alice")
        engine.structures.hset("user:100", "role", "admin")
        hdict = engine.structures.hgetall("user:100")
        console.print(f"  • Hash (HGETALL user:100): [yellow]{hdict}[/yellow]")

        # Lists
        engine.structures.rpush("queue:jobs", "job_1", "job_2", "job_3")
        jobs = engine.structures.lrange("queue:jobs", 0, -1)
        console.print(f"  • List (LRANGE queue:jobs 0 -1): [cyan]{jobs}[/cyan]")

        # Sets
        engine.structures.sadd("tags:python", "asyncio", "lsm", "database", "fastapi")
        tags = engine.structures.smembers("tags:python")
        console.print(f"  • Set (SMEMBERS tags:python): [magenta]{tags}[/magenta]")

        # 3. Snapshot & Restore
        console.print("\n[bold cyan]3. Consistent Hot Backup & Restore:[/bold cyan]")
        snapshot_dir = tmp_path / "backup_snapshot"
        manifest = engine.export_snapshot(snapshot_dir)
        console.print(f"  • Snapshot exported to [dim]{snapshot_dir.name}[/dim] with [green]{manifest['sstable_count']}[/green] SSTables and manifest verification.")

        # Modify database
        engine.put("temp_key", "will_be_wiped")
        engine.delete("user:100")

        # Restore
        engine.restore_snapshot(snapshot_dir)
        restored_hash = engine.structures.hgetall("user:100")
        console.print(f"  • Database restored from snapshot. Restored hash user:100: [green]{restored_hash}[/green]")

        stats = engine.stats()
        console.print(f"\n[bold green]✓ Showcase Complete.[/bold green] Cache: [cyan]{stats['cache']}[/cyan]")
        engine.close()


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

            parts = line.split()
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
            elif cmd == "STATS" or cmd == "INFO":
                st = await client.stats()
                console.print(st)
            elif cmd == "COMPACT":
                ok = await client.compact()
                console.print("[green]OK[/green]" if ok else "[red]ERR[/red]")
            else:
                res = await client._send_cmd(*parts)
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

    # Demo
    subparsers.add_parser("demo", help="Run showcase of v0.3.0 features (TTL, Cache, Structures, Backup)")

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
    elif args.command == "demo":
        run_demo()
    elif args.command == "repl":
        asyncio.run(run_repl(args.host, args.port))
    elif args.command == "stats" or args.command is None:
        print_stats(Path(getattr(args, "dir", "./data")))


if __name__ == "__main__":
    main()

"""
Phase 4: Orchestrate
Runs extract -> transform -> load as one pipeline, logging start/end time
and status for each stage. This is the single command to run in a demo.
"""
import subprocess
import sys
import time

STAGES = [
    ("Extract", "src/extract.py"),
    ("Transform", "src/transform.py"),
    ("Load", "src/load.py"),
]


def run_stage(name: str, script_path: str) -> bool:
    print(f"\n{'=' * 60}")
    print(f"STAGE: {name}")
    print(f"{'=' * 60}")

    start = time.time()
    result = subprocess.run([sys.executable, script_path])
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n[FAILED] {name} exited with code {result.returncode} after {elapsed:.1f}s")
        return False

    print(f"\n[OK] {name} completed in {elapsed:.1f}s")
    return True


def main():
    pipeline_start = time.time()
    print(f"Starting TickerFlow pipeline ({len(STAGES)} stages)...")

    for name, script_path in STAGES:
        success = run_stage(name, script_path)
        if not success:
            print(f"\nPipeline stopped early — {name} stage failed.")
            sys.exit(1)

    total_elapsed = time.time() - pipeline_start
    print(f"\n{'=' * 60}")
    print(f"Pipeline completed successfully in {total_elapsed:.1f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
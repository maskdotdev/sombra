import sys
import subprocess
import shutil
from pathlib import Path


def find_sombra_binary():
    binary_name = "sombra.exe" if sys.platform == "win32" else "sombra"
    
    binary_path = shutil.which(binary_name)
    if binary_path:
        return binary_path
    
    install_dir = Path.home() / ".cargo" / "bin" / binary_name
    if install_dir.exists():
        return str(install_dir)
    
    return None


def main():
    binary_path = find_sombra_binary()
    
    if not binary_path:
        print("Error: Sombra CLI binary not found.", file=sys.stderr)
        print("Please install with: cargo install sombra", file=sys.stderr)
        print("Or build from source: cargo build --release", file=sys.stderr)
        sys.exit(1)
    
    args = sys.argv[1:]
    
    try:
        result = subprocess.run([binary_path] + args, check=False)
        sys.exit(result.returncode)
    except FileNotFoundError:
        print(f"Error: Could not execute {binary_path}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()

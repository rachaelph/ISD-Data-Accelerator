#!/usr/bin/env python3
"""Preflight check for agent workflows: verify az CLI is installed and signed in.

This accelerator targets **Azure Databricks only** — authentication uses the
Azure CLI (``az login``) to obtain Azure AD tokens for the Databricks SDK.

Run this at the start of a workflow session to confirm Azure authentication is
ready. Replaces the PowerShell-based auth preflight for agent-driven workflows.

Usage:
    python automation_scripts/agents/preflight.py           # check only (exit 0 if ready)
    python automation_scripts/agents/preflight.py --pretty  # indented JSON
    python automation_scripts/agents/preflight.py --install # offer to install missing tools (prompts before each)
    python automation_scripts/agents/preflight.py --install --yes  # install without prompting
    python automation_scripts/agents/preflight.py --python-only    # check only the Python interpreter version (used by /fdp-03 authoring flows that do not need Azure tooling)

--install uses the best available package manager for the current OS:
    Windows          -> winget
    macOS            -> Homebrew (brew)
    Debian/Ubuntu    -> apt-get (requires sudo)
Other Linux distros fall back to printed install hints.
"""
from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
REQUIREMENTS_FILE = REPO_ROOT / "automation_scripts" / "requirements-agent.txt"
# Minimum Python interpreter required by agent automation scripts.
# Several scripts (commit_pipeline.py, run_pipeline.py) use PEP 604 union syntax
# and builtin generic subscripting that requires Python 3.10+.
MIN_PYTHON_VERSION: tuple[int, int] = (3, 10)
# Map PyPI distribution name -> importable module name (used to probe presence).
# Azure Databricks: Azure AD auth via azure-identity + Databricks SDK for all
# workspace/SQL operations. No pyodbc/ODBC required.
REQUIRED_PACKAGES: dict[str, str] = {
    "azure-identity": "azure.identity",
    "databricks-sdk": "databricks.sdk",
}


INSTALL_HINT_WINDOWS = (
    "Install the Azure CLI via 'winget install --id Microsoft.AzureCLI --source winget' "
    "(or re-run this preflight with --install), then run 'az login' to sign in."
)
INSTALL_HINT_MACOS = (
    "Install the Azure CLI via 'brew install azure-cli' (or 'brew update && brew install azure-cli'), "
    "then run 'az login' to sign in."
)
INSTALL_HINT_LINUX = (
    "Install the Azure CLI using the Microsoft docs for your distro: "
    "https://learn.microsoft.com/cli/azure/install-azure-cli-linux . "
    "Then run 'az login' to sign in."
)
LOGIN_HINT = "Run 'az login' in the same terminal session, then re-run this preflight."
PWSH_HINT_WINDOWS = (
    "Install PowerShell 7 with 'winget install --id Microsoft.PowerShell --source winget' "
    "(or re-run this preflight with --install), then reload the VS Code window. "
    "Required for Copilot Chat to stream long-running command output."
)
PWSH_HINT_MACOS = (
    "Install PowerShell 7 with 'brew install --cask powershell', then reload the VS Code window. "
    "Required for Copilot Chat to stream long-running command output."
)
PWSH_HINT_LINUX = (
    "Install PowerShell 7 following the Microsoft docs for your distro: "
    "https://learn.microsoft.com/powershell/scripting/install/installing-powershell-on-linux . "
    "Then reload the VS Code window. Required for Copilot Chat to stream long-running command output."
)
WINGET_MISSING_HINT = (
    "winget (App Installer) is required for --install on Windows. Install 'App Installer' from the Microsoft Store, "
    "or install the tools manually per the hints above."
)
PYTHON_PACKAGES_HINT = (
    f"Install Python packages with 'pip install -r {REQUIREMENTS_FILE.relative_to(REPO_ROOT).as_posix()}', "
    "or re-run this preflight with --install to auto-install."
)
PYTHON_VERSION_HINT = (
    f"Agent automation scripts require Python {MIN_PYTHON_VERSION[0]}.{MIN_PYTHON_VERSION[1]}+. "
    "Install a supported interpreter from https://www.python.org/downloads/ "
    "(Windows: 'winget install --id Python.Python.3.12'; macOS: 'brew install python@3.12'), "
    "then re-run preflight using that interpreter."
)


def _platform_hint(windows: str, macos: str, linux: str) -> str:
    if sys.platform == "win32":
        return windows
    if sys.platform == "darwin":
        return macos
    return linux


def _install_hint() -> str:
    return _platform_hint(INSTALL_HINT_WINDOWS, INSTALL_HINT_MACOS, INSTALL_HINT_LINUX)


def _pwsh_hint() -> str:
    return _platform_hint(PWSH_HINT_WINDOWS, PWSH_HINT_MACOS, PWSH_HINT_LINUX)


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=False)


def _resolve_az() -> str | None:
    """Return a full path to the az executable.

    On Windows the Azure CLI is distributed as ``az.cmd``. Python's subprocess
    on Windows calls ``CreateProcess`` directly and does not honor ``PATHEXT``,
    so invoking ``["az", ...]`` without ``shell=True`` raises
    ``FileNotFoundError`` even when ``az`` is on PATH. Resolving the full path
    via ``shutil.which`` (which does honor PATHEXT) avoids the issue without
    needing ``shell=True``.
    """
    return shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")


def check_az_installed() -> dict[str, Any]:
    az_path = _resolve_az()
    if not az_path:
        return {"status": "missing", "hint": _install_hint()}
    version_result = _run([az_path, "--version"])
    if version_result.returncode != 0:
        return {
            "status": "error",
            "hint": _install_hint(),
            "details": (version_result.stderr or version_result.stdout).strip(),
        }
    first_line = next((line for line in version_result.stdout.splitlines() if line.strip()), "").strip()
    return {"status": "ok", "path": az_path, "version": first_line}


def check_az_account(az_path: str) -> dict[str, Any]:
    account_result = _run([az_path, "account", "show", "-o", "json"])
    if account_result.returncode != 0:
        return {
            "status": "signed_out",
            "hint": LOGIN_HINT,
            "details": (account_result.stderr or account_result.stdout).strip(),
        }
    try:
        payload = json.loads(account_result.stdout or "{}")
    except json.JSONDecodeError as exc:
        return {"status": "error", "hint": LOGIN_HINT, "details": f"Could not parse 'az account show' output: {exc}"}

    user = payload.get("user") or {}
    return {
        "status": "ok",
        "subscriptionId": payload.get("id"),
        "subscriptionName": payload.get("name"),
        "tenantId": payload.get("tenantId"),
        "userName": user.get("name"),
    }


def check_pwsh() -> dict[str, Any]:
    pwsh_path = shutil.which("pwsh") or shutil.which("pwsh.exe")
    if not pwsh_path:
        return {"status": "missing", "hint": _pwsh_hint()}
    version_result = _run([pwsh_path, "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"])
    if version_result.returncode != 0:
        return {
            "status": "error",
            "hint": _pwsh_hint(),
            "details": (version_result.stderr or version_result.stdout).strip(),
        }
    version = version_result.stdout.strip()
    return {"status": "ok", "path": pwsh_path, "version": version}


def check_python_version() -> dict[str, Any]:
    """Verify the running interpreter meets MIN_PYTHON_VERSION."""
    current = sys.version_info[:2]
    current_str = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    required_str = f"{MIN_PYTHON_VERSION[0]}.{MIN_PYTHON_VERSION[1]}"
    if current < MIN_PYTHON_VERSION:
        return {
            "status": "missing",
            "hint": PYTHON_VERSION_HINT,
            "interpreter": sys.executable,
            "current": current_str,
            "required": f">={required_str}",
        }
    return {
        "status": "ok",
        "interpreter": sys.executable,
        "current": current_str,
        "required": f">={required_str}",
    }


def check_python_packages() -> dict[str, Any]:
    """Verify required Python packages declared in requirements-agent.txt are importable."""
    packages: list[dict[str, Any]] = []
    missing: list[str] = []
    for dist_name, module_name in REQUIRED_PACKAGES.items():
        try:
            importlib.import_module(module_name)
            try:
                version = importlib.metadata.version(dist_name)
            except importlib.metadata.PackageNotFoundError:
                version = None
            packages.append({"name": dist_name, "module": module_name, "status": "ok", "version": version})
        except ImportError as exc:
            packages.append({"name": dist_name, "module": module_name, "status": "missing", "error": str(exc)})
            missing.append(dist_name)

    if missing:
        return {
            "status": "missing",
            "hint": PYTHON_PACKAGES_HINT,
            "interpreter": sys.executable,
            "missing": missing,
            "packages": packages,
        }
    return {"status": "ok", "interpreter": sys.executable, "packages": packages}


def _resolve_winget() -> str | None:
    return shutil.which("winget") or shutil.which("winget.exe")


def _winget_install(package_id: str, log: list[str]) -> bool:
    """Invoke ``winget install`` for a given package id. Returns True on success."""
    winget = _resolve_winget()
    if not winget:
        log.append(WINGET_MISSING_HINT)
        return False
    log.append(f"Running: winget install --id {package_id} --source winget (user scope)")
    proc = subprocess.run(
        [
            winget,
            "install",
            "--id",
            package_id,
            "--source",
            "winget",
            "--silent",
            "--accept-source-agreements",
            "--accept-package-agreements",
        ],
        check=False,
    )
    if proc.returncode == 0:
        log.append(f"winget install {package_id} succeeded.")
        return True
    # winget returns 0x8A15002B (-1978335189) when the package is already installed.
    if proc.returncode in (-1978335189, 0x8A15002B & 0xFFFFFFFF):
        log.append(f"winget reports {package_id} is already installed.")
        return True
    log.append(f"winget install {package_id} exited with code {proc.returncode}.")
    return False


def _refresh_path() -> None:
    """Append common install locations to PATH so newly installed tools resolve in this process."""
    extra = [
        r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin",
        r"C:\Program Files\PowerShell\7",
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
    ]
    current = os.environ.get("PATH", "")
    parts = [p for p in extra if p not in current]
    if parts:
        os.environ["PATH"] = os.pathsep.join(parts + [current])


def _resolve_brew() -> str | None:
    return shutil.which("brew")


def _brew_install(package: str, log: list[str], cask: bool = False) -> bool:
    brew = _resolve_brew()
    if not brew:
        log.append("Homebrew (brew) not found. Install from https://brew.sh and re-run.")
        return False
    args = [brew, "install"]
    if cask:
        args.append("--cask")
    args.append(package)
    log.append(f"Running: {' '.join(args)}")
    proc = subprocess.run(args, check=False)
    if proc.returncode == 0:
        log.append(f"brew install {package} succeeded.")
        return True
    log.append(f"brew install {package} exited with code {proc.returncode}.")
    return False


def _resolve_apt() -> str | None:
    if sys.platform != "linux":
        return None
    return shutil.which("apt-get")


def _apt_install(packages: list[str], log: list[str]) -> bool:
    apt = _resolve_apt()
    if not apt:
        log.append("apt-get not found. Install the missing tools manually per the hint.")
        return False
    sudo = shutil.which("sudo")
    cmd: list[str] = []
    if sudo and os.geteuid() != 0:
        cmd.append(sudo)
    cmd.extend([apt, "install", "-y", *packages])
    log.append(f"Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, check=False)
    if proc.returncode == 0:
        log.append(f"apt-get install {' '.join(packages)} succeeded.")
        return True
    log.append(f"apt-get install exited with code {proc.returncode}.")
    return False


def _pip_install_requirements(log: list[str]) -> bool:
    if not REQUIREMENTS_FILE.exists():
        log.append(f"requirements-agent.txt not found at {REQUIREMENTS_FILE}. Skipping pip install.")
        return False
    log.append(f"Running: {sys.executable} -m pip install -r {REQUIREMENTS_FILE}")
    proc = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(REQUIREMENTS_FILE)],
        check=False,
    )
    if proc.returncode == 0:
        log.append("pip install succeeded.")
        return True
    log.append(f"pip install exited with code {proc.returncode}.")
    return False


def _prompt_yes_no(question: str, assume_yes: bool) -> bool:
    """Prompt the user for y/N confirmation. Returns True on yes."""
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        print(f"{question} (non-interactive session; pass --yes to auto-confirm) -> skipping", file=sys.stderr, flush=True)
        return False
    try:
        answer = input(f"{question} [y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("", file=sys.stderr, flush=True)
        return False
    return answer in ("y", "yes")


def _install_system_tool(
    display_name: str,
    winget_id: str,
    brew_args: tuple[str, bool],
    apt_packages: list[str],
    log: list[str],
    assume_yes: bool,
) -> None:
    if not _prompt_yes_no(f"{display_name} is missing. Install it now?", assume_yes):
        log.append(f"Skipped {display_name} install (user declined).")
        return
    if sys.platform == "win32":
        _winget_install(winget_id, log)
    elif sys.platform == "darwin":
        formula, is_cask = brew_args
        _brew_install(formula, log, cask=is_cask)
    elif sys.platform == "linux":
        if _resolve_apt() and apt_packages:
            _apt_install(apt_packages, log)
        else:
            log.append(
                f"{display_name} auto-install not supported on this Linux distribution. "
                "Install manually per the hint in the preflight output."
            )
    else:
        log.append(f"{display_name} auto-install not supported on platform '{sys.platform}'.")


def install_missing(
    az_status: dict[str, Any],
    pwsh_status: dict[str, Any],
    packages_status: dict[str, Any],
    assume_yes: bool,
) -> list[str]:
    """Install missing Azure CLI, PowerShell 7, and Python packages using the best package manager for this OS.

    Windows -> winget; macOS -> brew; Debian/Ubuntu Linux -> apt-get.
    """
    log: list[str] = []
    if az_status["status"] == "missing":
        _install_system_tool(
            "Azure CLI",
            winget_id="Microsoft.AzureCLI",
            brew_args=("azure-cli", False),
            apt_packages=["azure-cli"],
            log=log,
            assume_yes=assume_yes,
        )
    if pwsh_status["status"] == "missing":
        _install_system_tool(
            "PowerShell 7",
            winget_id="Microsoft.PowerShell",
            brew_args=("powershell", True),
            apt_packages=["powershell"],
            log=log,
            assume_yes=assume_yes,
        )
    _refresh_path()

    if packages_status["status"] == "missing":
        missing_names = ", ".join(packages_status.get("missing") or [])
        if _prompt_yes_no(
            f"Python packages missing ({missing_names}). Install now via pip into {sys.executable}?",
            assume_yes,
        ):
            _pip_install_requirements(log)
        else:
            log.append("Skipped Python package install (user declined).")
    return log


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify az CLI, PowerShell 7, Python packages, and Azure sign-in are ready for agent workflows."
    )
    parser.add_argument("--pretty", action="store_true", help="Emit indented JSON output.")
    parser.add_argument(
        "--install",
        action="store_true",
        help="Offer to install missing tools (Azure CLI, PowerShell 7, Python packages). Prompts for confirmation unless --yes is also passed.",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip confirmation prompts during --install. Use with caution.",
    )
    parser.add_argument(
        "--python-only",
        action="store_true",
        help=(
            "Check only the Python interpreter version (no Azure CLI, PowerShell, or package checks). "
            "Used by authoring flows like /fdp-03-author and /fdp-03-convert that do not need cloud auth."
        ),
    )
    return parser.parse_args()


def _collect() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    python_version = check_python_version()
    az = check_az_installed()
    account = check_az_account(az["path"]) if az["status"] == "ok" else {"status": "skipped"}
    pwsh = check_pwsh()
    packages = check_python_packages()
    return python_version, az, account, pwsh, packages


def main() -> int:
    args = parse_args()

    if args.python_only:
        python_version = check_python_version()
        ready = python_version["status"] == "ok"
        result: dict[str, Any] = {
            "ready": ready,
            "status": "ready" if ready else "not_ready",
            "mode": "python-only",
            "pythonVersion": python_version,
        }
        if not ready:
            result["nextStep"] = python_version.get("hint")
        output = json.dumps(result, indent=2) if args.pretty else json.dumps(result, separators=(",", ":"))
        print(output, flush=True)
        return 0 if ready else 1

    python_version, az, account, pwsh, packages = _collect()
    install_log: list[str] = []

    needs_install = (
        az["status"] == "missing"
        or pwsh["status"] == "missing"
        or packages["status"] == "missing"
    )
    if args.install and needs_install:
        install_log = install_missing(az, pwsh, packages, assume_yes=args.yes)
        python_version, az, account, pwsh, packages = _collect()

    ready = (
        python_version["status"] == "ok"
        and az["status"] == "ok"
        and account["status"] == "ok"
        and pwsh["status"] == "ok"
        and packages["status"] == "ok"
    )
    result: dict[str, Any] = {
        "ready": ready,
        "status": "ready" if ready else "not_ready",
        "pythonVersion": python_version,
        "azCli": az,
        "account": account,
        "pwsh": pwsh,
        "pythonPackages": packages,
    }
    if install_log:
        result["installLog"] = install_log

    if not ready:
        if python_version["status"] != "ok":
            next_step = python_version.get("hint")
        elif az["status"] != "ok":
            next_step = az.get("hint")
        elif account["status"] != "ok":
            next_step = account.get("hint")
        elif pwsh["status"] != "ok":
            next_step = pwsh.get("hint")
        else:
            next_step = packages.get("hint")
        result["nextStep"] = next_step

    output = json.dumps(result, indent=2) if args.pretty else json.dumps(result, separators=(",", ":"))
    print(output, flush=True)
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())

# Installation Guide for Astronomer CLI on Windows

## Prerequisites

Before installing the Astronomer CLI, ensure you have the following prerequisites:

- Windows 10 or later
- Administrator privileges
- PowerShell or Command Prompt

## Step-by-Step Installation

### 1. Install Docker Desktop

Astronomer CLI requires Docker to run Airflow. Follow these steps to install Docker Desktop:

1. Download Docker Desktop from the [official website](https://www.docker.com/products/docker-desktop).
2. Run the installer and follow the on-screen instructions.
3. After installation, start Docker Desktop and ensure it is running.

### 2. Install WSL 2

Docker Desktop requires WSL 2 as its backend. Follow these steps to install WSL 2:

1. Open PowerShell as an administrator.
2. Run the following command to enable WSL:
    ```sh
    wsl --install
    ```
3. Restart your computer if prompted.

### 3. Install Astronomer CLI

1. Open PowerShell or Command Prompt.
2. Run the following command to download and install the Astronomer CLI:
    ```sh
    winget install -e --id Astronomer.Astro
    ```
3. Verify the installation by running:
    ```sh
    astro version
    ```

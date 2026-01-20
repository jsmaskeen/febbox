# FebBox Uploader

A CLI tool to upload files and directories to FebBox with resume capability, parallel chunk uploads.

## Features

- Automatically resumes interrupted uploads.
- Maximizes bandwidth by uploading multiple chunks simultaneously.
- Uploads entire directory structures, maintaining hierarchy.
- Beautiful progress bars and status updates .

## Installation

```bash
pip install febbox
```

## Configuration

Before using the tool, you need to configure your FebBox cookies (`PHPSESSID` and `ui`).

1.  Run the configuration wizard:
    ```bash
    febbox --configure
    ```

2.  Enter your `PHPSESSID` and `ui` cookies when prompted. You can find these in your browser's developer tools (Application -> Cookies) while logged into FebBox. Or use the [editthiscookie](https://editcookie.com/) extension for chrome.


### How to Get Your Febbox UI Token

1. Visit [febbox.com](https://www.febbox.com) and log in with Google (use a fresh account).
2. Open `DevTools` in your browser or inspect the page.
3. Go to the `Application` tab -> `Cookies`.
4. Look for the cookie named `ui` and `PHPSESSID`.
5. Copy both the values.
6. Close the tab, but **DO NOT LOGOUT** to keep your token valid.

> DO NOT share your cookies with anyone!

## Usage

### Upload a Single File

```bash
febbox path/to/your/file.mp4
```

### Upload a Directory

```bash
febbox path/to/your/folder
```

### Get Storage Quota Used

```bash
febbox --info
```

### Advanced Usage

Specify parent folder ID (default is root: 0):
```bash
febbox path/to/file.mp4 --parent-id 12345
```

Adjust concurrency (chunks per file, default: 20):
```bash
febbox path/to/file.mp4 --concurrency 50
```

Adjust parallel file uploads (for directories, default: 5):
```bash
febbox path/to/folder --max-uploads 10
```

## Help

For a full list of commands:
```bash
febbox --help
```

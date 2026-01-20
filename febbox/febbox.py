import argparse
import asyncio
import aiohttp
import hashlib
import os
import math
import time
import json
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    FileSizeColumn
)
from rich.theme import Theme


def humanbytes(size: float) -> str:
    if not size:
        return ""
    power = 2**10
    number = 0
    dict_power_n = {0: "", 1: "Ki", 2: "Mi", 3: "Gi", 4: "Ti", 5: "Pi"}
    while size > power:
        size /= power
        number += 1
    return str(round(size, 3)) + " " + dict_power_n[number] + "B"

custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "bold red",
    "success": "bold green",
})
console = Console(theme=custom_theme)

class FebboxUploader:
    def __init__(self, cookies=None, headers=None):
        self.cookies = cookies if cookies else {}
        self.headers = {}
        if headers:
            filtered_headers = {
                k: v for k, v in headers.items() 
                if not k.startswith(":") and k.lower() not in ["content-type", "content-length"]
            }
            self.headers.update(filtered_headers)
        
        self.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://www.febbox.com/console",
            "Origin": "https://www.febbox.com"
        })
        
        self.executor = ThreadPoolExecutor(max_workers=4)

    def _calculate_md5_sync(self, file_object):
        hash_md5 = hashlib.md5()
        if isinstance(file_object, bytes):
            hash_md5.update(file_object)
        else:
            pos = file_object.tell()
            for chunk in iter(lambda: file_object.read(4096), b""):
                hash_md5.update(chunk)
            file_object.seek(pos)
        return hash_md5.hexdigest()

    def _calculate_febbox_hash_sync(self, file_object, size):
        def get_slice_md5(start, length):
            if isinstance(file_object, bytes):
                data = file_object[start:start+length]
            else:
                file_object.seek(start)
                data = file_object.read(length)
            return hashlib.md5(data).hexdigest()

        md5_1 = get_slice_md5(4096, 4096)
        
        start2 = math.floor(size / 3) * 2
        md5_2 = get_slice_md5(start2, 4096)
        
        start3 = math.floor(size / 3)
        md5_3 = get_slice_md5(start3, 4096)
        
        start4 = math.floor(size - 8192)
        if start4 <= 0:
            start4 = math.floor(size / 3) + 4096
        md5_4 = get_slice_md5(start4, 4096)
        
        combined = md5_1 + md5_2 + md5_3 + md5_4 + "_" + str(size)
        return hashlib.md5(combined.encode('utf-8')).hexdigest()

    async def calculate_hash(self, file_path, file_size, is_large_file):
        loop = asyncio.get_running_loop()
        def _hash():
            with open(file_path, "rb") as f:
                if is_large_file:
                    return self._calculate_febbox_hash_sync(f, file_size)
                else:
                    return self._calculate_md5_sync(f)
        return await loop.run_in_executor(self.executor, _hash)

    async def _read_chunk(self, file_path, offset, size):
        loop = asyncio.get_running_loop()
        def _read():
            with open(file_path, "rb") as f:
                f.seek(offset)
                return f.read(size)
        return await loop.run_in_executor(self.executor, _read)

    async def _upload_chunk(self, session, chunk_idx, file_path, chunk_token, chunk_size_default, file_name, file_size, file_md5, parent_id, last_time, is_large_file, semaphore):
        async with semaphore:
            try:
                offset = chunk_idx * chunk_size_default
                chunk_content = await self._read_chunk(file_path, offset, chunk_size_default)
                current_chunk_size = len(chunk_content)

                loop = asyncio.get_running_loop()
                
                if is_large_file:
                     chunk_hash = await loop.run_in_executor(self.executor, self._calculate_febbox_hash_sync, chunk_content, current_chunk_size)
                else:
                     chunk_hash = await loop.run_in_executor(self.executor, self._calculate_md5_sync, chunk_content)

                url_chunk2 = "https://www.febbox.com/console/file_upload_chunk2"
                
                data_chunk2 = aiohttp.FormData()
                data_chunk2.add_field("chunk_data", chunk_token)
                data_chunk2.add_field("file_name", file_name)
                data_chunk2.add_field("file_size", str(file_size))
                data_chunk2.add_field("file_hash", file_md5)
                data_chunk2.add_field("chunk_hash", chunk_hash)
                data_chunk2.add_field("chunk_size2", str(current_chunk_size))
                
                async with session.post(url_chunk2, params={"chunk": chunk_idx}, data=data_chunk2) as resp2:
                    resp2_json = await resp2.json()
                
                api_chunk_url = resp2_json.get("api_chunk")
                if not api_chunk_url:
                    return {"status": "error", "msg": f"[{file_name}] Chunk {chunk_idx} Failed: No api_chunk url. Msg: {resp2_json.get('msg')}", "bytes": 0}

                api_data_token = resp2_json.get("api_data")
                file_data_token = resp2_json.get("file_data")

                data_step3 = aiohttp.FormData()
                data_step3.add_field("data", api_data_token)
                data_step3.add_field("file", chunk_content, filename=file_name, content_type="application/octet-stream")
                
                headers_step3 = {
                    "User-Agent": self.headers["User-Agent"],
                    "Origin": "https://www.febbox.com",
                    "Referer": "https://www.febbox.com/"
                }
                
                async with session.post(api_chunk_url, data=data_step3, headers=headers_step3) as resp3:
                    resp3_text = await resp3.text()
                    try:
                        resp3_json = json.loads(resp3_text)
                    except json.JSONDecodeError:
                         return {"status": "error", "msg": f"[{file_name}] Chunk {chunk_idx} Failed: Step 3 non-JSON response. Body: {resp3_text[:200]}", "bytes": 0}

                if resp3_json.get("msg") == "file success":
                    oss_fid = resp3_json["data"]["oss_fid"]
                    
                    url_add = "https://www.febbox.com/console/file_add"
                    
                    data_add = aiohttp.FormData()
                    data_add.add_field("file_data", file_data_token)
                    data_add.add_field("oss_fid", oss_fid)
                    data_add.add_field("path", "")
                    data_add.add_field("last_time", str(last_time))
                    
                    params_add = {
                        "parent_id": parent_id,
                        "from_uid": "",
                        "fid": ""
                    }
                    
                    async with session.post(url_add, params=params_add, data=data_add) as resp4:
                        resp4_json = await resp4.json()
                    
                    if resp4_json.get("msg") == "file add success":
                        return {"status": "success", "msg": f"[{file_name}] DONE", "finished": True, "bytes": current_chunk_size}
                    else:
                        return {"status": "success", "msg": f"[{file_name}] Chunk {chunk_idx} DONE", "finished": False, "bytes": current_chunk_size}
                else:
                    return {"status": "error", "msg": f"[{file_name}] Chunk {chunk_idx} FAILED: Step 3. {resp3_json}", "bytes": 0}

            except Exception as e:
                import traceback
                return {"status": "error", "msg": f"[{file_name}] Chunk {chunk_idx} Exception: {str(e)} {traceback.format_exc()}", "bytes": 0}

    async def upload_file(self, file_path, parent_id=0, concurrency=20, progress=None):
        if not os.path.exists(file_path):
            console.print(f"[error]File not found: {file_path}[/error]")
            return

        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)
        last_time = int(time.time() * 1000)
        
        is_large_file = file_size >= 52428800 # 50MB

        if not progress:
            console.print(f"[info]Preparing to upload: {file_name}[/info]")

        file_md5 = await self.calculate_hash(file_path, file_size, is_large_file)
        
        async with aiohttp.ClientSession(cookies=self.cookies, headers=self.headers) as session:
            url_init = "https://www.febbox.com/console/file_upload"
            params_init = {
                "parent_id": parent_id,
                "from_uid": "",
                "fid": ""
            }
            
            data_init = aiohttp.FormData()
            data_init.add_field("name", file_name)
            data_init.add_field("size", str(file_size))
            data_init.add_field("hash", file_md5)
            data_init.add_field("path", "")
            data_init.add_field("last_time", str(last_time))
            
            async with session.post(url_init, params=params_init, data=data_init) as resp1:
                resp1_json = await resp1.json()

            if resp1_json.get("msg") == "file add success":
                if progress:
                     progress.console.print(f"[success]File already exists or uploaded instantly: {file_name}[/success]")
                else:
                     console.print(f"[success]File already exists or uploaded instantly: {file_name}[/success]")
                return

            if resp1_json.get("code") != 1:
                msg = f"[error]Error in Step 1 for {file_name}: {resp1_json.get('msg')}[/error]"
                if progress:
                    progress.console.print(msg)
                else:
                    console.print(msg)
                return

            chunk_token = resp1_json["data"]["chunk_data"]
            chunk_size_default = resp1_json["data"]["chunk_size"]
            not_upload_chunks = resp1_json["data"].get("not_upload", [])

            if not not_upload_chunks:
                if progress:
                     progress.console.print(f"[info]No chunks to upload for {file_name}.[/info]")
                else:
                     console.print(f"[info]No chunks to upload for {file_name}.[/info]")
                return

            semaphore = asyncio.Semaphore(concurrency)
            tasks = []
            for chunk_idx in not_upload_chunks:
                task = asyncio.create_task(
                    self._upload_chunk(
                        session,
                        chunk_idx,
                        file_path,
                        chunk_token,
                        chunk_size_default,
                        file_name,
                        file_size,
                        file_md5,
                        parent_id,
                        last_time,
                        is_large_file,
                        semaphore
                    )
                )
                tasks.append(task)

            should_close_progress = False
            if progress is None:
                should_close_progress = True
                progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
                    BarColumn(bar_width=None),
                    "[progress.percentage]{task.percentage:>3.1f}%",
                    "•",
                    FileSizeColumn(),
                    "•",
                    TransferSpeedColumn(),
                    "•",
                    TimeRemainingColumn(),
                    console=console
                )
                progress.start()
            
            task_id = progress.add_task("upload", filename=file_name, total=file_size, start=True)

            try:
                for completed_task in asyncio.as_completed(tasks):
                    result = await completed_task
                    progress.update(task_id, advance=result.get("bytes", 0))
                    if result["status"] == "error":
                        progress.console.print(f"[error]{result['msg']}[/error]")
                
                progress.update(task_id, description=f"[green]Done[/green]")
            finally:
                if should_close_progress:
                    progress.stop()
                

    async def create_directory(self, dir_name, parent_id=0):
        async with aiohttp.ClientSession(cookies=self.cookies, headers=self.headers) as session:
            url_init = "https://www.febbox.com/console/new_floder"
            params_init = {
                "parent_id": parent_id,
                "name":dir_name,
                "from_uid": "",
                "fid": ""
            }
            
            async with session.get(url_init, params=params_init) as resp1:
                resp1_json = await resp1.json()

                if resp1_json.get("code") == 1:
                    return resp1_json["data"]["fid"]
                else:
                    console.print(f"[error]Error creating directory '{dir_name}': {resp1_json.get('msg')}[/error]")
                    return None

    async def upload_directory(self, dir_path, parent_id=0, concurrency=20, max_uploads=5):
        if not os.path.exists(dir_path):
            console.print(f"[error]Directory not found: {dir_path}[/error]")
            return

        dir_name = os.path.basename(os.path.normpath(dir_path))
        console.print(f"[info]Creating root directory: {dir_name}[/info]")
        root_fid = await self.create_directory(dir_name, parent_id)
        if not root_fid:
            console.print(f"[error]Aborting: Could not create root directory {dir_name}[/error]")
            return

        queue = deque([(os.path.abspath(dir_path), root_fid)])
        file_tasks = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]{task.description}"),
            console=console,
            transient=True
        ) as scan_progress:
            scan_task = scan_progress.add_task("Scanning structure...", total=None)
            
            dir_count = 0
            file_count = 0

            while queue:
                local_path, parent_fid = queue.popleft()
                
                try:
                    entries = os.listdir(local_path)
                except OSError as e:
                    console.print(f"[error]Error reading directory {local_path}: {e}[/error]")
                    continue
                    
                dirs = []
                files = []
                for entry in entries:
                    full_path = os.path.join(local_path, entry)
                    if os.path.isdir(full_path):
                        dirs.append(entry)
                    else:
                        files.append(full_path)
                
                for f_path in files:
                    file_tasks.append((f_path, parent_fid))
                    file_count += 1
                
                scan_progress.update(scan_task, description=f"Scanning structure... (Dirs created: {dir_count}, Files found: {file_count})")

                for d in dirs:
                    new_fid = await self.create_directory(d, parent_fid)
                    if new_fid:
                         queue.append((os.path.join(local_path, d), new_fid))
                         dir_count += 1
                         scan_progress.update(scan_task, description=f"Scanning structure... (Dirs created: {dir_count}, Files found: {file_count})")
                    else:
                         console.print(f"[error]Failed to create folder: {d}[/error]")
        
        console.print(f"[success]Structure Ready. Found {len(file_tasks)} files. Starting uploads (Max Parallel Files: {max_uploads})...[/success]")
        
        upload_semaphore = asyncio.Semaphore(max_uploads)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            FileSizeColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            console=console
        ) as upload_progress:
            
            async def protected_upload(f_path, p_id):
                async with upload_semaphore:
                    await self.upload_file(f_path, parent_id=p_id, concurrency=concurrency, progress=upload_progress)
            
            tasks = [asyncio.create_task(protected_upload(fp, pid)) for fp, pid in file_tasks]
            if tasks:
                await asyncio.gather(*tasks)
        
        console.print(f"[success]Directory upload complete: {dir_name}[/success]")
        
    async def userinfo(self):
        async with aiohttp.ClientSession(cookies=self.cookies, headers=self.headers) as session:
            url = "https://www.febbox.com/console/user_info"
            async with session.get(url) as resp:
                resp_json = await resp.json()
                if resp_json.get("code") == 1:
                    console.print(f"[info]Quota Used:[/info] [success]{humanbytes(resp_json['user']['quota_used2'])}[/success] of [success]{humanbytes(resp_json['user']['quota_total2'])}[/success]")        

CONFIG_FILE = os.path.join(os.path.expanduser("~"), ".febbox_config")

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return None
    try:
        with open(CONFIG_FILE, 'r') as f:
            cookies = json.load(f)
            if any(cookies.get(key) == '' for key in ["PHPSESSID", "ui"]):
                return None
            if not cookies:
                return None
            return cookies
    except Exception:
        return None

def save_config(cookies):
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(cookies, f, indent=4)
        console.print(f"[success]Configuration saved to {os.path.abspath(CONFIG_FILE)}[/success]")
    except Exception as e:
        console.print(f"[error]Error saving configuration: {e}[/error]")

def configure_interactive():
    console.print("[bold]Febbox Configuration Wizard[/bold]")
    console.print("Please enter the following cookie values from your browser:")
    
    phpsessid = input("PHPSESSID: ").strip()
    ui = input("ui: ").strip()    
    cookies = {
        "PHPSESSID": phpsessid,
        "ui": ui,
        "perf_dv6Tr4b": "1",
        "list_mode": "grid2"
    }
    
    save_config(cookies)

def main():
    parser = argparse.ArgumentParser(description="Febbox Uploader")
    parser.add_argument("path", nargs="?", help="Path to the file or directory to upload")
    parser.add_argument("--parent-id", type=int, default=0, help="Parent folder ID on Febbox")
    parser.add_argument("--concurrency", type=int, default=20, help="Number of concurrent chunks per file")
    parser.add_argument("--max-uploads", type=int, default=5, help="Number of concurrent file uploads")
    parser.add_argument("--configure", action="store_true", help="Configure cookies")
    parser.add_argument("--info", action="store_true", help="Show Quota Info")
    args = parser.parse_args()
    
    if args.configure:
        configure_interactive()
        exit(0)

    cookies = load_config()
    if not cookies:
        console.print("[warning]Configuration file not found or invalid.[/warning]")
        console.print("Please run 'python febbox.py --configure' or 'febbox --configure' to set up your cookies.")
        exit(1)
    

    headers = {
        ":authority": "www.febbox.com",
        ":method": "POST",
        ":path": f"/console/file_upload?parent_id={args.parent_id}&from_uid=&fid=",
        ":scheme": "https",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-length": "562",
        "content-type": "multipart/form-data; boundary=----WebKitFormBoundarylmgk08EBzyHKokRx",
        "origin": "https://www.febbox.com",
        "priority": "u=1, i",
        "referer": "https://www.febbox.com/console",
        "sec-ch-ua": "\"Google Chrome\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }

    uploader = FebboxUploader(cookies=cookies, headers=headers)
    
    if args.info:
        asyncio.run(uploader.userinfo())
        exit(0)
        
    if not args.path:
        parser.print_help()
        exit(1)

    path_to_upload = args.path
    if os.path.isfile(path_to_upload):
        asyncio.run(uploader.upload_file(path_to_upload, parent_id=args.parent_id, concurrency=args.concurrency))
    elif os.path.isdir(path_to_upload):
        asyncio.run(uploader.upload_directory(path_to_upload, parent_id=args.parent_id, concurrency=args.concurrency, max_uploads=args.max_uploads))
    else:
        console.print(f"[error]Invalid path: {path_to_upload}[/error]")

if __name__ == "__main__":
    main()

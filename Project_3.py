"""
s3_monitor_gui.py

Monitors a local directory for new/modified files and uploads them to an S3 bucket.
Provides a Tkinter GUI to configure settings and view logs.

Dependencies:
    pip install boto3 watchdog
"""

import os
import threading
import logging
import time
import queue
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import boto3
from botocore.exceptions import BotoCoreError, ClientError

LOG_FILENAME = "../s3_monitor.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILENAME, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

class S3Uploader:
    def __init__(self, bucket_name, region=None, aws_access_key_id=None, aws_secret_access_key=None):
        self.bucket_name = bucket_name
        try:
            if aws_access_key_id and aws_secret_access_key:
                session = boto3.Session(
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region
                )
            else:
                session = boto3.Session(region_name=region)
            self.s3 = session.client('s3')
            logging.info("Initialized S3 client.")
        except Exception as e:
            logging.exception("Failed to initialize S3 client: %s", e)
            raise

    def upload_file(self, local_path, s3_key):
        """
        Upload a file to S3. Raises exception if failed.
        """
        logging.info("Uploading %s => s3://%s/%s", local_path, self.bucket_name, s3_key)
        # Attempt upload
        try:
            # Use multipart upload for big files automatically by boto3.
            self.s3.upload_file(local_path, self.bucket_name, s3_key)
            logging.info("Uploaded %s to %s/%s", local_path, self.bucket_name, s3_key)
            return True
        except (BotoCoreError, ClientError) as e:
            logging.exception("Upload failed for %s: %s", local_path, e)
            raise

class WatchHandler(FileSystemEventHandler):
    def __init__(self, root_dir, uploader, gui_queue, max_retries=2, debounce_seconds=1.0):
        """
        root_dir: the monitored root directory
        uploader: S3Uploader instance
        gui_queue: queue.Queue to send logs/notifications to GUI
        max_retries: number of retries for uploads
        debounce_seconds: ignore very rapid duplicate events for the same path
        """
        super().__init__()
        self.root_dir = os.path.abspath(root_dir)
        self.uploader = uploader
        self.gui_queue = gui_queue
        self.max_retries = max_retries
        self._recent = {}  # path => last_event_time
        self.debounce_seconds = debounce_seconds

    def _should_process(self, src_path):
        now = time.time()
        last = self._recent.get(src_path)
        if last and (now - last) < self.debounce_seconds:
            logging.debug("Debounced event for %s", src_path)
            return False
        self._recent[src_path] = now
        return True

    def _upload(self, src_path):
        # Compute S3 key as relative path from root_dir (use forward slashes)
        try:
            rel = os.path.relpath(src_path, self.root_dir)
        except Exception:
            rel = os.path.basename(src_path)
        s3_key = rel.replace(os.sep, "/")
        attempts = 0
        while attempts <= self.max_retries:
            try:
                self.uploader.upload_file(src_path, s3_key)
                msg = f"Uploaded: {src_path} -> s3://{self.uploader.bucket_name}/{s3_key}"
                logging.info(msg)
                self.gui_queue.put(("info", msg))
                return True
            except Exception as e:
                attempts += 1
                logging.warning("Attempt %d failed for %s: %s", attempts, src_path, e)
                time.sleep(1)  # small backoff
        # If reached here, all attempts failed
        err_msg = f"Failed to upload {src_path} after {self.max_retries+1} attempts."
        logging.error(err_msg)
        self.gui_queue.put(("error", err_msg))
        return False

    def _is_regular_file(self, path):
        # ignore directories
        return os.path.isfile(path)

    def on_created(self, event):
        if event.is_directory:
            return
        src_path = event.src_path
        if not self._should_process(src_path):
            return
        # wait a short moment for file to be fully written (can be tuned)
        time.sleep(0.2)
        if not self._is_regular_file(src_path):
            return
        logging.info("Detected created: %s", src_path)
        self.gui_queue.put(("event", f"Created: {src_path}"))
        threading.Thread(target=self._upload, args=(src_path,), daemon=True).start()

    def on_modified(self, event):
        if event.is_directory:
            return
        src_path = event.src_path
        if not self._should_process(src_path):
            return
        # small pause to ensure write finished
        time.sleep(0.2)
        if not self._is_regular_file(src_path):
            return
        logging.info("Detected modified: %s", src_path)
        self.gui_queue.put(("event", f"Modified: {src_path}"))
        threading.Thread(target=self._upload, args=(src_path,), daemon=True).start()

class MonitorThread(threading.Thread):
    def __init__(self, path, handler):
        super().__init__(daemon=True)
        self.path = path
        self.handler = handler
        self._observer = Observer()

    def run(self):
        logging.info("Starting observer on %s", self.path)
        self._observer.schedule(self.handler, self.path, recursive=True)
        self._observer.start()
        try:
            while True:
                time.sleep(1)
        except Exception:
            logging.exception("Observer thread exiting")
        finally:
            self._observer.stop()
            self._observer.join()

    def stop(self):
        self._observer.stop()
        self._observer.join()

class App:
    def __init__(self, root):
        self.root = root
        root.title("S3 Scheduled Backup & Monitor")
        root.geometry("800x520")

        self.gui_queue = queue.Queue()
        self.monitor_thread = None
        self.watch_handler = None
        self.uploader = None

        # GUI variables
        self.folder_var = tk.StringVar()
        self.bucket_var = tk.StringVar()
        self.region_var = tk.StringVar(value="us-east-1")
        self.aws_key_var = tk.StringVar()
        self.aws_secret_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Idle")

        # Top frame: inputs
        frm_top = ttk.Frame(root, padding=8)
        frm_top.pack(fill=tk.X)

        ttk.Label(frm_top, text="Folder to monitor:").grid(row=0, column=0, sticky=tk.W)
        ent_folder = ttk.Entry(frm_top, textvariable=self.folder_var, width=60)
        ent_folder.grid(row=0, column=1, padx=6, sticky=tk.W)
        ttk.Button(frm_top, text="Browse", command=self.browse_folder).grid(row=0, column=2)

        ttk.Label(frm_top, text="S3 Bucket:").grid(row=1, column=0, sticky=tk.W, pady=(6,0))
        ttk.Entry(frm_top, textvariable=self.bucket_var, width=30).grid(row=1, column=1, sticky=tk.W, padx=6, pady=(6,0))

        ttk.Label(frm_top, text="Region:").grid(row=1, column=2, sticky=tk.W, padx=(6,0), pady=(6,0))
        ttk.Entry(frm_top, textvariable=self.region_var, width=12).grid(row=1, column=3, sticky=tk.W, pady=(6,0))

        # AWS keys (optional)
        ttk.Label(frm_top, text="AWS Access Key (optional):").grid(row=2, column=0, sticky=tk.W, pady=(6,0))
        ttk.Entry(frm_top, textvariable=self.aws_key_var, width=40).grid(row=2, column=1, sticky=tk.W, padx=6, pady=(6,0))
        ttk.Label(frm_top, text="AWS Secret Key (optional):").grid(row=2, column=2, sticky=tk.W, pady=(6,0))
        ttk.Entry(frm_top, textvariable=self.aws_secret_var, show="*", width=40).grid(row=2, column=3, sticky=tk.W, pady=(6,0))

        # control buttons
        frm_controls = ttk.Frame(root, padding=8)
        frm_controls.pack(fill=tk.X)

        self.btn_start = ttk.Button(frm_controls, text="Start Monitoring", command=self.start_monitoring)
        self.btn_start.pack(side=tk.LEFT, padx=6)
        self.btn_stop = ttk.Button(frm_controls, text="Stop Monitoring", command=self.stop_monitoring, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=6)

        ttk.Label(frm_controls, text="Status:").pack(side=tk.LEFT, padx=(20,4))
        ttk.Label(frm_controls, textvariable=self.status_var).pack(side=tk.LEFT)

        ttk.Button(frm_controls, text="Open log file", command=self.open_log_file).pack(side=tk.RIGHT)

        # Log pane
        lbl = ttk.Label(root, text="Activity Log:")
        lbl.pack(anchor=tk.W, padx=8)
        self.log_text = scrolledtext.ScrolledText(root, height=18, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        # update loop for queue
        self.root.after(200, self.process_queue)

    def browse_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.folder_var.set(folder)

    def open_log_file(self):
        import subprocess, sys
        path = os.path.abspath(LOG_FILENAME)
        if os.name == 'nt':
            os.startfile(path)
        elif sys.platform == 'darwin':
            subprocess.call(('open', path))
        else:
            subprocess.call(('xdg-open', path))

    def log(self, level, text):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp} [{level.upper()}] {text}\n"
        # append to GUI text widget
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, line)
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)
        # also to logging module (which writes to file)
        if level == "info":
            logging.info(text)
        elif level == "warning":
            logging.warning(text)
        elif level == "error":
            logging.error(text)
        else:
            logging.debug(text)

    def process_queue(self):
        """
        Pull messages from worker threads and display them
        Messages are tuples: ("info"|"event"|"error", "text")
        """
        try:
            while True:
                kind, text = self.gui_queue.get_nowait()
                if kind == "error":
                    # popup notify about upload failure
                    messagebox.showerror("Upload failed", text)
                    self.log("error", text)
                elif kind in ("info", "event"):
                    self.log("info", text)
                else:
                    self.log("info", text)
        except queue.Empty:
            pass
        finally:
            self.root.after(200, self.process_queue)

    def start_monitoring(self):
        folder = self.folder_var.get().strip()
        bucket = self.bucket_var.get().strip()
        region = self.region_var.get().strip() or None
        key = self.aws_key_var.get().strip() or None
        secret = self.aws_secret_var.get().strip() or None

        if not folder or not os.path.isdir(folder):
            messagebox.showwarning("Folder missing", "Please choose an existing folder to monitor.")
            return
        if not bucket:
            messagebox.showwarning("Bucket missing", "Please enter your S3 bucket name.")
            return

        # create uploader
        try:
            self.uploader = S3Uploader(bucket_name=bucket, region=region,
                                       aws_access_key_id=key, aws_secret_access_key=secret)
            # optional: validate bucket exists by listing or head_bucket
            try:
                self.uploader.s3.head_bucket(Bucket=bucket)
            except ClientError as e:
                # head_bucket will fail if the bucket doesn't exist or access denied
                logging.warning("S3 head_bucket check failed: %s", e)
                # still allow start, but warn
                if messagebox.askyesno("Bucket check", f"Could not access bucket {bucket}.\nProceed anyway?"):
                    pass
                else:
                    return
        except Exception as e:
            messagebox.showerror("S3 init error", f"Failed to initialize S3 connection: {e}")
            return

        # create watcher
        self.watch_handler = WatchHandler(folder, self.uploader, self.gui_queue)
        self.monitor_thread = MonitorThread(folder, self.watch_handler)
        self.monitor_thread.start()
        self.status_var.set(f"Monitoring {folder}")
        self.btn_start.configure(state=tk.DISABLED)
        self.btn_stop.configure(state=tk.NORMAL)
        self.log("info", f"Started monitoring {folder} for changes. Uploads to bucket: {bucket}")

    def stop_monitoring(self):
        if self.monitor_thread:
            try:
                self.monitor_thread._observer.stop()
                self.monitor_thread._observer.join(timeout=2)
            except Exception:
                logging.exception("Error stopping observer")
            self.monitor_thread = None
            self.watch_handler = None
        self.status_var.set("Idle")
        self.btn_start.configure(state=tk.NORMAL)
        self.btn_stop.configure(state=tk.DISABLED)
        self.log("info", "Stopped monitoring.")

def main():
    root = tk.Tk()
    app = App(root)
    root.protocol("WM_DELETE_WINDOW", lambda: on_close(root, app))
    root.mainloop()

def on_close(root, app):
    if messagebox.askokcancel("Quit", "Stop monitoring and quit?"):
        try:
            app.stop_monitoring()
        except Exception:
            logging.exception("Error during stop")
        root.destroy()

if __name__ == "__main__":
    main()


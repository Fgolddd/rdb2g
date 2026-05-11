import sys
import time


def format_elapsed(seconds):
    seconds = max(float(seconds or 0), 0.0)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{sec:02d}s"


class ProgressBar:
    def __init__(self, total, label, unit="项", width=24, min_interval=1.0):
        self.total = max(int(total or 0), 0)
        self.label = str(label)
        self.unit = str(unit)
        self.width = int(width)
        self.min_interval = float(min_interval)
        self.current = 0
        self.start_time = time.perf_counter()
        self.last_print = 0.0
        self.is_tty = sys.stdout.isatty()
        self._closed = False
        self.render(force=True)

    def update(self, step=1, detail=None, force=False):
        if self._closed:
            return
        self.current += int(step)
        if self.total:
            self.current = min(self.current, self.total)
        self.render(detail=detail, force=force)

    def render(self, detail=None, force=False):
        now = time.perf_counter()
        if not force and now - self.last_print < self.min_interval:
            return
        self.last_print = now

        elapsed = now - self.start_time
        if self.total:
            ratio = self.current / self.total if self.total else 0
            done = int(self.width * ratio)
            bar = "#" * done + "-" * (self.width - done)
            percent = ratio * 100
            rate = self.current / elapsed if elapsed > 0 else 0
            remaining = (self.total - self.current) / rate if rate > 0 else 0
            msg = (
                f"[{bar}] {percent:5.1f}% {self.current}/{self.total} {self.unit} "
                f"elapsed={format_elapsed(elapsed)} eta={format_elapsed(remaining)}"
            )
        else:
            msg = f"{self.current} {self.unit} elapsed={format_elapsed(elapsed)}"

        if detail:
            msg = f"{msg} | {detail}"
        line = f"{self.label}: {msg}"
        if self.is_tty:
            sys.stdout.write("\r" + line)
            sys.stdout.flush()
        else:
            print(line, flush=True)

    def close(self, detail=None):
        if self._closed:
            return
        if self.total:
            self.current = self.total
        self.render(detail=detail or "完成", force=True)
        if self.is_tty:
            sys.stdout.write("\n")
            sys.stdout.flush()
        self._closed = True


def progress_iter(iterable, total, label, unit="项", min_interval=1.0):
    progress = ProgressBar(total=total, label=label, unit=unit, min_interval=min_interval)
    try:
        for item in iterable:
            yield item
            progress.update(detail=str(item))
    finally:
        progress.close()

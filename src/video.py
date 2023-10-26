import subprocess
from pathlib import Path
from random import random


class Mp4:
    def __init__(self, path: Path):
        self.location = path

    def play(self):
        subprocess.run(["open", self.location])


class FileList:
    def __init__(self, files: list[Path]):
        self.playlist = [Mp4(file) for file in files]
        self.size = len(self.playlist)

    def shuffle(self, times: int = 1):
        for _ in range(times):
            shuffled_seed = random.randint(a=0, b=self.size)
            self.playlist[shuffled_seed].play()

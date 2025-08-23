import subprocess
from pathlib import Path

from amulet.api.data_types import Dimension, ChunkCoordinates
from more_itertools import flatten


def preload_chunk_coords(load_world) -> dict[Dimension, list[ChunkCoordinates]]:
    world = load_world()
    try:
        return {
            dimension: list(world.all_chunk_coords(dimension)) for dimension in world.dimensions
        }  # Preload
    finally:
        world.close()


def fix_zip(path: Path):
    """
    Note: Fixing perhaps lead to the zip being broken
    :param path:
    :return:
    """
    abspath = path.resolve()
    tempzip_path = abspath.with_name("temp.zip")
    subprocess.run([
        "zip", "-FF", str(abspath), "--out", str(tempzip_path)
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    tempzip_path.replace(abspath)
    return path


def is_unicode(text: str) -> bool:
    return any(ord(c) > 0xff for c in text)


flatten_list = lambda x: list(flatten(x))



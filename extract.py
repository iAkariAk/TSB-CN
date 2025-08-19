import json
import math
import os
import re
from Lib.test.test_buffer import flatten
from contextlib import suppress
from functools import partial
from pathlib import Path
from typing import TypedDict, cast
from zipfile import Path as ZipPath, BadZipFile
from zipfile import ZipFile

import amulet
import amulet_nbt
from amulet.api.block_entity import BlockEntity
from amulet_nbt import AbstractBaseTag, ListTag, CompoundTag, StringTag
from more_itertools import unique_everseen, chunked

from utils import preload_chunk_coords, flatten_list, is_unicode


def create_workspace_from_zip(path: Path) -> Path:
    workspace_dir = Path("./workspace")
    with ZipFile(path) as izip:
        izip.extractall(workspace_dir)
    return workspace_dir


original_path = Path("./download/TheSkyBlessing.zip" if not (p := os.getenv("TEST_TSB_PATH")) else p)
workspace_path = create_workspace_from_zip(original_path) if not (p := os.getenv("WORKSPACE_TSB")) else p

snbt_cache_path = workspace_path / "block_entities.json"

TEXT_PATTERN = re.compile(r'(\\\\"|")((?:[^"\\]|\\.)*)\1')


def extract_datapack(datapack_path):
    with ZipFile(datapack_path) as zipf:
        root = ZipPath(zipf)
        for file in root.glob("**/*"):
            print(file)
            if file.is_dir() or (not (file.name.endswith(".json") or file.name.endswith(".mcfunction"))):
                continue
            with suppress(BadZipFile):
                text = file.read_text()
                for bounds, body in re.findall(TEXT_PATTERN, text):
                    if is_unicode(body):
                        yield body


class MakoBlockEntity(TypedDict):
    namespaced_name: str
    name: str
    namespace: str
    x: int
    y: int
    z: int
    snbt: str


def load_block_entities(tsb_path):
    load_world = partial(amulet.load_level, tsb_path)
    pre_chunk_coords = preload_chunk_coords(load_world)
    block_entities: list[BlockEntity] = []

    for dimension, chunk_coords in pre_chunk_coords.items():
        print(f"Dumping for {dimension}")
        index = 0
        for taken in chunked(chunk_coords, 1000):  # Avoid history manager
            print(f"Dumping sub: {index}/{math.ceil(len(chunk_coords) / 1000)}")
            world = load_world()
            for (cx, cy) in taken:
                chunk = world.get_chunk(cx, cy, dimension)
                block_entities.extend(chunk.block_entities)
            world.close()
            index += 1
    mako = list(unique_everseen([{
        "namespaced_name": be.namespaced_name,
        "name": be.base_name,
        "namespace": be.namespace,
        "x": be.x,
        "y": be.y,
        "z": be.z,
        "snbt": be.nbt.to_snbt()
    } for be in block_entities]))
    snbt_cache_path.write_text(json.dumps(mako, ensure_ascii=False))
    return mako


def extract_text_from_json(element) -> list[str]:
    if isinstance(element, list):
        return flatten_list(extract_text_from_json(e) for e in element)
    elif isinstance(element, dict):
        text = [element["text"]] if "text" in element else []
        extra = extract_text_from_json(element["extra"]) if "extra" in element else []
        return text + extra
    elif isinstance(element, str):
        return [element]
    else:
        return []


def extract_text(tag: AbstractBaseTag) -> list[str]:
    match tag.tag_id:
        case ListTag.tag_id:
            tag = cast(ListTag, tag)
            return flatten_list(extract_text(e) for e in tag.py_list)
        case CompoundTag.tag_id:
            tag = cast(CompoundTag, tag)
            return flatten_list(extract_text(v) for v in tag.values())
        case StringTag.tag_id:  # Minecraft saves text compound in nbt via json string
            tag = cast(StringTag, tag)
            data: str = tag.py_str
            data_strip = data.strip()
            if (data_strip.startswith("{") and data_strip.endswith("}")) or (
                    data_strip.startswith("[") and data_strip.endswith("]")):
                return extract_text_from_json(json.loads(data_strip))
            return [data]
        case _:
            return []


def extract_map(tsb_path):
    block_entities: list[MakoBlockEntity] = j if snbt_cache_path.exists() and (
        j := json.loads(snbt_cache_path.read_text())) else load_block_entities(tsb_path)
    return unique_everseen(
        t for t in flatten(extract_text(amulet_nbt.from_snbt(e["snbt"])) for e in block_entities) if is_unicode(t))


tsb_path = workspace_path / "TheSkyBlessing"
datapack_path = tsb_path / "datapacks"
dp_text_pool = set(
    text for dp in datapack_path.glob('*.zip') for text in extract_datapack(dp)
)
map_text_pool = set(extract_map(tsb_path))
text_pool = dp_text_pool | map_text_pool
Path("text_pool.json").write_text(json.dumps(text_pool, ensure_ascii=False))

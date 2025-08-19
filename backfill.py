import json
import math
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone, timedelta
from functools import partial
from pathlib import Path
from typing import cast

import amulet
import more_itertools
import zipfile
from amulet.api.data_types import ChunkCoordinates, Dimension
from amulet_nbt import AnyNBT, NamedTag, ListTag, CompoundTag, StringTag

from utils import fix_zip, preload_chunk_coords, is_unicode

original_path = "./download/TheSkyBlessing.zip" if  not (p := os.getenv("TEST_TSB_PATH")) else p

mapping = json.loads(Path("mapping.json").read_text())

TEXT_PATTERN = re.compile(r'(\\\\"|")((?:[^"\\]|\\.)*)\1')

def handle_dp(map_dir):
    datapacks_dir = map_dir / "datapacks"
    for zip_path in datapacks_dir.glob('**/*.zip'):
        print(f"Processing: {zip_path}")
        fix_zip(zip_path)  # Maybe the zip isn't so standard, e.g. Assets.zip and TheSkyBlessing.zip
        temp_zip_path = zip_path.with_name(f"temp_{zip_path.name}")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zin, zipfile.ZipFile(temp_zip_path, 'w') as zout:
                for item in zin.infolist():
                    with zin.open(item) as file:
                        if item.filename.endswith(('.json', '.mcfunction')):
                            content = file.read().decode('utf-8')
                            translated = content
                            for bounds, body in re.findall(TEXT_PATTERN, content):
                                if is_unicode(body) and body in mapping:
                                    translated = translated.replace(f'{bounds}{body}{bounds}',
                                                                    f'{bounds}{mapping[body]}{bounds}')
                            content = translated
                            zout.writestr(item, content.encode('utf-8'))
                        else:
                            content = file.read()
                            zout.writestr(item, content)

            temp_zip_path.replace(zip_path)
            print(f"Successfully updated: {zip_path}")
        except Exception as e:
            print(f"Error processing {zip_path}: {str(e)}")
            if temp_zip_path.exists():
                temp_zip_path.unlink()
            raise e



def translate_text_from_json(element):
    if isinstance(element, list):
        return [translate_text_from_json(e) for e in element]
    elif isinstance(element, dict):
        text = translate_text_from_json(element["text"]) if "text" in element else None
        extra = translate_text_from_json(element["extra"]) if "extra" in element else None
        return element | {k: v for k, v in [("text", text), ("extra", extra)] if v}
    elif isinstance(element, str):
        if element in mapping:
            translated = mapping[element]
            return translated
        return element
    else:
        return element  # Not translate


def translate_text(tag: AnyNBT | NamedTag) -> AnyNBT | NamedTag:
    if isinstance(tag, NamedTag):
        return NamedTag(translate_text(tag.tag), tag.name)

    match tag.tag_id:
        case ListTag.tag_id:
            tag = cast(ListTag, tag)
            return ListTag(translate_text(e) for e in tag.py_list)
        case CompoundTag.tag_id:
            tag = cast(CompoundTag, tag)
            return CompoundTag({key: translate_text(value) for key, value in tag.py_dict.items()})
        case StringTag.tag_id:  # Minecraft saves text compound in nbt via json string
            tag = cast(StringTag, tag)
            data: str = tag.py_str
            data_strip = data.strip()
            if (data_strip.startswith("{") and data_strip.endswith("}")) or (
                    data_strip.startswith("[") and data_strip.endswith("]")):
                return StringTag(json.dumps(translate_text_from_json(json.loads(data_strip)), ensure_ascii=False))
            return StringTag(mapping[data]) if data in mapping else tag
        case _:
            return tag


def handle_nbt(map_dir):
    load_world = partial(amulet.load_level, map_dir)
    pre_chunk_coords: dict[Dimension, list[ChunkCoordinates]] = preload_chunk_coords(load_world)
    for dimension, chunk_coords in pre_chunk_coords.items():
        print(f"Translate for {dimension}")
        index = 0
        for taken in more_itertools.chunked(chunk_coords, 1000):  # Avoid history manager
            print(f"Translate subchunk: {index}/{math.ceil(len(chunk_coords) / 1000)}")
            world = load_world()
            for (cx, cy) in taken:
                chunk = world.get_chunk(cx, cy, dimension)
                modified = False
                for coord, be in chunk.block_entities.items():
                    translated = translate_text(be.nbt)
                    if translated == be.nbt:
                        continue
                    print(f"Translate {be.nbt} => {translated}")
                    be.nbt = translated
                    modified = True
                if modified:
                    chunk.changed = True
            world.save()
            world.close()
            index += 1


def assemble_map(tsb_dir: Path):
    pack_dir = Path("./packing")
    packed_map = Path("./TheSkyBlessing_CN.zip")
    if pack_dir.exists():
        shutil.rmtree(pack_dir)
    shutil.copytree(tsb_dir, pack_dir)
    for attachment in os.listdir("./attachments"):
        print(attachment)
        shutil.copy2(f"./attachments/{attachment}", pack_dir / attachment)
    readme_p = (pack_dir / "README.md")
    with readme_p.open("a") as f:
        tz = timezone(timedelta(hours=8))
        now = datetime.now(tz)
        f.write(f"\n该包构建时间: {now}")
    shutil.make_archive("TheSkyBlessing_CN", "zip", pack_dir)
    shutil.rmtree(pack_dir)
    return packed_map


with zipfile.ZipFile(original_path) as izip:
    tsb_dir = Path("tsb")
    izip.extractall(tsb_dir)
    map_dir = tsb_dir / "TheSkyBlessing"
    raw_readme_fname = more_itertools.first_true(os.listdir(tsb_dir), pred=lambda it: it.endswith(".txt"))
    if raw_readme_fname:  # Encode as utf8
        readme_fname = raw_readme_fname.encode("cp437").decode("shift-jis")
        (tsb_dir / raw_readme_fname).replace(tsb_dir / readme_fname)
    print(f"{'Handle Datapacks':-^80}")
    handle_dp(map_dir)
    print(f"{'Handle Map NBTs':-^80}")
    handle_nbt(map_dir)  # time-consumingly
    print(f"{'Pack Translated Map':-^80}")
    packed_path = assemble_map(tsb_dir)
    print(f"Done({packed_path})")
    shutil.rmtree(tsb_dir)

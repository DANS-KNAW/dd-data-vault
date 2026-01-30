#!/usr/bin/env python3
#
# Copyright (C) 2024 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Validate OCFL extension registries in this repository:
- packaging-format-registry
- property-registry

It checks them against the DANS OCFL extensions docs in modules/dans-ocfl-extensions.
This script uses only the standard library and is intended for CI usage.
"""

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Default to this script's repo (dd-data-vault root)
REPO_ROOT = Path(__file__).resolve().parents[0]

SUPPORTED_ALGORITHMS = {"md5", "sha1", "sha256", "sha512"}

@dataclass
class ValidationError:
    path: Path
    message: str

    def __str__(self) -> str:
        return f"{self.path}: {self.message}"


def hexdigest(alg: str, data: bytes) -> str:
    alg_lower = alg.lower()
    if alg_lower not in SUPPORTED_ALGORITHMS:
        raise ValueError(f"Unsupported digest algorithm: {alg}")
    h = getattr(hashlib, alg_lower)()
    h.update(data)
    return h.hexdigest()


def validate_packaging_format_registry(path: Path) -> List[ValidationError]:
    errors: List[ValidationError] = []

    # Required files
    config_path = path / "config.json"
    inventory_path = path / "packaging_format_inventory.json"

    if not config_path.is_file():
        errors.append(ValidationError(config_path, "Missing config.json"))
        return errors
    if not inventory_path.is_file():
        errors.append(ValidationError(inventory_path, "Missing packaging_format_inventory.json"))
        return errors

    # Read config
    try:
        with config_path.open("r", encoding="utf-8") as f:
            config = json.load(f)
    except Exception as e:
        errors.append(ValidationError(config_path, f"Invalid JSON: {e}"))
        return errors

    # Validate config keys
    if config.get("extensionName") != "packaging-format-registry":
        errors.append(ValidationError(config_path, 'extensionName must be "packaging-format-registry"'))
    pf_alg = str(config.get("packagingFormatDigestAlgorithm", "md5")).lower()
    if pf_alg not in SUPPORTED_ALGORITHMS:
        errors.append(ValidationError(config_path, f"packagingFormatDigestAlgorithm must be one of {sorted(SUPPORTED_ALGORITHMS)}"))
    digest_alg = str(config.get("digestAlgorithm", "sha512")).lower()
    if digest_alg not in SUPPORTED_ALGORITHMS:
        errors.append(ValidationError(config_path, f"digestAlgorithm must be one of {sorted(SUPPORTED_ALGORITHMS)}"))

    # Sidecar filename depends on digest_alg
    sidecar_path = path / f"packaging_format_inventory.json.{digest_alg}"
    if not sidecar_path.is_file():
        errors.append(ValidationError(sidecar_path, f"Missing sidecar file for digest {digest_alg}"))
    else:
        try:
            sidecar_first_token = (sidecar_path.read_text(encoding="utf-8").strip().split() or [""])[0]
            actual_digest = hexdigest(digest_alg, inventory_path.read_bytes())
            if sidecar_first_token != actual_digest:
                errors.append(ValidationError(sidecar_path, f"Sidecar digest does not match inventory ({sidecar_first_token} != {actual_digest})"))
        except Exception as e:
            errors.append(ValidationError(sidecar_path, f"Error reading sidecar: {e}"))

    # Read inventory
    try:
        with inventory_path.open("r", encoding="utf-8") as f:
            inv = json.load(f)
    except Exception as e:
        errors.append(ValidationError(inventory_path, f"Invalid JSON: {e}"))
        return errors

    manifest = inv.get("manifest")
    if not isinstance(manifest, dict):
        errors.append(ValidationError(inventory_path, "Top-level key 'manifest' must be an object"))
        return errors

    # packaging_formats directory must exist
    pf_dir = path / "packaging_formats"
    if not pf_dir.is_dir():
        errors.append(ValidationError(pf_dir, "Missing packaging_formats directory"))
        return errors

    # Validate entries
    seen_pairs: Set[Tuple[str, str]] = set()
    for key, entry in manifest.items():
        entry_path = inventory_path
        if not isinstance(entry, dict):
            errors.append(ValidationError(entry_path, f"Manifest entry for key {key!r} must be an object"))
            continue
        name = entry.get("name")
        version = entry.get("version")
        summary = entry.get("summary")
        if not isinstance(name, str) or not isinstance(version, str):
            errors.append(ValidationError(entry_path, f"Manifest entry {key!r} must contain string 'name' and 'version'"))
            continue
        if (name, version) in seen_pairs:
            errors.append(ValidationError(entry_path, f"Duplicate name/version pair: {name}/{version}"))
        seen_pairs.add((name, version))
        if summary is None or not isinstance(summary, str):
            errors.append(ValidationError(entry_path, f"Manifest entry {key!r} must contain string 'summary'"))
        expected = hexdigest(pf_alg, f"{name}/{version}".encode("utf-8"))
        if key != expected:
            errors.append(ValidationError(entry_path, f"Manifest key {key} != {pf_alg}({name}/{version}) {expected}"))
        # Directory existence
        if not (pf_dir / key).is_dir():
            errors.append(ValidationError(pf_dir / key, f"Missing packaging_formats/{key} directory for {name}/{version}"))

    # One-to-one correspondence: no extra hashed directories
    dir_keys = sorted([p.name for p in pf_dir.iterdir() if p.is_dir()])
    manifest_keys = sorted(list(manifest.keys()))
    extra_dirs = sorted(list(set(dir_keys) - set(manifest_keys)))
    missing_dirs = sorted(list(set(manifest_keys) - set(dir_keys)))
    if extra_dirs:
        errors.append(ValidationError(pf_dir, f"Directories with no manifest entries: {', '.join(extra_dirs)}"))
    if missing_dirs:
        errors.append(ValidationError(pf_dir, f"Manifest entries without directories: {', '.join(missing_dirs)}"))

    return errors


def validate_property_registry(path: Path) -> List[ValidationError]:
    errors: List[ValidationError] = []

    config_path = path / "config.json"
    if not config_path.is_file():
        errors.append(ValidationError(config_path, "Missing config.json"))
        return errors

    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        errors.append(ValidationError(config_path, f"Invalid JSON: {e}"))
        return errors

    # Only allowed top-level keys per spec
    allowed_top_keys = {"extensionName", "propertyRegistry"}
    extra_top_keys = set(config.keys()) - allowed_top_keys
    if extra_top_keys:
        errors.append(ValidationError(config_path, f"Unexpected top-level keys: {', '.join(sorted(extra_top_keys))}"))

    if config.get("extensionName") != "property-registry":
        errors.append(ValidationError(config_path, 'extensionName must be "property-registry"'))

    registry = config.get("propertyRegistry")
    if not isinstance(registry, dict):
        errors.append(ValidationError(config_path, "propertyRegistry must be an object"))
        return errors

    allowed_types = {"number", "string", "boolean", "object"}

    for prop_name, desc in registry.items():
        if not isinstance(desc, dict):
            errors.append(ValidationError(config_path, f"Property '{prop_name}' description must be an object"))
            continue
        # Required keys
        if "description" not in desc or not isinstance(desc["description"], str):
            errors.append(ValidationError(config_path, f"Property '{prop_name}' must have string 'description'"))
        if "type" not in desc or not isinstance(desc["type"], str):
            errors.append(ValidationError(config_path, f"Property '{prop_name}' must have string 'type'"))
            continue
        typ = desc.get("type")
        if typ not in allowed_types:
            errors.append(ValidationError(config_path, f"Property '{prop_name}' type must be one of {sorted(allowed_types)}"))
        # Optional keys
        if "constraints" in desc and not isinstance(desc["constraints"], str):
            errors.append(ValidationError(config_path, f"Property '{prop_name}' optional 'constraints' must be a string if present"))
        if "required" in desc and not isinstance(desc["required"], bool):
            errors.append(ValidationError(config_path, f"Property '{prop_name}' optional 'required' must be a boolean if present"))
        if "default" in desc:
            # Cannot have default if required is true
            if desc.get("required") is True:
                errors.append(ValidationError(config_path, f"Property '{prop_name}' cannot specify 'default' when 'required' is true"))
        # Object type must have 'properties' as object (mapping)
        if typ == "object":
            if "properties" not in desc or not isinstance(desc["properties"], dict):
                errors.append(ValidationError(config_path, f"Property '{prop_name}' of type object must have 'properties' object mapping"))

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate OCFL extension registries (packaging-format-registry, property-registry)")
    parser.add_argument("--ocfl-extensions-root", dest="ocfl_root", default=None, help="Override path to ocfl-root-extensions directory")
    args = parser.parse_args()

    # Determine ocfl-root-extensions base directory
    if args.ocfl_root:
        ocfl_root = Path(args.ocfl_root)
    else:
        ocfl_root = REPO_ROOT / "src/main/assembly/dist/cfg/ocfl-root-extensions"

    if not ocfl_root.is_dir():
        print("OCFL extension validation: FAIL")
        print(" - Could not locate ocfl-root-extensions directory at:")
        print(f"   - {ocfl_root}")
        print("   You can pass --ocfl-extensions-root to override.")
        return 1

    packaging_format_registry_path = ocfl_root / "packaging-format-registry"
    property_registry_path = ocfl_root / "property-registry"

    pack_errors = validate_packaging_format_registry(packaging_format_registry_path)
    prop_errors = validate_property_registry(property_registry_path)

    all_errors = pack_errors + prop_errors
    if all_errors:
        print("OCFL extension validation: FAIL")
        for e in all_errors:
            print(f" - {e}")
        return 1
    print("OCFL extension validation: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())

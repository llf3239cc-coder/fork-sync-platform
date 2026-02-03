#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stage 1: Hard filters (cheap, high precision)

Quickly reject changes not suitable for sync:
- Large refactors (configurable threshold)
- Vendor/dependency-only updates
"""

import re
from pathlib import Path
from typing import Dict, List, Any, Optional

from syncability_common import CandidateItem


class Stage1HardFilters:
    """Stage 1: Hard filters"""

    def __init__(self, work_dir: Path, config: Dict[str, Any]):
        """
        Initialize.

        Args:
            work_dir: Work directory
            config: Config dict
        """
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.config = config
        
        # Get vendor/dependency patterns from config, else use defaults
        self.vendor_patterns = config.get("vendor_dependency_patterns", None)
        
        self.large_refactor_threshold = config.get("large_refactor_threshold", 50)
    
    def check_large_refactor(self, files: List[str]) -> tuple[bool, str]:
        """
        Check if large refactor.

        Returns:
            (reject, reason)
        """
        if len(files) > self.large_refactor_threshold:
            return True, f"Large refactor: {len(files)} files (threshold: {self.large_refactor_threshold})"
        return False, ""
    
    def check_vendor_dependency_only(self, files: List[str], commit_message: str) -> tuple[bool, str]:
        """
        Check if vendor/dependency only.

        Returns:
            (reject, reason)
        """
        # Use config patterns or defaults
        if self.vendor_patterns is not None:
            vendor_patterns = self.vendor_patterns
        else:
            vendor_patterns = [
            # Common vendor/dependency dirs
            r"vendor/",
            r"third_party/",
            r"third-party/",
            r"dependencies/",
            r"deps/",
            r"external/",
            r"externals/",
            r"submodules/",
            r"submodule/",
            r"libs/",
            r"lib/",
            r"modules/",
            r"contrib/",
            r"contribs/",
            
            # C/C++ dependency files
            r"CMakeLists\.txt$",  # CMake
            r"cmake/.*\.cmake$",
            r"CMakeCache\.txt$",
            r"Makefile$",
            r"makefile$",
            r"GNUmakefile$",
            r"configure\.ac$",  # Autotools
            r"configure\.in$",
            r"aclocal\.m4$",
            r"Makefile\.am$",
            r"Makefile\.in$",
            r"conanfile\.txt$",  # Conan
            r"conanfile\.py$",
            r"conan\.lock$",
            r"vcpkg\.json$",  # vcpkg
            r"vcpkg\.lock$",
            r"meson\.build$",  # Meson
            r"meson_options\.txt$",
            r"premake5\.lua$",  # Premake
            r"premake4\.lua$",
            r"BUILD$",  # Bazel
            r"BUILD\.bazel$",
            r"WORKSPACE$",
            r"WORKSPACE\.bazel$",
            r"\.bazelrc$",
            r"\.clang_complete$",  # Clang
            r"compile_commands\.json$",
            r"\.clangd$",
            r"hunter\.cmake$",  # Hunter
            r"CPM\.cmake$",  # CPM
            r"biicode\.conf$",  # Biicode
            r"buckaroo\.json$",  # Buckaroo
            r"BUCK$",  # Buck
            r"Pants$",  # Pants
            r"build\.gradle$",  # Android NDK / Gradle
            r"build\.gradle\.kts$",
            r"settings\.gradle$",
            r"settings\.gradle\.kts$",
            
            # Go
            r"go\.mod$",
            r"go\.sum$",
            r"Gopkg\.toml$",  # dep
            r"Gopkg\.lock$",
            r"glide\.yaml$",  # Glide
            r"glide\.lock$",
            
            # Node.js / JavaScript
            r"package\.json$",
            r"package-lock\.json$",
            r"yarn\.lock$",
            r"pnpm-lock\.yaml$",
            r"\.npmrc$",
            r"\.yarnrc$",
            r"\.yarnrc\.yml$",
            r"bower\.json$",  # Bower
            r"bower_components/",
            r"node_modules/",
            
            # Python
            r"requirements\.txt$",
            r"requirements.*\.txt$",
            r"setup\.py$",
            r"setup\.cfg$",
            r"pyproject\.toml$",
            r"Pipfile$",
            r"Pipfile\.lock$",
            r"poetry\.lock$",  # Poetry
            r"conda\.yml$",  # Conda
            r"environment\.yml$",
            r"conda-requirements\.txt$",
            r"\.python-version$",
            
            # Java / JVM
            r"pom\.xml$",  # Maven
            r"\.mvn/",
            r"build\.gradle$",
            r"build\.gradle\.kts$",
            r"ivy\.xml$",  # Ivy
            r"build\.sbt$",  # SBT
            r"project/.*\.scala$",
            r"project/.*\.sbt$",
            
            # Ruby
            r"Gemfile$",
            r"Gemfile\.lock$",
            r"\.gemspec$",
            r"Rakefile$",
            
            # Rust
            r"Cargo\.toml$",
            r"Cargo\.lock$",
            
            # PHP
            r"composer\.json$",
            r"composer\.lock$",
            
            # .NET / C# / F# / VB
            r"\.csproj$",
            r"\.vbproj$",
            r"\.fsproj$",
            r"packages\.config$",
            r"project\.json$",
            r"project\.lock\.json$",
            r"paket\.dependencies$",  # Paket
            r"paket\.lock$",
            r"\.sln$",  # Solution file
            r"\.xproj$",
            
            # Swift / Objective-C
            r"Package\.swift$",
            r"Podfile$",  # CocoaPods
            r"Podfile\.lock$",
            r"\.podspec$",
            r"Cartfile$",  # Carthage
            r"Cartfile\.resolved$",
            
            # Scala
            r"build\.sbt$",
            r"project/.*\.scala$",
            
            # Haskell
            r"\.cabal$",
            r"stack\.yaml$",
            r"cabal\.project$",
            
            # Elixir / Erlang
            r"mix\.exs$",
            r"mix\.lock$",
            r"rebar\.config$",
            r"rebar\.lock$",
            
            # Lua
            r"\.rockspec$",
            
            # R
            r"DESCRIPTION$",
            
            # Julia
            r"Project\.toml$",
            r"Manifest\.toml$",
            
            # Nim
            r"\.nimble$",
            
            # D
            r"dub\.json$",
            r"dub\.sdl$",
            
            # OCaml
            r"opam$",
            r"dune-project$",
            r"dune-workspace$",
            
            # Clojure
            r"project\.clj$",
            r"deps\.edn$",
            
            # Dart / Flutter
            r"pubspec\.yaml$",
            r"pubspec\.lock$",
            
            # Kotlin
            r"build\.gradle\.kts$",
            
            # Groovy
            r"build\.gradle$",
            
            # Maven Wrapper
            r"mvnw$",
            r"mvnw\.cmd$",
            r"\.mvn/wrapper/",
            
            # Gradle Wrapper
            r"gradlew$",
            r"gradlew\.bat$",
            r"gradle/wrapper/",
            
            # Other build tools
            r"\.travis\.yml$",
            r"\.circleci/",
            r"\.github/workflows/",
            ]
        
        vendor_files = []
        for file_path in files:
            for pattern in vendor_patterns:
                if re.search(pattern, file_path, re.IGNORECASE):
                    vendor_files.append(file_path)
                    break
        
        # If all files are vendor/dependency
        if len(vendor_files) == len(files) and len(files) > 0:
            return True, f"Vendor/dependency only: {len(files)} files"
        
        return False, ""
    
    def process(self, candidates: List[CandidateItem]) -> List[CandidateItem]:
        """
        Process candidates, apply hard filters.
        Returns: candidates passing Stage 1
        """
        passed_candidates = []
        
        for candidate in candidates:
            result = {
                "passed": True,
                "reasons": [],
                "checks": {},
            }
            
            # Get file list and message
            files = []
            message = ""
            
            if candidate.item_type == "commit":
                files = candidate.raw_data.get("files", [])
                message = candidate.raw_data.get("message", "")
            elif candidate.item_type == "pr":
                # Get files from PR
                files = candidate.raw_data.get("files", [])
                if not files:
                    # Try diff or other fields
                    pass
                message = f"{candidate.raw_data.get('title', '')} {candidate.raw_data.get('body', '')}"
            
            # Check 1: large refactor
            rejected, reason = self.check_large_refactor(files)
            result["checks"]["large_refactor"] = not rejected
            if rejected:
                result["passed"] = False
                result["reasons"].append(reason)
            
            # Check 2: vendor/dependency only
            rejected, reason = self.check_vendor_dependency_only(files, message)
            result["checks"]["vendor_dependency_only"] = not rejected
            if rejected:
                result["passed"] = False
                result["reasons"].append(reason)
            
            # Save result
            candidate.stage1_result = result
            
            # Keep only passed candidates
            if result["passed"]:
                passed_candidates.append(candidate)
        
        return passed_candidates


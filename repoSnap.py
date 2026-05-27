from pathlib import Path
from datetime import datetime, timezone
import hashlib
import mimetypes
import os


# IMPORTANT:
# Root is wherever this script is RAN FROM.
# Not where this script file is located.
ROOT = Path.cwd().resolve()

CHUNK_COUNT = 6
OUTPUT_PREFIX = "repo_snapshot_part_"
OUTPUT_SUFFIX = ".md"
MAX_TOTAL_FILES = 2000              # hard cap
MAX_TOTAL_INDEX_BYTES = 300 * 1024 * 1024   # 300MB cap of files indexed
SKIP_HUGE_DIRS = True
MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024

EXCLUDED_DIRS = {
    ".git",
    ".md",
    ".hg",
    ".svn",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".idea",
    ".vscode",
    "node_modules",
    ".venv",
    "venv",
    "env",
    "dist",
    "build",
    "coverage",
    ".next",
    ".nuxt",
    ".turbo",
    ".cache",
    "target",
    "bin",
    "obj",
    ".txt",
    ".pyc",
    "data",
    "datasets",
    "dataset",
    "storage",
    "logs",
    "log",
    "artifacts",
    "runs",
    "outputs",
    "snapshots",
    "cache",
    "tmp",
    "temp",
    ".tmp",
    ".temp",
    "models",
    "weights",
    "checkpoints",
}

EXCLUDED_FILES = {
    ".DS_Store",
    "Thumbs.db",
}

TEXT_EXTENSIONS = {
    ".py",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".mjs",
    ".cjs",
    ".json",
    ".md",
    ".mdx",
    ".txt",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".css",
    ".scss",
    ".sass",
    ".less",
    ".html",
    ".htm",
    ".xml",
    ".svg",
    ".sql",
    ".sh",
    ".bash",
    ".zsh",
    ".ps1",
    ".bat",
    ".cmd",
    ".java",
    ".kt",
    ".kts",
    ".cs",
    ".cpp",
    ".c",
    ".h",
    ".hpp",
    ".go",
    ".rs",
    ".php",
    ".rb",
    ".swift",
    ".dart",
    ".vue",
    ".svelte",
}

TEXT_FILENAMES = {
    ".env",
    ".env.local",
    ".env.development",
    ".env.production",
    ".env.test",
    ".gitignore",
    ".gitattributes",
    ".editorconfig",
    ".prettierrc",
    ".eslintrc",
    ".babelrc",
    "Dockerfile",
    "dockerfile",
    "Makefile",
    "makefile",
    "README",
    "LICENSE",
    "CHANGELOG",
    "requirements.txt",
    "Pipfile",
    "pyproject.toml",
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "tsconfig.json",
    "vite.config.ts",
    "vite.config.js",
    "next.config.js",
    "next.config.ts",
}


def output_file_names() -> set[str]:
    names = set()

    for index in range(1, CHUNK_COUNT + 1):
        names.add(f"{OUTPUT_PREFIX}{index}{OUTPUT_SUFFIX}")

    return names


def should_skip_path(path: Path) -> bool:
    if path.name in EXCLUDED_FILES:
        return True

    if path.name in output_file_names():
        return True

    if path.name == Path(__file__).name:
        return True

    try:
        relative_parts = path.relative_to(ROOT).parts
    except ValueError:
        relative_parts = path.parts

    for part in relative_parts:
        if part in EXCLUDED_DIRS:
            return True

    return False


def is_probably_text_file(path: Path) -> bool:
    if path.name in TEXT_FILENAMES:
        return True

    if path.suffix.lower() in TEXT_EXTENSIONS:
        return True

    guessed_type, _ = mimetypes.guess_type(str(path))

    if guessed_type and guessed_type.startswith("text/"):
        return True

    try:
        with path.open("rb") as file:
            chunk = file.read(4096)

        if b"\x00" in chunk:
            return False

        chunk.decode("utf-8")
        return True

    except Exception:
        return False


def markdown_language(path: Path) -> str:
    name = path.name
    suffix = path.suffix.lower()

    if name in {"Dockerfile", "dockerfile"}:
        return "dockerfile"

    if name in {"Makefile", "makefile"}:
        return "makefile"

    if name.startswith(".env"):
        return "bash"

    language_map = {
        ".py": "python",
        ".js": "javascript",
        ".jsx": "jsx",
        ".ts": "typescript",
        ".tsx": "tsx",
        ".mjs": "javascript",
        ".cjs": "javascript",
        ".json": "json",
        ".md": "markdown",
        ".mdx": "mdx",
        ".yml": "yaml",
        ".yaml": "yaml",
        ".toml": "toml",
        ".ini": "ini",
        ".cfg": "ini",
        ".conf": "text",
        ".css": "css",
        ".scss": "scss",
        ".sass": "sass",
        ".less": "less",
        ".html": "html",
        ".htm": "html",
        ".xml": "xml",
        ".svg": "xml",
        ".sql": "sql",
        ".sh": "bash",
        ".bash": "bash",
        ".zsh": "bash",
        ".ps1": "powershell",
        ".bat": "batch",
        ".cmd": "batch",
        ".java": "java",
        ".kt": "kotlin",
        ".kts": "kotlin",
        ".cs": "csharp",
        ".cpp": "cpp",
        ".c": "c",
        ".h": "c",
        ".hpp": "cpp",
        ".go": "go",
        ".rs": "rust",
        ".php": "php",
        ".rb": "ruby",
        ".swift": "swift",
        ".dart": "dart",
        ".vue": "vue",
        ".svelte": "svelte",
    }

    return language_map.get(suffix, "text")



def safe_code_fence(content: str) -> str:
    longest = 0
    current = 0

    for char in content:
        if char == "`":
            current += 1
            longest = max(longest, current)
        else:
            current = 0

    return "`" * max(3, longest + 1)


def collect_files() -> list[Path]:
    files = []
    total_size = 0

    for current_root, dir_names, file_names in os.walk(ROOT):
        current_path = Path(current_root)

        # Filter directories early (this is the biggest speed win)
        kept_dirs = []
        for dir_name in dir_names:
            dir_path = current_path / dir_name
            if should_skip_path(dir_path):
                continue
            kept_dirs.append(dir_name)
        dir_names[:] = kept_dirs

        # Process files
        for file_name in file_names:
            file_path = current_path / file_name

            if should_skip_path(file_path):
                continue

            if not file_path.is_file():
                continue

            # optional: only include text-like files
            if not is_probably_text_file(file_path):
                continue

            try:
                size = file_path.stat().st_size
            except Exception:
                size = 0

            # skip massive files early
            if size > MAX_FILE_SIZE_BYTES:
                continue

            files.append(file_path)
            total_size += size

            # hard stop conditions
            if len(files) >= MAX_TOTAL_FILES:
                print(f"[STOP] Hit MAX_TOTAL_FILES={MAX_TOTAL_FILES}")
                break

            if total_size >= MAX_TOTAL_INDEX_BYTES:
                print(f"[STOP] Hit MAX_TOTAL_INDEX_BYTES={MAX_TOTAL_INDEX_BYTES}")
                break

        if len(files) >= MAX_TOTAL_FILES or total_size >= MAX_TOTAL_INDEX_BYTES:
            break

    files.sort(key=lambda path: path.relative_to(ROOT).as_posix().lower())
    return files


def project_tree_text(files: list[Path]) -> str:
    lines = []

    for file_path in files:
        lines.append(file_path.relative_to(ROOT).as_posix())

    return "\n".join(lines)


def file_block(file_path: Path) -> str:
    relative_path = file_path.relative_to(ROOT).as_posix()
    absolute_path = file_path.resolve()
    size = file_path.stat().st_size

    guessed_type, guessed_encoding = mimetypes.guess_type(str(file_path))
    guessed_type = guessed_type or "unknown"
    guessed_encoding = guessed_encoding or "unknown"

    parts = []

    parts.append("\n---\n\n")
    parts.append(f"## FILE: `{relative_path}`\n\n")


    if size > MAX_FILE_SIZE_BYTES:
        parts.append(
            f"> Skipped content because file is too large: "
            f"{size} bytes > {MAX_FILE_SIZE_BYTES} bytes.\n"
        )
        return "".join(parts)

    if not is_probably_text_file(file_path):
        parts.append("> Binary/non-text file detected. Content not copied.\n")
        return "".join(parts)

    try:
        content = file_path.read_text(encoding="utf-8", errors="replace")
        language = markdown_language(file_path)
        fence = safe_code_fence(content)

        parts.append(f"{fence}{language}\n")
        parts.append(content)

        if not content.endswith("\n"):
            parts.append("\n")

        parts.append(f"{fence}\n")

    except Exception as exc:
        parts.append(f"> Error reading file: `{exc}`\n")

    return "".join(parts)


def split_files_into_chunks(files: list[Path], chunk_count: int) -> list[list[Path]]:
    chunks = [[] for _ in range(chunk_count)]
    chunk_sizes = [0 for _ in range(chunk_count)]

    for file_path in files:
        try:
            size = file_path.stat().st_size
        except Exception:
            size = 0

        smallest_chunk_index = chunk_sizes.index(min(chunk_sizes))
        chunks[smallest_chunk_index].append(file_path)
        chunk_sizes[smallest_chunk_index] += size

    return chunks


def write_chunk(
    chunk_index: int,
    total_chunks: int,
    chunk_files: list[Path],
    all_files: list[Path],
    created_at: str,
) -> Path:
    output_path = ROOT / f"{OUTPUT_PREFIX}{chunk_index}{OUTPUT_SUFFIX}"

    with output_path.open("w", encoding="utf-8", newline="\n") as output:
        output.write(f"# Repository Snapshot - Part {chunk_index} of {total_chunks}\n\n")
        output.write(f"- Root folder: `{ROOT}`\n")
        output.write(f"- You know my wholle Jinjnibacktester simulator thign whre ther is a UI bascially and then i can see  charst and stuff when i need to run simulatiosn liek i send simulatio nto my flask backend server it runs sims and then shows stast and stuff and i can load strategy and shit for now take a look we will be doing bug fixes and some validation and shit. udnerrtsnad each code and its role how it works and keep in ir conetxt i will ask u exactly wha tto do later code later duinerstood\n")
        output.write(f"- Total files indexed: `{len(all_files)}`\n")
        output.write(f"- Files in this chunk: `{len(chunk_files)}`\n")


        output.write("## Full Project Tree\n\n")
        output.write("```text\n")
        output.write(project_tree_text(all_files))
        output.write("\n```\n\n")

        output.write(f"## Files In This Chunk - Part {chunk_index}\n\n")
        output.write("```text\n")

        for file_path in chunk_files:
            output.write(file_path.relative_to(ROOT).as_posix())
            output.write("\n")

        output.write("```\n\n")

        output.write("## File Contents\n\n")

        for file_path in chunk_files:
            output.write(file_block(file_path))

    return output_path


def delete_old_outputs() -> None:
    for index in range(1, CHUNK_COUNT + 1):
        output_path = ROOT / f"{OUTPUT_PREFIX}{index}{OUTPUT_SUFFIX}"

        if output_path.exists():
            output_path.unlink()


def main() -> None:
    delete_old_outputs()

    files = collect_files()
    chunks = split_files_into_chunks(files, CHUNK_COUNT)
    created_at = datetime.now(timezone.utc).isoformat()

    output_paths = []

    for index, chunk_files in enumerate(chunks, start=1):
        output_path = write_chunk(
            chunk_index=index,
            total_chunks=CHUNK_COUNT,
            chunk_files=chunk_files,
            all_files=files,
            created_at=created_at,
        )

        output_paths.append(output_path)

    print("")
    print("Snapshot chunks created successfully.")
    print(f"Root used: {ROOT}")
    print(f"Total files indexed: {len(files)}")
    print("")
    print("Created files:")

    for output_path in output_paths:
        size = output_path.stat().st_size
        print(f"- {output_path.name} ({size} bytes)")

    print("")
    print("Upload these 4 files here:")
    for output_path in output_paths:
        print(f"- {output_path.name}")


if __name__ == "__main__":
    main()
# evernote2notion

Migrate your Evernote notes into a Notion database — with full fidelity.

- Converts Evernote's HTML export (higher quality than ENEX) into native Notion blocks
- Uploads images, PDFs, and other file attachments using Notion's file upload API
- Resumes automatically if interrupted; skips already-imported notes
- Handles Notion API quirks: rate limits, payload size limits, WAF-blocked content

## Installation

```bash
pipx install evernote2notion
```

Or run without installing:

```bash
pipx run evernote2notion --export-dir my-export --parent-page-id <PAGE_ID>
```

## Setup

### 1. Export your notes from Evernote

In Evernote, select all notes → **File → Export Notes → HTML**. You'll get a folder of year-named subfolders:

```
my-export/
  2018/
  2019/
  ...
  2025/
```

### 2. Create a Notion integration

1. Go to <https://www.notion.so/my-integrations> and create a new **Internal** integration.
2. Copy the **Internal Integration Secret** (starts with `secret_`).

### 3. Share a Notion page with the integration

1. In Notion, open (or create) the page that will contain your imported notes.
2. Click **"…"** → **"Add connections"** and select your integration.
3. Copy the **page ID** from the URL — it's the 32-character hex string at the end:
   ```
   https://www.notion.so/My-Page-3a8f2b1c4d5e6f7a8b9c0d1e2f3a4b5c
                                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
   ```

## Usage

```bash
export NOTION_TOKEN=secret_xxx

# Test: parse notes and report block counts without uploading
evernote2notion --export-dir my-export --dry-run

# Import a small batch first
evernote2notion --export-dir my-export --parent-page-id <PAGE_ID> --limit 10

# Import a single year
evernote2notion --export-dir my-export --parent-page-id <PAGE_ID> --year 2022

# Import everything
evernote2notion --export-dir my-export --parent-page-id <PAGE_ID>

# Remove duplicate pages (if you re-imported notes)
evernote2notion --parent-page-id <PAGE_ID> --deduplicate
```

Progress is saved to `import_progress.json` in the current directory after every note. If the run is interrupted, re-run the same command and it picks up where it left off.

## What gets imported

Each note becomes a page in an **"Evernote Notes"** Notion database:

| Property | Type | Description |
|---|---|---|
| Name | Title | Note title |
| Year | Select | Year the note was created |
| Created | Date | Original creation timestamp |
| Updated | Date | Last updated timestamp |
| Tags | Multi-select | Evernote tags |
| Source URL | URL | Original URL (for web clips) |
| Source | Select | Source app (`web.clip`, `mobile`, etc.) |

Note content is converted to native Notion blocks: paragraphs, headings, bullet/numbered lists, checkboxes, code blocks, tables, quotes, dividers, images, PDFs, and other file attachments.

## License

MIT

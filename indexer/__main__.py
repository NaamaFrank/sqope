import typer
from pathlib import Path
# from typing import Optional, List
# from indexer.parse_pdf import parse_pdf
from indexer.ingest import upsert_document

app = typer.Typer(help="Sqope Indexer CLI")

def _validate_file(p: str) -> Path:
    path = Path(p)
    if not path.exists():
        raise typer.BadParameter(f"File not found: {p}")
    if not path.is_file():
        raise typer.BadParameter(f"Not a file: {p}")
    return path

@app.command("file")
def file_cmd(
    path: str = typer.Option(..., "--path", "-p", help="Path to a single PDF file"),
):
    pdf_path = _validate_file(path)
    # meta, paragraphs, tables = parse_pdf(str(pdf_path))
    meta = {"filepath": pdf_path}
    n_chunks = upsert_document(meta)    
    
    typer.echo(f"Indexed {pdf_path} → {n_chunks} hybrid chunks")


# @app.command("batch")
# def batch_cmd(
#     paths: List[str] = typer.Option(..., "--path", "-p", help="One or more PDF paths or globs", multiple=True),
#     prefix: Optional[str] = typer.Option(None, "--prefix", "-x", help="Prefix to add to auto-generated doc IDs"),
#     recursive: bool = typer.Option(False, "--recursive", "-r", help="Recurse into directories"),
# ):
#     files: List[Path] = []
#     for p in paths:
#         pp = Path(p)
#         if pp.is_dir():
#             it = pp.rglob("*.pdf") if recursive else pp.glob("*.pdf")
#             files.extend(list(it))
#         else:
#             # allow globs like *.pdf
#             files.extend(list(pp.parent.glob(pp.name)))
#
#     seen = set()
#     for f in files:
#         if f in seen:
#             continue
#         seen.add(f)
#         doc_id = (prefix + "-" if prefix else "") + f.stem
#         meta, paragraphs, tables = parse_pdf(str(f), doc_id)
#         upsert_document(doc_id, meta, paragraphs, tables)
#         typer.echo(f"Indexed {f} (id={doc_id})")

if __name__ == "__main__":
    app()

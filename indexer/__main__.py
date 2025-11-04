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
    meta = {"filepath": pdf_path}
    n_chunks = upsert_document(meta)    
    
    typer.echo(f"Indexed {pdf_path} → {n_chunks} hybrid chunks")


if __name__ == "__main__":
    app()

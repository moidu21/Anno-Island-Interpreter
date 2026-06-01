from __future__ import annotations

import os
import queue
import threading
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from island_interpreter import (
    APP_TITLE,
    DECODE_MODE,
    ENCODE_MODE,
    InterpreterConfig,
    IslandInterpreter,
    ResourceResolver,
    InterpreterError,
)


LANGUAGES = ("FR", "EN", "GER")

TRANSLATIONS: dict[str, dict[str, str]] = {
    "FR": {
        "language": "Langue",
        "source_folder": "Dossier source",
        "output_folder": "Dossier de sortie",
        "browse": "Parcourir",
        "mode": "Mode",
        "decode": "Décoder",
        "encode": "Recoder",
        "exclude_river": 'Décodage : retirer tout fichier/dossier contenant "river"',
        "start": "Lancer",
        "clear_log": "Effacer le journal",
        "log": "Journal",
        "choose_interpreted_folder": "Choisir le dossier interprété à recoder",
        "choose_source_island": "Choisir le dossier de l'île source",
        "choose_output_folder": "Choisir le dossier de sortie",
        "already_running": "Un traitement est déjà en cours.",
        "choose_valid_source": "Choisissez un dossier source valide.",
        "choose_valid_output": "Choisissez un dossier de sortie valide.",
        "decode_action_lower": "décodage",
        "encode_action_lower": "recodage",
        "decode_action": "Décodage",
        "encode_action": "Recodage",
        "starting": "Démarrage du {action}...",
        "unexpected_error_log": "ERREUR INATTENDUE: {error}",
        "unexpected_error_box": "Erreur inattendue:\n{error}",
        "error_log": "ERREUR: {error}",
        "finished_log": "{action} terminé. Rapport: {report_path}",
        "finished_box": "{action} terminé.\n\nRapport:\n{report_path}",
    },
    "EN": {
        "language": "Language",
        "source_folder": "Source folder",
        "output_folder": "Output folder",
        "browse": "Browse",
        "mode": "Mode",
        "decode": "Decode",
        "encode": "Encode",
        "exclude_river": 'Decode: remove every file/folder containing "river"',
        "start": "Start",
        "clear_log": "Clear log",
        "log": "Log",
        "choose_interpreted_folder": "Choose the interpreted folder to encode",
        "choose_source_island": "Choose the source island folder",
        "choose_output_folder": "Choose the output folder",
        "already_running": "A process is already running.",
        "choose_valid_source": "Choose a valid source folder.",
        "choose_valid_output": "Choose a valid output folder.",
        "decode_action_lower": "decoding",
        "encode_action_lower": "encoding",
        "decode_action": "Decoding",
        "encode_action": "Encoding",
        "starting": "Starting {action}...",
        "unexpected_error_log": "UNEXPECTED ERROR: {error}",
        "unexpected_error_box": "Unexpected error:\n{error}",
        "error_log": "ERROR: {error}",
        "finished_log": "{action} finished. Report: {report_path}",
        "finished_box": "{action} finished.\n\nReport:\n{report_path}",
    },
    "GER": {
        "language": "Sprache",
        "source_folder": "Quellordner",
        "output_folder": "Ausgabeordner",
        "browse": "Durchsuchen",
        "mode": "Modus",
        "decode": "Dekodieren",
        "encode": "Kodieren",
        "exclude_river": 'Dekodierung: alle Dateien/Ordner mit "river" entfernen',
        "start": "Starten",
        "clear_log": "Protokoll löschen",
        "log": "Protokoll",
        "choose_interpreted_folder": "Interpretierten Ordner zum Kodieren auswählen",
        "choose_source_island": "Quellordner der Insel auswählen",
        "choose_output_folder": "Ausgabeordner auswählen",
        "already_running": "Ein Vorgang läuft bereits.",
        "choose_valid_source": "Wählen Sie einen gültigen Quellordner.",
        "choose_valid_output": "Wählen Sie einen gültigen Ausgabeordner.",
        "decode_action_lower": "Dekodierung",
        "encode_action_lower": "Kodierung",
        "decode_action": "Dekodierung",
        "encode_action": "Kodierung",
        "starting": "Starte {action}...",
        "unexpected_error_log": "UNERWARTETER FEHLER: {error}",
        "unexpected_error_box": "Unerwarteter Fehler:\n{error}",
        "error_log": "FEHLER: {error}",
        "finished_log": "{action} abgeschlossen. Bericht: {report_path}",
        "finished_box": "{action} abgeschlossen.\n\nBericht:\n{report_path}",
    },
}


class IslandInterpreterApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("860x560")
        self.resources = ResourceResolver()
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.worker: threading.Thread | None = None

        home_docs = (Path.home() / "Documents").resolve() if (Path.home() / "Documents").exists() else Path.home().resolve()
        self.language_var = tk.StringVar(value="EN")
        self.source_var = tk.StringVar()
        self.output_var = tk.StringVar(value=str(home_docs))
        self.mode_var = tk.StringVar(value=DECODE_MODE)
        self.exclude_river_var = tk.BooleanVar(value=False)
        # Paramètres internes permanents : pas exposés dans l'interface.
        # Utilise jusqu'à 32 threads si le CPU les fournit, sinon le nombre de threads CPU disponibles.
        self.max_workers = max(1, min(os.cpu_count() or 1, 32))
        self.detailed_log = False
        self.fast_zip = True

        self.i18n_widgets: dict[str, tuple[tk.Widget, str]] = {}
        self.path_rows: list[tuple[ttk.Label, str, ttk.Button]] = []
        self._build_ui()
        self._apply_language()
        self._pump_log_queue()

    def tr(self, key: str) -> str:
        return TRANSLATIONS.get(self.language_var.get(), TRANSLATIONS["EN"]).get(key, key)

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill="both", expand=True)
        main.columnconfigure(1, weight=1)

        self.language_label = ttk.Label(main)
        self.language_label.grid(row=0, column=0, sticky="w", pady=(4, 4))
        self.language_combo = ttk.Combobox(main, textvariable=self.language_var, values=LANGUAGES, state="readonly", width=10)
        self.language_combo.grid(row=0, column=1, sticky="w", pady=(4, 4))
        self.language_combo.bind("<<ComboboxSelected>>", lambda _event: self._apply_language())

        self._add_path_row(main, 1, "source_folder", self.source_var, self._browse_source)
        self._add_path_row(main, 2, "output_folder", self.output_var, self._browse_output)

        self.mode_frame = ttk.LabelFrame(main)
        self.mode_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(10, 10))
        self.decode_radio = ttk.Radiobutton(self.mode_frame, value=DECODE_MODE, variable=self.mode_var, command=self._sync_mode_ui)
        self.decode_radio.grid(row=0, column=0, sticky="w", padx=8, pady=8)
        self.encode_radio = ttk.Radiobutton(self.mode_frame, value=ENCODE_MODE, variable=self.mode_var, command=self._sync_mode_ui)
        self.encode_radio.grid(row=0, column=1, sticky="w", padx=8, pady=8)
        self.exclude_river_check = ttk.Checkbutton(
            self.mode_frame,
            variable=self.exclude_river_var,
        )
        self.exclude_river_check.grid(row=1, column=0, columnspan=2, sticky="w", padx=8, pady=(0, 8))

        actions = ttk.Frame(main)
        actions.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(0, 8))
        self.start_button = ttk.Button(actions, command=self._start)
        self.start_button.pack(side="left")
        self.clear_log_button = ttk.Button(actions, command=self._clear_log)
        self.clear_log_button.pack(side="left", padx=(8, 0))

        self.log_label = ttk.Label(main)
        self.log_label.grid(row=5, column=0, sticky="w")
        self.log_widget = tk.Text(main, wrap="word", height=23)
        self.log_widget.grid(row=6, column=0, columnspan=3, sticky="nsew")
        scrollbar = ttk.Scrollbar(main, orient="vertical", command=self.log_widget.yview)
        scrollbar.grid(row=6, column=3, sticky="ns")
        self.log_widget.configure(yscrollcommand=scrollbar.set)
        main.rowconfigure(6, weight=1)
        self._sync_mode_ui()

    def _add_path_row(self, parent: ttk.Frame, row: int, label_key: str, variable: tk.StringVar, browse_command) -> None:
        label = ttk.Label(parent)
        label.grid(row=row, column=0, sticky="w", pady=(4, 4))
        ttk.Entry(parent, textvariable=variable).grid(row=row, column=1, sticky="ew", pady=(4, 4))
        button = ttk.Button(parent, command=browse_command)
        button.grid(row=row, column=2, sticky="ew", padx=(8, 0), pady=(4, 4))
        self.path_rows.append((label, label_key, button))

    def _apply_language(self) -> None:
        self.language_label.configure(text=self.tr("language"))
        for label, label_key, button in self.path_rows:
            label.configure(text=self.tr(label_key))
            button.configure(text=self.tr("browse"))
        self.mode_frame.configure(text=self.tr("mode"))
        self.decode_radio.configure(text=self.tr("decode"))
        self.encode_radio.configure(text=self.tr("encode"))
        self.exclude_river_check.configure(text=self.tr("exclude_river"))
        self.start_button.configure(text=self.tr("start"))
        self.clear_log_button.configure(text=self.tr("clear_log"))
        self.log_label.configure(text=self.tr("log"))

    def _browse_source(self) -> None:
        if self.mode_var.get() == ENCODE_MODE:
            title = self.tr("choose_interpreted_folder")
        else:
            title = self.tr("choose_source_island")
        selected = filedialog.askdirectory(title=title)
        if selected:
            self.source_var.set(selected)

    def _browse_output(self) -> None:
        selected = filedialog.askdirectory(title=self.tr("choose_output_folder"))
        if selected:
            self.output_var.set(selected)

    def _sync_mode_ui(self) -> None:
        is_decode = self.mode_var.get() == DECODE_MODE
        state = "!disabled" if is_decode else "disabled"
        self.exclude_river_check.state([state])
        if not is_decode:
            self.exclude_river_var.set(False)

    def _clear_log(self) -> None:
        self.log_widget.delete("1.0", tk.END)

    def _append_log(self, message: str) -> None:
        self.log_widget.insert(tk.END, message.rstrip() + "\n")
        self.log_widget.see(tk.END)

    def _pump_log_queue(self) -> None:
        try:
            while True:
                self._append_log(self.log_queue.get_nowait())
        except queue.Empty:
            pass
        self.root.after(100, self._pump_log_queue)

    def _start(self) -> None:
        if self.worker is not None and self.worker.is_alive():
            messagebox.showwarning(APP_TITLE, self.tr("already_running"))
            return
        try:
            cfg = self._build_config()
        except ValueError as exc:
            messagebox.showerror(APP_TITLE, str(exc))
            return
        action_label = self.tr("decode_action_lower") if cfg.mode == DECODE_MODE else self.tr("encode_action_lower")
        self._append_log("=" * 72)
        self._append_log(self.tr("starting").format(action=action_label))
        self.worker = threading.Thread(target=self._run_worker, args=(cfg, self.language_var.get()), daemon=True)
        self.worker.start()

    def _build_config(self) -> InterpreterConfig:
        source = Path(self.source_var.get().strip())
        output = Path(self.output_var.get().strip())
        if not source.exists():
            raise ValueError(self.tr("choose_valid_source"))
        if not output.exists():
            raise ValueError(self.tr("choose_valid_output"))
        max_workers = self.max_workers
        return InterpreterConfig(
            source_island_dir=source,
            output_root_dir=output,
            mode=self.mode_var.get(),
            exclude_river=self.exclude_river_var.get(),
            max_workers=max_workers,
            detailed_log=self.detailed_log,
            fast_zip=self.fast_zip,
        )

    def _run_worker(self, cfg: InterpreterConfig, language: str) -> None:
        texts = TRANSLATIONS.get(language, TRANSLATIONS["EN"])
        action_label = texts["decode_action"] if cfg.mode == DECODE_MODE else texts["encode_action"]

        def qlog(message: str) -> None:
            self.log_queue.put(message)

        try:
            report_path = IslandInterpreter(cfg, self.resources, qlog).run()
        except InterpreterError as exc:
            self.log_queue.put(texts["error_log"].format(error=exc))
            self.root.after(0, lambda: messagebox.showerror(APP_TITLE, str(exc)))
            return
        except Exception as exc:
            self.log_queue.put(texts["unexpected_error_log"].format(error=exc))
            self.root.after(0, lambda: messagebox.showerror(APP_TITLE, texts["unexpected_error_box"].format(error=exc)))
            return
        self.log_queue.put(texts["finished_log"].format(action=action_label, report_path=report_path))
        self.root.after(0, lambda: messagebox.showinfo(APP_TITLE, texts["finished_box"].format(action=action_label, report_path=report_path)))


def main() -> None:
    root = tk.Tk()
    IslandInterpreterApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()

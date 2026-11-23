"""Colour themes and the application stylesheet."""

# PyQt6 stubs type most widget getters as Optional; they are never None here.
# pyright: reportOptionalMemberAccess=false

import atexit
import shutil
import tempfile
from pathlib import Path

from PyQt6.QtCore import QPointF, Qt
from PyQt6.QtGui import QColor, QImage, QPainter, QPalette, QPolygonF
from PyQt6.QtWidgets import (
    QApplication,
)

# ---------------------------------------------------------------------------
# Themes
# ---------------------------------------------------------------------------

# fmt: off
THEMES: dict[str, dict[str, str]] = {
    "dark": {
        # SWP-S50 colour palette
        "window":         "#16203A",
        "window_text":    "#E8EAF6",
        "base":           "#0E1830",
        "alt_base":       "#243046",
        "button":         "#243046",
        "button_text":    "#E8EAF6",
        "highlight":      "#4A9EFF",
        "highlight_text": "#16203A",
        "placeholder":    "#8d99ae",
        "disabled":       "#4A5568",   # disabled text stays dim
        "tooltip_base":   "#243046",
        "tooltip_text":   "#E8EAF6",
        "border":         "#3A4E6E",
        "accent":         "#4A9EFF",
        "accent_dim":     "#1C3A6E",
        "btn_success_bg": "#a6e3a1",   # keep Catppuccin green (no local equivalent)
        "btn_success_fg": "#16203A",
        "btn_danger_bg":  "#FF6B6B",   # SWP-S50 red
        "btn_danger_fg":  "#16203A",
        "btn_info_bg":    "#4A9EFF",   # SWP-S50 blue
        "btn_info_fg":    "#16203A",
        "log_green":      "#a6e3a1",
        "log_red":        "#FF6B6B",
        "log_cyan":       "#94e2d5",
        "log_yellow":     "#f9e2af",
        "log_orange":     "#fab387",
        "log_gray":       "#8d99ae",
        "log_default":    "#E8EAF6",
    },
    "light": {
        # Catppuccin Latte-inspired
        "window":         "#eff1f5",
        "window_text":    "#4c4f69",
        "base":           "#e6e9ef",
        "alt_base":       "#dce0e8",
        "button":         "#ddd9f5",   # lavender-tinted instead of cold gray
        "button_text":    "#4c4f69",
        "highlight":      "#7287fd",   # Latte Lavender (pastel, replaces bold blue)
        "highlight_text": "#eff1f5",
        "placeholder":    "#5b6072",
        "disabled":       "#9ca0b0",   # disabled text stays dim
        "tooltip_base":   "#ddd9f5",
        "tooltip_text":   "#4c4f69",
        "border":         "#bcc0cc",
        "accent":         "#7287fd",   # Latte Lavender
        "accent_dim":     "#b4b8f5",   # pale lilac for scroll handles
        "btn_success_bg": "#b8e8b8",   # pastel green
        "btn_success_fg": "#1e3a1e",
        "btn_danger_bg":  "#f8b8c4",   # pastel red/pink
        "btn_danger_fg":  "#3a1e24",
        "btn_info_bg":    "#b8d0f8",   # pastel blue
        "btn_info_fg":    "#1e2a4a",
        "log_green":      "#40a02b",
        "log_red":        "#d20f39",
        "log_cyan":       "#179299",
        "log_yellow":     "#df8e1d",
        "log_orange":     "#fe640b",
        "log_gray":       "#5b6072",
        "log_default":    "#4c4f69",
    },
    "pink": {
        # Catppuccin-flavoured deep magenta
        "window":         "#1e1228",
        "window_text":    "#f5d0fe",
        "base":           "#160e1e",
        "alt_base":       "#2d1a42",
        "button":         "#3e2060",
        "button_text":    "#f5d0fe",
        "highlight":      "#e879f9",
        "highlight_text": "#1e1228",
        "placeholder":    "#a87cb7",
        "disabled":       "#9f6fb0",   # disabled text stays dim
        "tooltip_base":   "#3e2060",
        "tooltip_text":   "#f5d0fe",
        "border":         "#5b3070",
        "accent":         "#f5c2e7",   # Mocha Pink
        "accent_dim":     "#9a6080",   # muted Pink for scroll handles
        "btn_success_bg": "#a6e3a1",   # green (contrasts well against pink)
        "btn_success_fg": "#1e1228",
        "btn_danger_bg":  "#f38ba8",   # red-pink
        "btn_danger_fg":  "#1e1228",
        "btn_info_bg":    "#cba6f7",   # mauve/purple
        "btn_info_fg":    "#1e1228",
        "log_green":      "#a6e3a1",
        "log_red":        "#f38ba8",
        "log_cyan":       "#f5c2e7",
        "log_yellow":     "#f9e2af",
        "log_orange":     "#fab387",
        "log_gray":       "#a87cb7",
        "log_default":    "#f5d0fe",
    },
    "pink_light": {
        # Soft pastel pink — light counterpart to "pink", centred on #FFD7EE
        "window":         "#fff4f9",
        "window_text":    "#5c3a4e",
        "base":           "#ffe8f2",
        "alt_base":       "#ffd7ee",
        "button":         "#ffd7ee",
        "button_text":    "#5c3a4e",
        "highlight":      "#f06292",
        "highlight_text": "#fff4f9",
        "placeholder":    "#8c506d",
        "disabled":       "#c9a0b4",   # disabled text stays dim
        "tooltip_base":   "#ffd7ee",
        "tooltip_text":   "#5c3a4e",
        "border":         "#e8b8d0",
        "accent":         "#f06292",
        "accent_dim":     "#f8bbd0",
        "btn_success_bg": "#c8e6c9",   # pastel green
        "btn_success_fg": "#2e4830",
        "btn_danger_bg":  "#f8b8c4",   # pastel red-pink
        "btn_danger_fg":  "#4a1e28",
        "btn_info_bg":    "#f8bbd0",   # pastel pink
        "btn_info_fg":    "#4a1e30",
        "log_green":      "#2e7d32",
        "log_red":        "#c62828",
        "log_cyan":       "#00838f",
        "log_yellow":     "#f57f17",
        "log_orange":     "#e65100",
        "log_gray":       "#8c506d",
        "log_default":    "#5c3a4e",
    },
}
# fmt: on

# Mutable mapping updated by apply_theme(); used by log_append()
LOG_COLORS: dict[str, str] = {}


_ICON_DIR: Path | None = None


def _branch_arrows(color: str) -> tuple[str, str]:
    """Closed/open tree arrows in *color*, as PNGs in a per-process temp dir."""
    global _ICON_DIR
    if _ICON_DIR is None:
        _ICON_DIR = Path(tempfile.mkdtemp(prefix="mtr-icons-"))
        atexit.register(shutil.rmtree, _ICON_DIR, True)
    paths = []
    for kind, pts in (("closed", [(4, 2), (9, 6), (4, 10)]), ("open", [(2, 4), (10, 4), (6, 9)])):
        img = QImage(12, 12, QImage.Format.Format_ARGB32)
        img.fill(Qt.GlobalColor.transparent)
        qp = QPainter(img)
        qp.setRenderHint(QPainter.RenderHint.Antialiasing)
        qp.setPen(Qt.PenStyle.NoPen)
        qp.setBrush(QColor(color))
        qp.drawPolygon(QPolygonF([QPointF(x, y) for x, y in pts]))
        qp.end()
        path = _ICON_DIR / f"{kind}-{color.lstrip('#')}.png"
        img.save(str(path))
        paths.append(path.as_posix())
    return paths[0], paths[1]


def apply_theme(app: QApplication, name: str) -> None:
    """Apply a named theme to the application (palette + stylesheet)."""
    t = THEMES[name]
    LOG_COLORS.update({k: t[k] for k in t if k.startswith("log_")})

    app.setStyle("Fusion")

    def c(key: str) -> QColor:
        return QColor(t[key])

    pal = QPalette()
    # fmt: off
    pal.setColor(QPalette.ColorRole.Window,          c("window"))
    pal.setColor(QPalette.ColorRole.WindowText,      c("window_text"))
    pal.setColor(QPalette.ColorRole.Base,            c("base"))
    pal.setColor(QPalette.ColorRole.AlternateBase,   c("alt_base"))
    pal.setColor(QPalette.ColorRole.Button,          c("button"))
    pal.setColor(QPalette.ColorRole.ButtonText,      c("button_text"))
    pal.setColor(QPalette.ColorRole.Highlight,       c("highlight"))
    pal.setColor(QPalette.ColorRole.HighlightedText, c("highlight_text"))
    pal.setColor(QPalette.ColorRole.PlaceholderText, c("placeholder"))
    pal.setColor(QPalette.ColorRole.ToolTipBase,     c("tooltip_base"))
    pal.setColor(QPalette.ColorRole.ToolTipText,     c("tooltip_text"))
    pal.setColor(QPalette.ColorRole.Text,            c("window_text"))
    pal.setColor(QPalette.ColorRole.BrightText,      c("highlight"))
    # Rich text takes link colours from the application palette; Qt's default is unreadable on dark.
    pal.setColor(QPalette.ColorRole.Link,            c("accent"))
    pal.setColor(QPalette.ColorRole.LinkVisited,     c("accent"))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.WindowText, c("disabled"))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, c("disabled"))
    pal.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text,       c("disabled"))
    # fmt: on
    app.setPalette(pal)

    # fmt: off
    brd        = t["border"]
    hl         = t["highlight"]
    win        = t["window"]
    win_text   = t["window_text"]
    alt        = t["alt_base"]
    log_gray   = t["log_gray"]
    accent     = t["accent"]
    accent_dim = t["accent_dim"]
    s_bg  = t["btn_success_bg"];  s_fg  = t["btn_success_fg"]
    d_bg  = t["btn_danger_bg"];   d_fg  = t["btn_danger_fg"]
    i_bg  = t["btn_info_bg"];     i_fg  = t["btn_info_fg"]
    # hover colours: slightly darker for light themes, lighter for dark/pink
    _shift = (lambda col: QColor(col).darker(110).name()) if name in ("light", "pink_light") \
             else (lambda col: QColor(col).lighter(118).name())
    s_hov = _shift(s_bg); d_hov = _shift(d_bg)
    i_hov = _shift(i_bg); a_hov = _shift(accent)
    a_fg           = t["highlight_text"]   # accent-role button uses highlight_text
    highlight_text = a_fg
    dis_fg         = t["disabled"]
    # fmt: on

    def faded(col: str) -> str:
        """*col* 35 % of the way from the window colour: a disabled button's fill."""
        a, b = QColor(col), QColor(win)
        return QColor(
            *(round(0.35 * x + 0.65 * y) for x, y in zip(a.getRgb()[:3], b.getRgb()[:3], strict=True))
        ).name()

    arrow_closed, arrow_open = _branch_arrows(win_text)

    app.setStyleSheet(f"""
        QGroupBox {{
            border: 1px solid {brd};
            border-radius: 6px;
            margin-top: 10px;
            padding-top: 4px;
            font-weight: bold;
        }}
        QGroupBox::title {{
            color: {accent};
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 4px;
        }}
        QTabWidget::pane {{
            border: 1px solid {brd};
        }}
        QTabBar::tab {{
            padding: 8px 0;
            min-width: 0;
            border: 1px solid {accent_dim};
            border-bottom: none;
            border-top-left-radius: 6px;
            border-top-right-radius: 6px;
        }}
        QTabBar::tab:selected {{
            background: {win};
            color: {accent};
            border-color: {accent};
            border-top: 3px solid {accent};
        }}
        QTabBar::tab:hover:!selected {{
            background: {alt};
            border-color: {accent};
        }}
        QLineEdit:focus, QTextEdit:focus {{
            border: 1px solid {hl};
        }}
        QScrollBar::handle:vertical, QScrollBar::handle:horizontal {{
            background: {accent_dim};
            border-radius: 5px;
            min-height: 20px;
            min-width: 20px;
        }}
        QScrollBar::handle:vertical:hover, QScrollBar::handle:horizontal:hover {{
            background: {accent};
        }}
        QLabel[hint="true"] {{
            color: {log_gray};
            font-size: 12px;
        }}
        QLabel[intro="true"] {{
            font-size: 14px;
        }}
        QLabel[hint="note"] {{
            color: {log_gray};
            font-size: 14px;
        }}
        QComboBox {{
            border: 1px solid {accent_dim};
            border-radius: 6px;
            padding: 3px 8px;
            background-color: {alt};
            color: {win_text};
            min-height: 22px;
        }}
        QComboBox:focus {{
            border-color: {hl};
        }}
        /* Deliberately NOT styling QComboBox::drop-down: touching that
           subcontrol suppresses Qt's native chevron, which is the only mouse
           affordance an *editable* combo has for opening its list. */
        QComboBox QAbstractItemView {{
            border: 1px solid {accent_dim};
            background-color: {alt};
            color: {win_text};
            selection-background-color: {hl};
            selection-color: {highlight_text};
            outline: none;
            padding: 2px;
        }}
        QComboBox QAbstractItemView::item {{
            color: {win_text};
            background-color: {alt};
            padding: 4px 8px;
        }}
        QComboBox QAbstractItemView::item:selected {{
            background-color: {hl};
            color: {highlight_text};
        }}
        QToolBar {{
            border: none;
            border-bottom: 1px solid {brd};
            spacing: 4px;
            padding: 2px 6px;
        }}
        QPushButton {{
            border: none;
            border-radius: 8px;
            padding: 5px 16px;
            min-height: 24px;
            font-weight: 500;
        }}
        QPushButton[role="success"] {{
            background-color: {s_bg}; color: {s_fg};
        }}
        QPushButton[role="success"]:hover {{
            background-color: {s_hov};
        }}
        QPushButton[role="danger"] {{
            background-color: {d_bg}; color: {d_fg};
        }}
        QPushButton[role="danger"]:hover {{
            background-color: {d_hov};
        }}
        QPushButton[role="info"] {{
            background-color: {i_bg}; color: {i_fg};
        }}
        QPushButton[role="info"]:hover {{
            background-color: {i_hov};
        }}
        QPushButton[role="accent"] {{
            background-color: {accent}; color: {a_fg};
        }}
        QPushButton[role="accent"]:hover {{
            background-color: {a_hov};
        }}
        QPushButton[role="browse"] {{
            background-color: transparent;
            border: 1px solid {accent_dim};
            color: {accent};
        }}
        QPushButton[role="browse"]:hover {{
            border-color: {accent};
            background-color: {alt};
        }}
        /* After the role rules, which would otherwise win. Qt stylesheets
           ignore `opacity`, so each role gets an explicitly faded look. */
        QPushButton[role="success"]:disabled {{
            background-color: {faded(s_bg)}; color: {dis_fg};
        }}
        QPushButton[role="danger"]:disabled {{
            background-color: {faded(d_bg)}; color: {dis_fg};
        }}
        QPushButton[role="info"]:disabled {{
            background-color: {faded(i_bg)}; color: {dis_fg};
        }}
        QPushButton[role="accent"]:disabled {{
            background-color: {faded(accent)}; color: {dis_fg};
        }}
        QPushButton[role="browse"]:disabled {{
            border-color: {brd}; color: {dis_fg};
        }}
        QPushButton[symbol="true"] {{
            font-size: 20px;
            padding: 0 8px;
        }}
        QLabel[fontsize="large"], QComboBox[fontsize="large"],
        QComboBox[fontsize="large"] QAbstractItemView {{
            font-size: 14px;
        }}
        QTreeWidget[role="reports"] {{
            font-size: 14px;
            border: 1px solid {brd};
            border-radius: 8px;
            padding: 4px;
            outline: none;
            show-decoration-selected: 0;
        }}
        QTreeWidget[role="reports"]::item {{
            padding: 5px 6px;
            margin: 1px 2px;
            border-radius: 6px;
        }}
        QTreeWidget[role="reports"]::item:hover:!selected {{
            background-color: {alt};
        }}
        QTreeWidget[role="reports"]::item:selected {{
            background-color: {hl};
            color: {highlight_text};
        }}
        /* Opaque in every state, else hover/selection paint a stray box here. */
        QTreeWidget[role="reports"]::branch {{
            background-color: palette(base);
        }}
        QTreeWidget[role="reports"]::branch:has-children:closed {{
            image: url("{arrow_closed}");
        }}
        QTreeWidget[role="reports"]::branch:has-children:open {{
            image: url("{arrow_open}");
        }}
    """)

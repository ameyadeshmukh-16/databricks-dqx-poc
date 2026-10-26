"""
Generate AI_Infusion_Delivery_Assessment.pptx from scratch using python-pptx.
Run: python3 generate_pptx.py
"""
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt
import copy

# ── Palette ──────────────────────────────────────────────────────────────────
DARK_BLUE   = RGBColor(0x0F, 0x34, 0x60)   # headings, title bg
MID_BLUE    = RGBColor(0x16, 0x21, 0x3E)   # divider bg
RED_ACCENT  = RGBColor(0xE9, 0x45, 0x60)   # accent / underlines
LIGHT_TEAL  = RGBColor(0xA8, 0xDA, 0xDC)   # divider subtitle
WHITE       = RGBColor(0xFF, 0xFF, 0xFF)
LIGHT_GREY  = RGBColor(0xF7, 0xF9, 0xFC)
MED_GREY    = RGBColor(0xCC, 0xCC, 0xCC)
DARK_GREY   = RGBColor(0x33, 0x33, 0x33)
CODE_BG     = RGBColor(0x1E, 0x1E, 0x2E)
CODE_FG     = RGBColor(0xCD, 0xD6, 0xF4)
TABLE_HDR   = DARK_BLUE

# ── Extended palette for visual diagrams ─────────────────────────────────────
BOX_BLUE    = RGBColor(0xE8, 0xF0, 0xFE)   # light blue — neutral flow boxes
BOX_TEAL    = RGBColor(0xE4, 0xF6, 0xF1)   # light teal — AI / positive steps
BOX_RED     = RGBColor(0xFF, 0xED, 0xED)   # light pink — today / warning
BOX_GREEN   = RGBColor(0xE6, 0xF4, 0xEA)   # light green — clean / approved
BOX_ORANGE  = RGBColor(0xFF, 0xF3, 0xE0)   # light amber — staging / pending
TEAL_BORDER = RGBColor(0x0B, 0x8A, 0x6A)   # teal border for AI boxes
GREEN_BORDER= RGBColor(0x1E, 0x7E, 0x34)   # green border for approved/clean
ORANGE_BRD  = RGBColor(0xE6, 0x5C, 0x00)   # orange border for pending

W = Inches(13.33)
H = Inches(7.5)

prs = Presentation()
prs.slide_width  = W
prs.slide_height = H

BLANK = prs.slide_layouts[6]   # completely blank


# ── Helper: add text box ──────────────────────────────────────────────────────
def txb(slide, text, x, y, w, h,
        size=20, bold=False, color=DARK_GREY, align=PP_ALIGN.LEFT,
        wrap=True, italic=False):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = wrap
    p  = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = text
    run.font.size  = Pt(size)
    run.font.bold  = bold
    run.font.color.rgb = color
    run.font.italic = italic
    return tb


def para(tf, text, size=18, bold=False, color=DARK_GREY,
         align=PP_ALIGN.LEFT, space_before=0, italic=False, bullet=False):
    p = tf.add_paragraph()
    p.alignment = align
    p.space_before = Pt(space_before)
    if bullet:
        p.level = 0
    run = p.add_run()
    run.text = text
    run.font.size  = Pt(size)
    run.font.bold  = bold
    run.font.color.rgb = color
    run.font.italic = italic
    return p


def rect(slide, x, y, w, h, fill_color, line_color=None):
    from pptx.util import Emu
    shape = slide.shapes.add_shape(
        1,  # MSO_SHAPE_TYPE.RECTANGLE
        x, y, w, h
    )
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    if line_color:
        shape.line.color.rgb = line_color
    else:
        shape.line.fill.background()
    return shape


def accent_bar(slide, y=Inches(1.45)):
    """Red horizontal rule under slide title."""
    r = rect(slide, Inches(0.5), y, Inches(12.33), Pt(3), RED_ACCENT)
    return r


def slide_title(slide, title_text, y=Inches(0.4)):
    txb(slide, title_text,
        Inches(0.5), y, Inches(12.33), Inches(0.9),
        size=30, bold=True, color=DARK_BLUE)
    accent_bar(slide, y + Inches(0.82))


def code_block(slide, code_text, x, y, w, h, size=10.5):
    bg = rect(slide, x, y, w, h, CODE_BG)
    # left red stripe
    rect(slide, x, y, Pt(4), h, RED_ACCENT)
    tb = slide.shapes.add_textbox(x + Inches(0.12), y + Inches(0.1),
                                   w - Inches(0.18), h - Inches(0.2))
    tf = tb.text_frame
    tf.word_wrap = False
    first = True
    for line in code_text.split("\n"):
        if first:
            p = tf.paragraphs[0]
            first = False
        else:
            p = tf.add_paragraph()
        p.alignment = PP_ALIGN.LEFT
        run = p.add_run()
        run.text = line
        run.font.size = Pt(size)
        run.font.color.rgb = CODE_FG
        run.font.name = "Courier New"
    return tb


def table_slide(slide, headers, rows, x, y, w, col_widths=None, hdr_size=13, row_size=12):
    cols = len(headers)
    if col_widths is None:
        col_widths = [w // cols] * cols
    row_h = Inches(0.38)
    hdr_h = Inches(0.42)

    # header row
    cx = x
    for i, h_text in enumerate(headers):
        bg = rect(slide, cx, y, col_widths[i], hdr_h, TABLE_HDR)
        txb(slide, h_text, cx + Inches(0.05), y + Pt(4),
            col_widths[i] - Inches(0.1), hdr_h,
            size=hdr_size, bold=True, color=WHITE)
        cx += col_widths[i]

    # data rows
    for ri, row in enumerate(rows):
        ry = y + hdr_h + ri * row_h
        cx = x
        fill = LIGHT_GREY if ri % 2 == 0 else WHITE
        for ci, cell in enumerate(row):
            bg = rect(slide, cx, ry, col_widths[ci], row_h, fill)
            txb(slide, cell, cx + Inches(0.05), ry + Pt(3),
                col_widths[ci] - Inches(0.1), row_h, size=row_size)
            cx += col_widths[ci]


def bullet_list(slide, items, x, y, w, h, size=18, color=DARK_GREY,
                bullet_char="•", indent=Inches(0.25), space=6):
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    first = True
    for item in items:
        if first:
            p = tf.paragraphs[0]
            first = False
        else:
            p = tf.add_paragraph()
        p.space_before = Pt(space)
        run = p.add_run()
        run.text = f"{bullet_char}  {item}"
        run.font.size  = Pt(size)
        run.font.color.rgb = color
    return tb


# ── Visual diagram helpers ────────────────────────────────────────────────────
def fbox(slide, label, x, y, w=Inches(2.4), h=Inches(0.62),
         bg=BOX_BLUE, border=DARK_BLUE, text_color=DARK_BLUE,
         size=12.5, bold=False, border_width=1.2):
    """Labelled flow/diagram box with coloured background."""
    s = slide.shapes.add_shape(1, x, y, w, h)
    s.fill.solid()
    s.fill.fore_color.rgb = bg
    s.line.color.rgb = border
    s.line.width = Pt(border_width)
    tf = s.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    from pptx.util import Pt as _Pt
    from pptx.oxml.ns import qn
    # vertical centre via margin
    tf.margin_top = int(h / 2 - _Pt(size) * 0.75)
    run = p.add_run()
    run.text = label
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.color.rgb = text_color
    return s


def arrow_r(slide, x, y, size=18, color=MED_GREY):
    """Right-pointing arrow label."""
    txb(slide, "→", x, y, Inches(0.45), Inches(0.5),
        size=size, bold=True, color=color, align=PP_ALIGN.CENTER)


def arrow_d(slide, x, y, size=18, color=MED_GREY):
    """Down-pointing arrow label."""
    txb(slide, "↓", x, y, Inches(0.35), Inches(0.42),
        size=size, bold=True, color=color, align=PP_ALIGN.CENTER)


def arrow_l(slide, x, y, size=18, color=MED_GREY):
    """Left-pointing arrow label."""
    txb(slide, "←", x, y, Inches(0.45), Inches(0.5),
        size=size, bold=True, color=color, align=PP_ALIGN.CENTER)


def badge(slide, text, x, y, bg=DARK_BLUE, color=WHITE, size=11):
    """Small numbered badge / pill label."""
    s = slide.shapes.add_shape(1, x, y, Inches(0.32), Inches(0.32))
    s.fill.solid()
    s.fill.fore_color.rgb = bg
    s.line.fill.background()
    tf = s.text_frame
    tf.margin_top = Pt(1)
    p = tf.paragraphs[0]
    p.alignment = PP_ALIGN.CENTER
    run = p.add_run()
    run.text = text
    run.font.size = Pt(size)
    run.font.bold = True
    run.font.color.rgb = color


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 1 — Title
# ═══════════════════════════════════════════════════════════════════════════
def s01_title():
    sl = prs.slides.add_slide(BLANK)
    rect(sl, 0, 0, W, H, MID_BLUE)
    # accent top bar
    rect(sl, 0, 0, W, Inches(0.12), RED_ACCENT)
    # accent bottom bar
    rect(sl, 0, H - Inches(0.12), W, Inches(0.12), RED_ACCENT)

    txb(sl, "AI-Infused Data Quality Platform",
        Inches(1), Inches(2.0), Inches(11.33), Inches(1.2),
        size=44, bold=True, color=RED_ACCENT, align=PP_ALIGN.CENTER)

    txb(sl, "Proof of Concept — Retail & Wholesale Operations",
        Inches(1), Inches(3.3), Inches(11.33), Inches(0.7),
        size=24, color=LIGHT_TEAL, align=PP_ALIGN.CENTER)

    txb(sl, "Assessment: AI Infusion in Delivery",
        Inches(1), Inches(4.2), Inches(11.33), Inches(0.5),
        size=18, color=WHITE, align=PP_ALIGN.CENTER)

    txb(sl, "Databricks  ·  Delta Lake  ·  Apache Kafka  ·  LLM-Powered Rule Generation",
        Inches(1), Inches(4.9), Inches(11.33), Inches(0.5),
        size=14, color=MED_GREY, align=PP_ALIGN.CENTER)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 2 — Agenda
# ═══════════════════════════════════════════════════════════════════════════
def s02_agenda():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Agenda")
    items = [
        "1.  The Problem — Data quality at scale in retail operations",
        "2.  The Vision — What AI-infused data quality looks like",
        "3.  What We Built — POC overview and technology stack",
        "4.  Phase-by-Phase Walkthrough — Five working components",
        "5.  AI in Action — Where and how AI is infused",
        "6.  Architecture Deep Dive — How it all connects",
        "7.  Production Considerations — From POC to enterprise scale",
        "8.  What We Proved — Key POC outcomes",
        "9.  AI Infusion Opportunities — What comes next",
    ]
    bullet_list(sl, items,
                Inches(0.7), Inches(1.65), Inches(12.0), Inches(5.5),
                size=19, bullet_char="", space=8)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 3 — Section divider: The Problem
# ═══════════════════════════════════════════════════════════════════════════
def divider(title, subtitle):
    sl = prs.slides.add_slide(BLANK)
    rect(sl, 0, 0, W, H, DARK_BLUE)
    rect(sl, 0, 0, W, Inches(0.12), RED_ACCENT)
    rect(sl, 0, H - Inches(0.12), W, Inches(0.12), RED_ACCENT)
    txb(sl, title,
        Inches(1), Inches(2.6), Inches(11.33), Inches(1.0),
        size=40, bold=True, color=RED_ACCENT, align=PP_ALIGN.CENTER)
    txb(sl, subtitle,
        Inches(1), Inches(3.75), Inches(11.33), Inches(0.7),
        size=20, color=LIGHT_TEAL, align=PP_ALIGN.CENTER, italic=True)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 4 — The Data Quality Challenge
# ═══════════════════════════════════════════════════════════════════════════
def s04_challenge():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "The Data Quality Challenge in Retail & Wholesale")
    txb(sl, "Every day, bad data costs the business",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.4),
        size=17, bold=True, color=DARK_BLUE)

    headers = ["Data Problem", "Business Impact"]
    rows = [
        ["Incorrect pricing records",      "Revenue leakage, pricing disputes"],
        ["Missing inventory quantities",   "Stockouts or overstock decisions"],
        ["Duplicate transaction records",  "Double-counting in financial reports"],
        ["Stale / out-of-order events",    "Wrong demand-forecasting inputs"],
        ["Corrupted supplier data",        "Bad purchase order generation"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.6), Inches(2.05), Inches(12.1),
                col_widths=[Inches(5.4), Inches(6.7)],
                hdr_size=14, row_size=13)

    # callout box
    bg = rect(sl, Inches(0.6), Inches(5.85), Inches(12.1), Inches(1.2), LIGHT_GREY)
    rect(sl, Inches(0.6), Inches(5.85), Pt(5), Inches(1.2), RED_ACCENT)
    txb(sl, "The hidden cost:  Most bad data is discovered after it has already influenced a "
            "decision — by the time it surfaces, the damage is done.",
        Inches(0.85), Inches(5.95), Inches(11.7), Inches(1.0),
        size=15, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 5 — Why Traditional DQ Breaks Down
# ═══════════════════════════════════════════════════════════════════════════
def s05_problem():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Why Traditional DQ Approaches Break Down")

    txb(sl, "Manual Rule Writing  →  Slow to create, slow to update, misses new patterns",
        Inches(0.6), Inches(1.65), Inches(12.1), Inches(0.45),
        size=14, color=CODE_FG,
        bold=False)
    # code-style background for above
    rect(sl, Inches(0.6), Inches(1.62), Inches(12.1), Inches(0.45), CODE_BG)
    txb(sl, "Manual Rule Writing  →  Slow to create, slow to update, misses new patterns",
        Inches(0.75), Inches(1.65), Inches(12.0), Inches(0.45),
        size=13, color=CODE_FG, bold=False)

    items = [
        "A data engineer manually writes each validation rule",
        "Rules are written after a problem is discovered — reactive, not proactive",
        "Rules are rarely reviewed once written — stale rules pile up silently",
        "When source schemas change, rules silently become irrelevant",
        "No visibility into which rules are actually catching anything",
        "Streaming data adds real-time complexity that batch-only rules can't address",
    ]
    bullet_list(sl, items,
                Inches(0.7), Inches(2.2), Inches(12.0), Inches(3.5),
                size=17, space=10)

    bg = rect(sl, Inches(0.6), Inches(5.85), Inches(12.1), Inches(1.2), LIGHT_GREY)
    rect(sl, Inches(0.6), Inches(5.85), Pt(5), Inches(1.2), RED_ACCENT)
    txb(sl, "The gap:  Data volumes and schema complexity are growing faster than a team "
            "can write and maintain rules by hand.",
        Inches(0.85), Inches(5.95), Inches(11.7), Inches(1.0),
        size=15, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 6 — What We Need
# ═══════════════════════════════════════════════════════════════════════════
def s06_vision_need():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "What We Need")

    # ── TODAY box (left) ──────────────────────────────────────────────────
    today_x, box_y = Inches(0.45), Inches(1.65)
    box_w, box_h   = Inches(5.55), Inches(4.85)
    s = sl.shapes.add_shape(1, today_x, box_y, box_w, box_h)
    s.fill.solid(); s.fill.fore_color.rgb = BOX_RED
    s.line.color.rgb = RED_ACCENT; s.line.width = Pt(1.8)

    txb(sl, "TODAY", today_x, box_y + Inches(0.1), box_w, Inches(0.45),
        size=17, bold=True, color=RED_ACCENT, align=PP_ALIGN.CENTER)
    txb(sl, "Reactive  ·  Slow  ·  Brittle",
        today_x, box_y + Inches(0.55), box_w, Inches(0.32),
        size=11.5, italic=True, color=RED_ACCENT, align=PP_ALIGN.CENTER)
    # thin rule under subtitle
    sep = sl.shapes.add_shape(1, today_x + Inches(0.3), box_y + Inches(0.88),
                               box_w - Inches(0.6), Pt(1))
    sep.fill.solid(); sep.fill.fore_color.rgb = RED_ACCENT; sep.line.fill.background()

    today_items = [
        "Engineer writes every rule manually",
        "Rules created after a problem surfaces",
        "Stale rules pile up — never reviewed",
        "Schema changes break rules silently",
        "No visibility into rule effectiveness",
    ]
    bullet_list(sl, today_items, today_x + Inches(0.22), box_y + Inches(1.05),
                box_w - Inches(0.3), Inches(3.5), size=14.5, space=14,
                color=RGBColor(0x55, 0x11, 0x1A))

    # ── Arrow + label (centre) ────────────────────────────────────────────
    txb(sl, "→", Inches(6.2), Inches(3.5), Inches(0.9), Inches(0.7),
        size=38, bold=True, color=DARK_BLUE, align=PP_ALIGN.CENTER)
    txb(sl, "AI\nInfusion", Inches(6.1), Inches(4.22), Inches(1.1), Inches(0.7),
        size=11, bold=True, color=DARK_BLUE, align=PP_ALIGN.CENTER)

    # ── TARGET box (right) ────────────────────────────────────────────────
    tgt_x = Inches(7.3)
    t = sl.shapes.add_shape(1, tgt_x, box_y, box_w, box_h)
    t.fill.solid(); t.fill.fore_color.rgb = BOX_BLUE
    t.line.color.rgb = DARK_BLUE; t.line.width = Pt(1.8)

    txb(sl, "TARGET", tgt_x, box_y + Inches(0.1), box_w, Inches(0.45),
        size=17, bold=True, color=DARK_BLUE, align=PP_ALIGN.CENTER)
    txb(sl, "Proactive  ·  Adaptive  ·  Governed",
        tgt_x, box_y + Inches(0.55), box_w, Inches(0.32),
        size=11.5, italic=True, color=DARK_BLUE, align=PP_ALIGN.CENTER)
    sep2 = sl.shapes.add_shape(1, tgt_x + Inches(0.3), box_y + Inches(0.88),
                                box_w - Inches(0.6), Pt(1))
    sep2.fill.solid(); sep2.fill.fore_color.rgb = DARK_BLUE; sep2.line.fill.background()

    target_items = [
        "AI analyses data and generates rules",
        "Rules suggested proactively from patterns",
        "Effectiveness tracked after every run",
        "Stale rules automatically detected",
        "Human governance at every approval step",
    ]
    bullet_list(sl, target_items, tgt_x + Inches(0.22), box_y + Inches(1.05),
                box_w - Inches(0.3), Inches(3.5), size=14.5, space=14,
                color=RGBColor(0x0A, 0x22, 0x44))

    # ── Three shifts banner ───────────────────────────────────────────────
    banner = sl.shapes.add_shape(1, Inches(0.45), Inches(6.65), Inches(12.4), Inches(0.58))
    banner.fill.solid(); banner.fill.fore_color.rgb = DARK_BLUE; banner.line.fill.background()
    txb(sl, "Three shifts:   Batch → Real-time   ·   Manual → AI-Generated   ·   Static → Self-Improving",
        Inches(0.45), Inches(6.68), Inches(12.4), Inches(0.52),
        size=14, bold=True, color=WHITE, align=PP_ALIGN.CENTER)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 7 — The AI Infusion Vision  (visual flow diagram)
# ═══════════════════════════════════════════════════════════════════════════
def s07_ai_vision():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "The AI Infusion Vision")

    # Banner label
    banner = sl.shapes.add_shape(1, Inches(2.5), Inches(1.65), Inches(8.3), Inches(0.42))
    banner.fill.solid(); banner.fill.fore_color.rgb = DARK_BLUE; banner.line.fill.background()
    txb(sl, "CONTINUOUS FEEDBACK LOOP", Inches(2.5), Inches(1.68), Inches(8.3), Inches(0.38),
        size=12, bold=True, color=WHITE, align=PP_ALIGN.CENTER)

    BW = Inches(2.5); BH = Inches(0.62)

    # ── Row 1: Raw Data → DQ Rule Engine → Clean Records ─────────────────
    r1y = Inches(2.25)
    fbox(sl, "Raw Data", Inches(0.5), r1y, BW, BH, BOX_BLUE, DARK_BLUE, DARK_BLUE, 12.5)
    arrow_r(sl, Inches(3.1), r1y + Inches(0.1))
    fbox(sl, "DQ Rule Engine", Inches(3.6), r1y, BW, BH, BOX_BLUE, DARK_BLUE, DARK_BLUE, 12.5, bold=True)
    arrow_r(sl, Inches(6.2), r1y + Inches(0.1))
    fbox(sl, "✓  Clean Records\n(analytics-ready)",
         Inches(6.75), r1y, BW, BH, BOX_GREEN, GREEN_BORDER, GREEN_BORDER, 11.5)

    # ── Down arrow from DQ Rule Engine ───────────────────────────────────
    arrow_d(sl, Inches(4.52), Inches(2.92))

    # ── Row 2: Quarantine → AI Analyses Patterns ─────────────────────────
    r2y = Inches(3.43)
    fbox(sl, "✗  Quarantine Records\n(rule violations)",
         Inches(3.6), r2y, BW, BH, BOX_RED, RED_ACCENT, RED_ACCENT, 11.5)
    arrow_r(sl, Inches(6.2), r2y + Inches(0.1))
    fbox(sl, "AI Analyses Patterns", Inches(6.75), r2y, BW, BH,
         BOX_TEAL, TEAL_BORDER, TEAL_BORDER, 12.5)

    # ── Down arrow from Quarantine ────────────────────────────────────────
    arrow_d(sl, Inches(4.52), Inches(4.1))

    # ── Rule Effectiveness side branch ────────────────────────────────────
    arrow_d(sl, Inches(7.98), Inches(4.1))

    # ── Row 3: Rules Registry ← Human Review ← AI Rule Suggester ─────────
    r3y = Inches(4.62)
    fbox(sl, "dq_rules_registry\n(active rules)", Inches(0.5), r3y, BW, BH,
         BOX_GREEN, GREEN_BORDER, GREEN_BORDER, 11.5)
    arrow_l(sl, Inches(3.1), r3y + Inches(0.1))
    fbox(sl, "Human Review\n(approve / reject)", Inches(3.6), r3y, BW, BH,
         BOX_ORANGE, ORANGE_BRD, ORANGE_BRD, 11.5)
    arrow_l(sl, Inches(6.2), r3y + Inches(0.1))
    fbox(sl, "AI Rule Suggester", Inches(6.75), r3y, BW, BH,
         BOX_TEAL, TEAL_BORDER, TEAL_BORDER, 12.5)

    # Rule Effectiveness box (right side)
    fbox(sl, "Rule Effectiveness\nDashboard", Inches(6.75), Inches(4.62) + Inches(0.82),
         BW, BH, BOX_BLUE, DARK_BLUE, DARK_BLUE, 11.5)

    # ── Return arrow: registry → raw data (shown as label) ───────────────
    txb(sl, "↑  rules updated → better validation on next run",
        Inches(0.5), r3y + Inches(0.68), Inches(2.5), Inches(0.55),
        size=9.5, italic=True, color=DARK_GREY)

    # ── Footer note ───────────────────────────────────────────────────────
    txb(sl, "AI is not a bolt-on — it is the feedback mechanism that makes the system self-improving.",
        Inches(0.5), Inches(7.05), Inches(12.3), Inches(0.32),
        size=13.5, bold=True, color=DARK_BLUE, italic=True, align=PP_ALIGN.CENTER)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 8 — POC Overview
# ═══════════════════════════════════════════════════════════════════════════
def s08_poc_overview():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "POC Overview — Five Phases plus Stage A")

    headers = ["#", "Phase", "What It Does", "Status"]
    rows = [
        ["1", "Batch DQ Validation",        "Validates daily transaction / product data against rules",     "✅ Complete"],
        ["2", "Real-time Streaming",         "Validates live event streams via Kafka / Event Hubs",         "✅ Complete"],
        ["3", "AI Rule Suggester",           "LLM analyses data profile and generates DQ rules",            "✅ Complete"],
        ["4", "Quarantine Analyzer",         "Surfaces patterns in bad records, root-cause insights",        "✅ Complete"],
        ["5", "Rule Effectiveness Dashboard","Tracks which rules fire, trends, zero-fire candidates",        "✅ Complete"],
        ["A", "Automated Alerting",          "Flags violation-rate spikes vs. each rule's own baseline; audit log", "✅ Built — Slack pending"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.5), Inches(1.65), Inches(12.3),
                col_widths=[Inches(0.5), Inches(2.6), Inches(6.6), Inches(2.6)],
                hdr_size=14, row_size=13)

    bg = rect(sl, Inches(0.5), Inches(5.55), Inches(12.3), Inches(1.15), LIGHT_GREY)
    rect(sl, Inches(0.5), Inches(5.55), Pt(5), Inches(1.15), RED_ACCENT)
    txb(sl, "All five phases and Stage A alerting are built and running on Databricks; the batch DAG and the "
            "streaming pipeline have been run end to end. The POC uses generated retail data (sales transactions "
            "+ event streams) designed to mirror real-world data patterns at scale.",
        Inches(0.75), Inches(5.62), Inches(11.9), Inches(1.0),
        size=14, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 9 — Technology Stack
# ═══════════════════════════════════════════════════════════════════════════
def s09_tech_stack():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Technology Stack")

    # Left column
    rect(sl, Inches(0.5), Inches(1.65), Inches(5.8), Inches(5.4), LIGHT_GREY)
    txb(sl, "Platform & Data", Inches(0.6), Inches(1.75), Inches(5.6), Inches(0.4),
        size=15, bold=True, color=DARK_BLUE)
    left_items = [
        "Databricks — unified compute, notebooks, jobs",
        "Delta Lake — ACID-compliant storage for all DQ data",
        "Apache Kafka / Azure Event Hubs — real-time streaming",
        "Unity Catalog — governed data asset management",
        "PySpark — distributed data processing",
    ]
    bullet_list(sl, left_items, Inches(0.65), Inches(2.2), Inches(5.6), Inches(4.0),
                size=15, space=10)

    # Right column
    rect(sl, Inches(7.0), Inches(1.65), Inches(5.8), Inches(5.4), LIGHT_GREY)
    txb(sl, "AI & Quality Framework", Inches(7.1), Inches(1.75), Inches(5.6), Inches(0.4),
        size=15, bold=True, color=DARK_BLUE)
    right_items = [
        "Databricks Labs DQX — core DQ rule engine",
        "Anthropic Claude — primary LLM for rule generation",
        "Azure OpenAI — enterprise LLM alternative (pluggable)",
        "OpenRouter — multi-model fallback option",
        "Databricks Asset Bundles — infra-as-code for all jobs",
    ]
    bullet_list(sl, right_items, Inches(7.1), Inches(2.2), Inches(5.6), Inches(4.0),
                size=15, space=10)

    bg = rect(sl, Inches(0.5), Inches(6.8), Inches(12.3), Inches(0.5), LIGHT_GREY)
    rect(sl, Inches(0.5), Inches(6.8), Pt(5), Inches(0.5), RED_ACCENT)
    txb(sl, "Key design: LLM backend is pluggable — switching providers requires only an env var change, not code changes.",
        Inches(0.75), Inches(6.85), Inches(12.0), Inches(0.4),
        size=13, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 10 — Phase 1: Batch DQ
# ═══════════════════════════════════════════════════════════════════════════
def s10_phase1():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Phase 1 — Batch Data Quality Validation")
    txb(sl, "Validates daily sales / inventory datasets against a governed rule set.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.4), size=15, italic=True, color=DARK_GREY)

    code = (
        "# notebooks/batch_dqx_validation.py\n"
        "dq_df = dqx.apply_checks_by_metadata(\n"
        "    sales_df, checks, custom_check_functions=custom_check_functions,\n"
        ")\n"
        "# Split clean vs. quarantine on the _errors array DQX attaches to each row\n"
        "good_df       = dq_df.filter((col('_errors').isNull()) | (size(col('_errors')) == 0))\n"
        "quarantine_df = dq_df.filter((col('_errors').isNotNull()) & (size(col('_errors')) > 0))\n"
        "\n"
        "# Rule effectiveness captured after every run\n"
        "_run_id  = str(uuid.uuid4())\n"
        "_metrics = compute_rule_metrics(quarantine_df, total_count, _run_id, 'batch', BATCH_RULE_CONFIG_NAME)\n"
        "_written = write_rule_metrics(_metrics, spark)"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(6.8), Inches(3.3), size=10)

    txb(sl, "Rules in configs/rules_batch.yml:", Inches(7.6), Inches(2.1), Inches(5.2), Inches(0.35),
        size=13, bold=True, color=DARK_BLUE)
    yaml_code = (
        "- name: unit_price_positive\n"
        "  column: unit_price\n"
        "  type: min_value\n"
        "  min: 0.01\n"
        "  criticality: error\n"
        "\n"
        "- name: division_allowlist\n"
        "  column: division\n"
        "  type: is_in_list\n"
        "  allowed_values: ['011','012','013']\n"
        "  criticality: error\n"
        "\n"
        "- name: coupon_division_integrity\n"
        "  column: division\n"
        "  type: custom_sql\n"
        "  sql_expression: >-\n"
        "    NOT (coupon_applied=true AND division='540')\n"
        "  criticality: error"
    )
    code_block(sl, yaml_code, Inches(7.6), Inches(2.5), Inches(5.2), Inches(3.7), size=10)

    txb(sl, "Business output: Every analytics pipeline consumes only validated data.",
        Inches(0.5), Inches(5.55), Inches(12.3), Inches(0.4),
        size=14, bold=True, color=DARK_BLUE)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 11 — Phase 2: Streaming
# ═══════════════════════════════════════════════════════════════════════════
def s11_phase2():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Phase 2 — Real-time Streaming Validation")
    txb(sl, "Validates every Kafka / Azure Event Hubs event before it reaches any consumer.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.4), size=15, italic=True, color=DARK_GREY)

    code = (
        "# notebooks/kafka_stream_dqx_validation.py — foreachBatch handler\n"
        "def process(batch_df, batch_id):\n"
        "    dq_df = dqx.apply_checks_by_metadata(\n"
        "        batch_df, checks, custom_check_functions=custom_check_functions,\n"
        "    )\n"
        "    # Normalise NULL arrays to keep Delta schema consistent across batches\n"
        "    dq_df = dq_df.withColumn('_errors',\n"
        "        expr('CASE WHEN _errors IS NULL THEN array() ELSE _errors END'))\n"
        "\n"
        "    clean      = dq_df.filter(size(col('_errors')) == 0).drop('_errors')\n"
        "    quarantine = dq_df.filter(size(col('_errors')) > 0)\n"
        "\n"
        "    clean.write.mode('append').option('mergeSchema','true') \\\n"
        "               .saveAsTable(TABLE_STREAM_CLEAN)\n"
        "    quarantine.write.mode('append').option('mergeSchema','true') \\\n"
        "               .saveAsTable(TABLE_STREAM_QUARANTINE)"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(12.3), Inches(4.0), size=10.5)

    txb(sl, "Key design:  Rules loaded once at stream startup — no per-batch registry reads.  "
            "ADLS-backed checkpoints ensure fault tolerance and exactly-once semantics.",
        Inches(0.5), Inches(6.25), Inches(12.3), Inches(0.9),
        size=14, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 12 — Phase 3: AI Rule Suggester overview
# ═══════════════════════════════════════════════════════════════════════════
def s12_phase3_overview():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Phase 3 — AI Rule Suggester  ✨")
    txb(sl, "Profiles a data table and uses an LLM to generate new DQ rules automatically.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.4), size=15, italic=True, color=DARK_GREY)

    STEP_W, STEP_H = Inches(3.4), Inches(1.35)

    # ── Row 1: Steps 1 → 2 → 3 ───────────────────────────────────────────
    r1y = Inches(2.1)
    fbox(sl, "Profile\nAnalyse column stats\n(min, max, nulls,\ncardinality)",
         Inches(0.4), r1y, STEP_W, STEP_H, BOX_BLUE, DARK_BLUE, DARK_BLUE, 12)
    txb(sl, "①", Inches(0.52), r1y + Inches(0.07), Inches(0.4), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    arrow_r(sl, Inches(3.92), r1y + Inches(0.36))

    fbox(sl, "Build Prompt\nConvert stats into\nstructured LLM prompt\n+ domain context",
         Inches(4.5), r1y, STEP_W, STEP_H, BOX_BLUE, DARK_BLUE, DARK_BLUE, 12)
    txb(sl, "②", Inches(4.62), r1y + Inches(0.07), Inches(0.4), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    arrow_r(sl, Inches(8.02), r1y + Inches(0.36))

    fbox(sl, "LLM Generates\nClaude / Azure OpenAI\nreturns candidate rules\nin structured JSON",
         Inches(8.6), r1y, STEP_W, STEP_H, BOX_TEAL, TEAL_BORDER, TEAL_BORDER, 12)
    txb(sl, "③", Inches(8.72), r1y + Inches(0.07), Inches(0.4), Inches(0.38),
        size=14, bold=True, color=TEAL_BORDER)
    arrow_d(sl, Inches(10.13), Inches(3.52))

    # ── Row 2: Registry ← Review ← Validate/Stage ────────────────────────
    r2y = Inches(4.0)
    fbox(sl, "Validate & Stage\nParse JSON, validate\nstructure, write to\ndq_rules_staging",
         Inches(8.6), r2y, STEP_W, STEP_H, BOX_BLUE, DARK_BLUE, DARK_BLUE, 12)
    txb(sl, "④", Inches(8.72), r2y + Inches(0.07), Inches(0.4), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    arrow_l(sl, Inches(8.02), r2y + Inches(0.36))

    fbox(sl, "Human Review\nEngineer approves\nor rejects each rule",
         Inches(4.5), r2y, STEP_W, STEP_H, BOX_ORANGE, ORANGE_BRD, ORANGE_BRD, 12)
    txb(sl, "⑤", Inches(4.62), r2y + Inches(0.07), Inches(0.4), Inches(0.38),
        size=14, bold=True, color=ORANGE_BRD)
    arrow_l(sl, Inches(3.92), r2y + Inches(0.36))

    fbox(sl, "dq_rules_registry\nPromoted & active\nin all future batch\n+ streaming runs",
         Inches(0.4), r2y, STEP_W, STEP_H, BOX_GREEN, GREEN_BORDER, GREEN_BORDER, 12)

    # ── Two-source footer ─────────────────────────────────────────────────
    banner = sl.shapes.add_shape(1, Inches(0.4), Inches(5.55), Inches(12.5), Inches(0.46))
    banner.fill.solid(); banner.fill.fore_color.rgb = LIGHT_GREY; banner.line.fill.background()
    txb(sl, "Two complementary sources:   Statistical rules (no LLM, pure maths)   +   LLM rules (domain reasoning)",
        Inches(0.4), Inches(5.58), Inches(12.5), Inches(0.4),
        size=13, color=DARK_BLUE, align=PP_ALIGN.CENTER)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 13 — Phase 3: LLM backend
# ═══════════════════════════════════════════════════════════════════════════
def s13_llm_backend():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Phase 3 — How the LLM Backend Works")
    txb(sl, "Three providers, one interface — switch via environment variable:",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38), size=15, italic=True, color=DARK_GREY)

    code = (
        "# ai_suggester/llm_backend.py\n"
        "def get_backend(name: str | None = None) -> LLMBackend:\n"
        "    resolved = (name or os.environ.get('LLM_BACKEND', 'claude')).lower()\n"
        "    if resolved == 'claude':        return ClaudeBackend()        # Anthropic API\n"
        "    if resolved == 'azure_openai':  return AzureOpenAIBackend()   # Azure OpenAI\n"
        "    if resolved == 'openrouter':    return OpenRouterBackend()     # OpenRouter\n"
        "\n"
        "# ClaudeBackend.generate() — actual API call\n"
        "message = client.messages.create(\n"
        "    model=self._model,           # e.g. 'claude-sonnet-4-6'\n"
        "    max_tokens=4096,\n"
        "    system=_SYSTEM_PROMPT,       # strict JSON-only output instructions\n"
        "    messages=[{'role': 'user', 'content': prompt}],\n"
        ")"
    )
    code_block(sl, code, Inches(0.5), Inches(2.05), Inches(12.3), Inches(3.65), size=11)

    txb(sl, "The LLM is given a strict system prompt specifying:",
        Inches(0.6), Inches(5.85), Inches(12.0), Inches(0.38),
        size=15, bold=True, color=DARK_BLUE)
    items = [
        "Output format: JSON array only — no prose, no markdown fences",
        "Allowed functions: is_not_null, min_value, max_value, sql_expression, matches_regex, is_in_list",
        "Required fields per rule: name, column, function, constraint, criticality, message, reasoning",
    ]
    bullet_list(sl, items, Inches(0.7), Inches(6.3), Inches(12.0), Inches(1.1), size=14, space=4)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 14 — Phase 3: Statistical rule generator
# ═══════════════════════════════════════════════════════════════════════════
def s14_stat_rules():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Phase 3 — Statistical Rule Generator (No LLM)")
    txb(sl, "Derives rules purely from column statistics — fast, deterministic, no API cost.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38), size=15, italic=True, color=DARK_GREY)

    code = (
        "# ai_suggester/rule_generator.py — suggest_statistical_rules()\n"
        "for col_name, stats in profile.items():\n"
        "    null_pct = stats.get('null_pct', 1.0)\n"
        "\n"
        "    # Rule: is_not_null — if < 1% nulls observed historically\n"
        "    if null_pct < STAT_NULL_THRESHOLD:   # STAT_NULL_THRESHOLD = 0.01\n"
        "        rules.append({'name': f'{col_name}_not_null',\n"
        "                       'function': 'is_not_null', 'confidence': 1.0 - null_pct, ...})\n"
        "\n"
        "    # Rule: min_value — if all observed values are strictly positive\n"
        "    # (skipped for code-like integers such as division / *_id)\n"
        "    if is_numeric and not _is_code_like(col_name, dtype) and col_min > 0:\n"
        "        rules.append({'name': f'{col_name}_min_value',\n"
        "                       'function': 'min_value', 'constraint': {'min': 0}, ...})\n"
        "\n"
        "    # Rule: is_in_list — for low-cardinality categoricals (≤ 50 distinct values)\n"
        "    if distinct_count <= STAT_DISTINCT_THRESHOLD and not is_numeric:\n"
        "        rules.append({'name': f'{col_name}_valid_values',\n"
        "                       'function': 'sql_expression', ...})"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(12.3), Inches(4.4), size=10.5)

    txb(sl, "Statistical rules provide a fast baseline before the LLM adds domain-reasoning rules on top.",
        Inches(0.5), Inches(6.65), Inches(12.3), Inches(0.5),
        size=15, bold=True, color=DARK_BLUE)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 15 — Phase 3: Human review & promotion
# ═══════════════════════════════════════════════════════════════════════════
def s15_human_review():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Phase 3 — Human Review & Promotion")
    txb(sl, "No AI rule becomes active without engineer sign-off.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38), size=15, italic=True, color=DARK_GREY)

    code = (
        "# ai_suggester/rule_writer.py — staging table schema\n"
        "CREATE TABLE IF NOT EXISTS dq_rules_staging (\n"
        "    rule_id         STRING,    -- UUID per suggestion\n"
        "    name            STRING,    -- e.g. 'unit_price_positive'\n"
        "    function        STRING,    -- 'min_value', 'is_not_null', etc.\n"
        "    criticality     STRING,    -- 'error' | 'warning'\n"
        "    source          STRING,    -- 'statistical' | 'llm'\n"
        "    llm_backend     STRING,    -- 'claude' | 'azure_openai' | null\n"
        "    confidence      DOUBLE,    -- 0.0–1.0\n"
        "    reasoning       STRING,    -- LLM's explanation\n"
        "    status          STRING,    -- 'pending' → 'approved' | 'rejected'\n"
        "    suggested_at    TIMESTAMP,\n"
        "    reviewed_at     TIMESTAMP,\n"
        "    reviewed_by     STRING\n"
        ") USING DELTA\n"
        "\n"
        "# Engineer calls:\n"
        "promote_rule(rule_id, reviewed_by, spark)  # DQX-native names -> dq_rules_registry\n"
        "reject_rule(rule_id, reviewed_by, spark)   # status='rejected', kept for fine-tuning\n"
        "\n"
        "# On write: duplicates and names of live rules are skipped"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(12.3), Inches(4.6), size=10.5)

    txb(sl, "Audit trail: rule_id · suggested_at · reviewed_by · llm_backend   |   "
            "Duplicates skipped on write · DQX names applied on promotion",
        Inches(0.5), Inches(6.8), Inches(12.3), Inches(0.6),
        size=13, bold=True, color=DARK_BLUE)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 16 — Phase 4: Quarantine Analyzer
# ═══════════════════════════════════════════════════════════════════════════
def s16_quarantine():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Phase 4 — Quarantine Analyzer")
    txb(sl, "Turns a table of bad records into actionable data quality incidents.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38), size=15, italic=True, color=DARK_GREY)

    code = (
        "  dq_sales_quarantine / stream_events_quarantine\n"
        "                │\n"
        "                ▼\n"
        "  Quarantine Analyzer Notebook\n"
        "  ┌──────────────────────────────────────────────────────┐\n"
        "  │  • Which rules are triggering most violations?        │\n"
        "  │  • Are violations concentrated in specific stores?    │\n"
        "  │  • Are violations correlated with time? (weekends?)   │\n"
        "  │  • Which columns appear in the most violation records?│\n"
        "  └──────────────────────────────────────────────────────┘\n"
        "                │\n"
        "                ▼\n"
        "  Example insight: '60% of price violations originate\n"
        "  from division 540 — flagged in coupon_division_integrity'"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(6.5), Inches(4.2), size=11)

    txb(sl, "Powered by the _errors column:", Inches(7.2), Inches(2.1), Inches(5.6), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    err_code = (
        "_errors: ARRAY<STRUCT<\n"
        "    name     STRING,\n"
        "    message  STRING,\n"
        "    function STRING,\n"
        "    columns  ARRAY<STRING>\n"
        ">>"
    )
    code_block(sl, err_code, Inches(7.2), Inches(2.55), Inches(5.6), Inches(2.0), size=11)

    txb(sl, "No separate error log table needed — the error context travels with each bad record.",
        Inches(7.2), Inches(4.65), Inches(5.6), Inches(1.2),
        size=13, italic=True, color=DARK_GREY)

    bg = rect(sl, Inches(0.5), Inches(6.45), Inches(12.3), Inches(0.8), LIGHT_GREY)
    rect(sl, Inches(0.5), Inches(6.45), Pt(5), Inches(0.8), RED_ACCENT)
    txb(sl, "Without this, quarantine tables grow indefinitely with no prioritisation. "
            "The analyzer turns raw violations into actionable data quality incidents.",
        Inches(0.75), Inches(6.52), Inches(11.9), Inches(0.7),
        size=13, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 17 — Phase 5: Rule Effectiveness Dashboard
# ═══════════════════════════════════════════════════════════════════════════
def s17_effectiveness():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Phase 5 — Rule Effectiveness Dashboard")
    txb(sl, "Tracks per-rule violation counts after every batch and stream run.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38), size=15, italic=True, color=DARK_GREY)

    code = (
        "# ai_suggester/rule_metrics.py — called after every run\n"
        "violation_rows = (\n"
        "    quarantine_df\n"
        "    .select(F.explode('_errors').alias('_err'))\n"
        "    .groupBy(\n"
        "        F.col('_err.name').alias('rule_name'),\n"
        "        F.col('_err.function').alias('function'),\n"
        "    )\n"
        "    .agg(F.count('*').alias('violations'))\n"
        "    .collect()\n"
        ")\n"
        "# violation_pct = violations / total_records * 100\n"
        "# Appended to dq_rule_metrics Delta table — full run history preserved\n"
        "\n"
        "-- Effectiveness Summary SQL (notebooks/rule_effectiveness_report.py)\n"
        "SELECT rule_name, COUNT(DISTINCT run_id) AS run_count,\n"
        "       SUM(violations) AS total_violations,\n"
        "       ROUND(AVG(violation_pct), 2) AS avg_violation_pct,\n"
        "       MAX_BY(violations, recorded_at) AS last_run_violations\n"
        "FROM dq_rule_metrics\n"
        "GROUP BY rule_name, function, run_config_name\n"
        "ORDER BY total_violations DESC"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(7.5), Inches(4.85), size=10)

    txb(sl, "Four report sections:", Inches(8.2), Inches(2.1), Inches(4.6), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    headers = ["Section", "Business Question"]
    rows = [
        ["Effectiveness Summary", "Which rules catch most violations overall?"],
        ["Violation Trend",       "Is violation rate increasing or decreasing?"],
        ["Top Offenders",         "Top 10 rules by cumulative catch count"],
        ["Zero-fire Candidates",  "Which rules haven't fired in last N runs?"],
    ]
    table_slide(sl, headers, rows,
                Inches(8.2), Inches(2.55), Inches(4.6),
                col_widths=[Inches(2.1), Inches(2.5)],
                hdr_size=12, row_size=11)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 18 — AI Infusion Map
# ═══════════════════════════════════════════════════════════════════════════
def s18_ai_map():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "AI Infusion Map — Current State")

    headers = ["AI Component", "Module", "What It Does", "Business Use Case"]
    rows = [
        ["Statistical Rule Generator", "rule_generator.py",    "Derives rules from null %, min/max, cardinality",    "Fast baseline rules — no LLM cost"],
        ["LLM Rule Generator",         "llm_rule_suggester.py","LLM reasons over profile, adds domain rules",         "Catch semantic violations stats can't see"],
        ["Pluggable LLM Backend",      "llm_backend.py",       "Claude / Azure OpenAI / OpenRouter — env var switch", "Enterprise flexibility, no vendor lock-in"],
        ["Rule Validation Layer",      "llm_rule_suggester.py","Validates LLM JSON before staging",                   "Prevents malformed rules entering system"],
        ["Quarantine Analyser",        "quarantine_analyzer.py","Finds violation clusters in bad records",             "Prioritise which data issues to fix first"],
        ["Zero-fire Detection",        "rule_effectiveness_report.py","Flags rules with no violations in last N runs","Identifies stale rules for AI refresh"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.4), Inches(1.65), Inches(12.5),
                col_widths=[Inches(2.5), Inches(2.5), Inches(3.8), Inches(3.7)],
                hdr_size=13, row_size=11)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 19 — Human-in-the-Loop
# ═══════════════════════════════════════════════════════════════════════════
def s19_hitl():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "The Human-in-the-Loop Design")
    txb(sl, "AI generates.  Humans govern.  System learns.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38),
        size=18, bold=True, color=DARK_BLUE)

    BW, BH = Inches(2.8), Inches(0.72)

    # ── Row 1: AI Suggests → Staging Table ───────────────────────────────
    r1y = Inches(2.15)
    fbox(sl, "AI Suggests\n(statistical + LLM rules)",
         Inches(0.5), r1y, BW, BH, BOX_TEAL, TEAL_BORDER, TEAL_BORDER, 12.5)
    arrow_r(sl, Inches(3.42), r1y + Inches(0.15))
    fbox(sl, "dq_rules_staging\nstatus = pending",
         Inches(4.0), r1y, BW, BH, BOX_ORANGE, ORANGE_BRD, ORANGE_BRD, 12.5)
    arrow_r(sl, Inches(6.92), r1y + Inches(0.15))
    fbox(sl, "Engineer Reviews\napprove  /  reject",
         Inches(7.5), r1y, BW, BH, BOX_BLUE, DARK_BLUE, DARK_BLUE, 12.5, bold=True)

    # ── Down arrows from Review ───────────────────────────────────────────
    txb(sl, "↙", Inches(7.7), Inches(2.95), Inches(0.5), Inches(0.5),
        size=24, bold=True, color=GREEN_BORDER, align=PP_ALIGN.CENTER)
    txb(sl, "↘", Inches(9.8), Inches(2.95), Inches(0.5), Inches(0.5),
        size=24, bold=True, color=RED_ACCENT, align=PP_ALIGN.CENTER)

    # ── Row 2: Approved (left) | Rejected (right) ─────────────────────────
    r2y = Inches(3.52)
    fbox(sl, "APPROVED\ndq_rules_registry",
         Inches(6.5), r2y, Inches(2.4), BH, BOX_GREEN, GREEN_BORDER, GREEN_BORDER, 12.5, bold=True)
    fbox(sl, "REJECTED\nPreserved for\nfine-tuning",
         Inches(9.5), r2y, Inches(2.4), BH, BOX_RED, RED_ACCENT, RED_ACCENT, 12.5)

    # ── Down arrow from Approved → active in pipelines ────────────────────
    arrow_d(sl, Inches(7.52), Inches(4.3))
    fbox(sl, "Active in all future batch + streaming runs",
         Inches(5.7), Inches(4.82), Inches(4.5), Inches(0.58),
         BOX_BLUE, DARK_BLUE, DARK_BLUE, 12.5, bold=True)

    # ── Key points ────────────────────────────────────────────────────────
    txb(sl, "Why this matters architecturally:", Inches(0.5), Inches(2.9), Inches(4.8), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    items = [
        "Full audit trail: rule_id · suggested_at · reviewed_by · llm_backend",
        "Rejected rules preserved — future fine-tuning data",
        "Identical pattern to feature-flag promotion workflow",
        "Zero risk of hallucinated rule entering production",
    ]
    bullet_list(sl, items, Inches(0.5), Inches(3.35), Inches(4.8), Inches(2.0), size=13.5, space=10)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 20 — Architecture
# ═══════════════════════════════════════════════════════════════════════════
def s20_architecture():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Full System Architecture")

    # ── Outer Unity Catalog container ─────────────────────────────────────
    outer = sl.shapes.add_shape(1, Inches(0.35), Inches(1.65), Inches(12.6), Inches(5.6))
    outer.fill.solid(); outer.fill.fore_color.rgb = RGBColor(0xF0, 0xF4, 0xFA)
    outer.line.color.rgb = DARK_BLUE; outer.line.width = Pt(1.5)
    txb(sl, "DATABRICKS UNITY CATALOG  —  dqx_poc_dev / retail_dqx_poc",
        Inches(0.45), Inches(1.68), Inches(12.4), Inches(0.32),
        size=9.5, bold=True, color=DARK_BLUE, align=PP_ALIGN.CENTER)

    # ── External: Azure Event Hubs (top-left) & LLM APIs (top-right) ─────
    fbox(sl, "Azure Event Hubs\n(Kafka protocol)",
         Inches(0.5), Inches(2.08), Inches(2.2), Inches(0.6),
         RGBColor(0xFF,0xF3,0xE0), ORANGE_BRD, ORANGE_BRD, 10.5)
    fbox(sl, "LLM APIs\nClaude / Azure OpenAI",
         Inches(10.5), Inches(2.08), Inches(2.2), Inches(0.6),
         RGBColor(0xFF,0xF3,0xE0), ORANGE_BRD, ORANGE_BRD, 10.5)

    # Down arrows from external into processing row
    arrow_d(sl, Inches(1.44), Inches(2.72))
    arrow_d(sl, Inches(11.44), Inches(2.72))

    # ── Row 1: Config | Batch | Streaming | AI Layer ──────────────────────
    r1y = Inches(3.22)
    fbox(sl, "Config Layer\nconstants.py\nrules_*.yml",
         Inches(0.5), r1y, Inches(1.95), Inches(0.9),
         BOX_BLUE, DARK_BLUE, DARK_BLUE, 10)
    arrow_r(sl, Inches(2.55), r1y + Inches(0.2))
    fbox(sl, "Batch Pipeline\nbatch_dqx_\nvalidation.py",
         Inches(3.1), r1y, Inches(2.3), Inches(0.9),
         BOX_BLUE, DARK_BLUE, DARK_BLUE, 10)
    fbox(sl, "Streaming Pipeline\nkafka_stream_dqx_\nvalidation.py",
         Inches(5.6), r1y, Inches(2.5), Inches(0.9),
         BOX_BLUE, DARK_BLUE, DARK_BLUE, 10)
    fbox(sl, "AI Layer\nprofiler\nllm_backend\nrule_writer",
         Inches(10.4), r1y, Inches(2.1), Inches(0.9),
         BOX_TEAL, TEAL_BORDER, TEAL_BORDER, 10)
    arrow_l(sl, Inches(8.75), r1y + Inches(0.2))

    # Down arrows into DQX engine
    arrow_d(sl, Inches(4.12), Inches(4.18))
    arrow_d(sl, Inches(6.72), Inches(4.18))

    # ── Row 2: DQX Rule Engine (centred) ─────────────────────────────────
    r2y = Inches(4.68)
    fbox(sl, "DQX Rule Engine  —  apply_checks_by_metadata()",
         Inches(2.8), r2y, Inches(5.2), Inches(0.58),
         RGBColor(0xE0,0xE8,0xFF), DARK_BLUE, DARK_BLUE, 12, bold=True)

    # Down arrows into output tables
    arrow_d(sl, Inches(4.12), Inches(5.32))
    arrow_d(sl, Inches(6.72), Inches(5.32))

    # ── Row 3: Clean tables | Quarantine tables ───────────────────────────
    r3y = Inches(5.82)
    fbox(sl, "sales_clean\nstream_events_clean",
         Inches(2.8), r3y, Inches(2.3), Inches(0.7),
         BOX_GREEN, GREEN_BORDER, GREEN_BORDER, 10)
    fbox(sl, "sales_quarantine\nstream_events_quarantine",
         Inches(5.6), r3y, Inches(2.4), Inches(0.7),
         BOX_RED, RED_ACCENT, RED_ACCENT, 10)

    # Arrow from quarantine → metrics
    arrow_r(sl, Inches(8.1), r3y + Inches(0.16))
    fbox(sl, "dq_rule_metrics\ndq_rules_staging\ndq_rules_registry",
         Inches(8.65), r3y, Inches(2.3), Inches(0.7),
         BOX_ORANGE, ORANGE_BRD, DARK_BLUE, 10)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 21 — Delta Table Data Model
# ═══════════════════════════════════════════════════════════════════════════
def s21_delta_tables():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Delta Table Data Model")

    code = (
        "# config/constants.py — single source of truth for all table names\n"
        "CATALOG  = 'dqx_poc_dev'\n"
        "DATABASE = 'retail_dqx_poc'\n"
        "\n"
        "TABLE_SALES_CLEAN       = f'{CATALOG}.{DATABASE}.sales_clean'\n"
        "TABLE_SALES_QUARANTINE  = f'{CATALOG}.{DATABASE}.sales_quarantine'\n"
        "TABLE_STREAM_CLEAN      = f'{CATALOG}.{DATABASE}.stream_events_clean'\n"
        "TABLE_STREAM_QUARANTINE = f'{CATALOG}.{DATABASE}.stream_events_quarantine'\n"
        "TABLE_RULES             = f'{CATALOG}.{DATABASE}.dq_rules_registry'\n"
        "TABLE_RULES_STAGING     = f'{CATALOG}.{DATABASE}.dq_rules_staging'\n"
        "TABLE_RULE_METRICS      = f'{CATALOG}.{DATABASE}.dq_rule_metrics'"
    )
    code_block(sl, code, Inches(0.5), Inches(1.65), Inches(12.3), Inches(2.55), size=11)

    headers = ["Table", "Key Columns", "Written By"]
    rows = [
        ["sales_clean",             "all source columns",                        "Batch pipeline"],
        ["sales_quarantine",        "source + _errors ARRAY<STRUCT>",            "Batch pipeline"],
        ["stream_events_clean",     "all source columns",                        "Streaming pipeline"],
        ["stream_events_quarantine","source + _errors ARRAY<STRUCT>",            "Streaming pipeline"],
        ["dq_rules_registry",       "name, check STRUCT, criticality",           "rule_writer.promote_rule()"],
        ["dq_rules_staging",        "status, llm_backend, confidence, reasoning","AI Rule Suggester"],
        ["dq_rule_metrics",         "run_id, rule_name, violations, violation_pct","After every run"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.5), Inches(4.3), Inches(12.3),
                col_widths=[Inches(3.1), Inches(4.5), Inches(4.7)],
                hdr_size=13, row_size=11)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 22 — Databricks Jobs
# ═══════════════════════════════════════════════════════════════════════════
def s22_jobs():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Databricks Jobs — Operational View")

    headers = ["Job Name", "Trigger", "Tasks"]
    rows = [
        ["[RTM] Seed Rules Registry",         "Once per environment",
         "seed_rules — loads configs/rules_*.yml into dq_rules_registry"],
        ["[RTM] Batch DQX Pipeline",          "On-demand / scheduled",
         "6-task DAG: stage → validate → suggest ∥ analyze → report → alerts"],
        ["[RTM] Kafka Streaming Pipeline",    "Continuous (runs until cancelled)",
         "stream_and_validate + produce_events, started together; readiness handshake"],
        ["[RTM] DQ Alert Check (Streaming)",  "Every 15 minutes",
         "check_alerts — spike detection on streaming rules"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.5), Inches(1.65), Inches(12.3),
                col_widths=[Inches(3.5), Inches(3.0), Inches(5.8)],
                hdr_size=14, row_size=12)

    bg = rect(sl, Inches(0.5), Inches(5.45), Inches(12.3), Inches(1.5), LIGHT_GREY)
    rect(sl, Inches(0.5), Inches(5.45), Pt(5), Inches(1.5), RED_ACCENT)
    txb(sl, "Six manually triggered jobs were chained into four orchestrated ones. All are defined in "
            "databricks.yml as a Databricks Asset Bundle, including the cluster libraries "
            "(pipeline_libraries), so a new environment needs no manual setup: databricks bundle deploy",
        Inches(0.75), Inches(5.55), Inches(11.9), Inches(1.3),
        size=14, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 23 — POC vs Production
# ═══════════════════════════════════════════════════════════════════════════
def s23_poc_vs_prod():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "POC vs. Production — Honest Assessment")

    headers = ["Dimension", "POC (Current State)", "Production Path"]
    rows = [
        ["Data",       "Generated synthetic retail data",   "Real transaction / event feeds from source systems"],
        ["Scale",      "Thousands of records",              "Billions of records — Spark scales horizontally, no code changes"],
        ["Rules",      "12 seeded + AI-suggested", "Hundreds of rules across dozens of datasets"],
        ["Streaming",  "Single Kafka topic",                "Multiple topics, partitioned, multi-tenant"],
        ["LLM Cost",   "Low (POC usage)",                   "Prompt caching implemented; cost monitoring needed"],
        ["Security",   "Secret scope backed by Azure Key Vault", "Managed identities, rotation policies, per-environment vaults"],
        ["Governance", "Unity Catalog in place",            "Column-level access controls + data lineage"],
        ["Monitoring", "Rule metrics Delta table",           "Connect to Datadog / Grafana / Azure Monitor"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.4), Inches(1.65), Inches(12.5),
                col_widths=[Inches(1.8), Inches(3.2), Inches(7.5)],
                hdr_size=13, row_size=11)

    bg = rect(sl, Inches(0.4), Inches(6.35), Inches(12.5), Inches(0.85), LIGHT_GREY)
    rect(sl, Inches(0.4), Inches(6.35), Pt(5), Inches(0.85), RED_ACCENT)
    txb(sl, "Every technology choice is production-grade. Databricks, Delta Lake, Kafka, and Unity Catalog "
            "are what we would use in production. The POC proves the pattern — plugging in real data is a "
            "configuration exercise, not a re-architecture.",
        Inches(0.65), Inches(6.42), Inches(12.1), Inches(0.75),
        size=13, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 24 — What We Proved
# ═══════════════════════════════════════════════════════════════════════════
def s24_outcomes():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Key POC Outcomes")

    outcomes = [
        ("1.  AI can write meaningful DQ rules from data alone",
         "The LLM produced structurally valid, business-relevant rules from a column profile — no domain hints given. "
         "A built-in translation maps them to DQX-native names on promotion, so approved rules run without manual editing."),
        ("2.  The human-in-the-loop governance model works",
         "Staging → review → registry promotion gives engineers full control. "
         "Approve/reject audit trail preserved for future model fine-tuning."),
        ("3.  Batch and streaming share a single rule set",
         "A rule approved in dq_rules_registry applies identically to daily batch runs and Kafka micro-batches — "
         "one source of truth, no duplication."),
        ("4.  Rule effectiveness tracking reveals rule health",
         "Zero-fire detection correctly surfaces rules that have stopped being relevant — "
         "impossible to get from a static rules list."),
        ("5.  Pluggable LLM backend delivers enterprise flexibility",
         "Switching from Claude to Azure OpenAI required only setting LLM_BACKEND=azure_openai — "
         "no code modification."),
    ]

    y = Inches(1.65)
    for title, body in outcomes:
        txb(sl, title, Inches(0.6), y, Inches(12.1), Inches(0.35),
            size=14, bold=True, color=DARK_BLUE)
        txb(sl, body, Inches(0.9), y + Inches(0.33), Inches(11.8), Inches(0.35),
            size=13, color=DARK_GREY)
        y += Inches(0.92)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 25 — Test Coverage
# ═══════════════════════════════════════════════════════════════════════════
def s25_tests():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Test Coverage and Code Quality")
    txb(sl, "105 unit tests across 9 files — 95 run without Spark; the 10 profiler tests need a local Spark session:",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38), size=15, bold=True, color=DARK_BLUE)

    headers = ["Test File", "Tests", "What Is Covered"]
    rows = [
        ["tests/test_profiler.py",            "10", "Column statistics, null detection, distinct values (local Spark)"],
        ["tests/test_rule_generator.py",      "25", "Statistical rules, code-column skipping, ceiling rounding"],
        ["tests/test_rule_writer.py",         "11", "DQX-name translation on promotion, duplicate filtering"],
        ["tests/test_rule_seed.py",           "7",  "YAML → DQX-native check conversion for every rule type"],
        ["tests/test_quarantine_analyzer.py", "14", "Prompt construction, JSON parsing, rule validation"],
        ["tests/test_rule_metrics.py",        "9",  "Metrics building, violation % calculation, write path"],
        ["tests/test_alerting.py",            "11", "Spike detection, Slack payload and delivery, orchestration"],
        ["tests/test_config.py",              "9",  "Settings loading and required-field validation"],
        ["tests/test_event_generator.py",     "9",  "Event schema and bad-data injection rates"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.5), Inches(2.05), Inches(12.3),
                col_widths=[Inches(4.2), Inches(1.0), Inches(7.1)],
                hdr_size=11, row_size=10.5)

    code = "$ pytest tests/\n...\n95 passed (10 Spark-only tests deselected)"
    code_block(sl, code, Inches(0.5), Inches(6.1), Inches(5.6), Inches(1.0), size=12)

    txb(sl, "Design principle:", Inches(6.4), Inches(6.05), Inches(6.4), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    txb(sl, "Pure-Python business logic is separated from Spark code, so most tests run offline "
            "without a cluster. Spark calls are mocked at the boundary.",
        Inches(6.4), Inches(6.42), Inches(6.4), Inches(0.85),
        size=12, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 26 — Roadmap table
# ═══════════════════════════════════════════════════════════════════════════
def s26_roadmap():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Known AI Infusion Opportunities — Roadmap")

    headers = ["Priority", "Opportunity", "AI Infusion Point", "Business Value"]
    rows = [
        ["A ✅", "Automated Alerting",       "Built: spike detection + log + Slack. Next: AI severity", "On-call team notified only when it matters"],
        ["B",   "Data Drift Detection",     "ML detects statistical distribution shift run-over-run",   "Catch silent degradation before rules fail"],
        ["C",   "DQ Scorecard",             "Aggregate quality scores per domain / table over time",    "Executive visibility into data health by area"],
        ["D",   "Rule Governance Workflow", "AI suggests rule retirement based on effectiveness data",  "Fully automated rule lifecycle management"],
        ["E",   "Multi-table / Cross-domain","Referential integrity + cross-table consistency checks",  "Catches issues single-table rules cannot see"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.4), Inches(1.65), Inches(12.5),
                col_widths=[Inches(1.0), Inches(2.8), Inches(5.0), Inches(3.7)],
                hdr_size=13, row_size=12)

    bg = rect(sl, Inches(0.4), Inches(5.55), Inches(12.5), Inches(0.85), LIGHT_GREY)
    rect(sl, Inches(0.4), Inches(5.55), Pt(5), Inches(0.85), RED_ACCENT)
    txb(sl, "Stage A is delivered: spike detection and the audit log are verified on Databricks; Slack "
            "delivery is ready once the webhook secret is configured. Next: choose among B–E, and add AI "
            "severity and plain-English explanations to A.",
        Inches(0.65), Inches(5.62), Inches(12.1), Inches(0.75),
        size=14, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 27 — Alerting detail
# ═══════════════════════════════════════════════════════════════════════════
def s27_alerting():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Stage A — Automated Alerting (Delivered)")

    code = (
        "  dq_rule_metrics (populated after every batch / stream run)\n"
        "           │\n"
        "           ▼\n"
        "  Alert check job (dq_alert_check.py)\n"
        "  ┌──────────────────────────────────────────────────────┐\n"
        "  │  baseline = avg violation_pct of the last N runs     │\n"
        "  │  spike if last run > max(2.5 × baseline, 5%)         │\n"
        "  │  needs at least 3 prior runs for the rule            │\n"
        "  │  never re-alert the same rule + run                  │\n"
        "  └──────────────────────────────────────────────────────┘\n"
        "           ├──► Slack webhook notification\n"
        "           ├──► dq_alert_log table (full audit trail)\n"
        "           └──► native Databricks SQL Alert (alternative path)"
    )
    code_block(sl, code, Inches(0.5), Inches(1.65), Inches(7.2), Inches(4.0), size=11.5)

    txb(sl, "Next: AI infusion in the alert message", Inches(7.9), Inches(1.65), Inches(5.0), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    txb(sl, "Not just:",
        Inches(7.9), Inches(2.1), Inches(5.0), Inches(0.35), size=13, color=DARK_GREY)
    code_block(sl, "\"rule X violated 12%\"",
               Inches(7.9), Inches(2.5), Inches(5.0), Inches(0.55), size=11)
    txb(sl, "But:",
        Inches(7.9), Inches(3.15), Inches(5.0), Inches(0.35), size=13, color=DARK_GREY)
    code_block(sl, "\"Price validation for division '540'\nfailed at 12% — this pattern has\nnot been seen before and may\nindicate a feed config issue.\"",
               Inches(7.9), Inches(3.55), Inches(5.0), Inches(1.5), size=11)

    bg = rect(sl, Inches(0.5), Inches(5.85), Inches(12.3), Inches(0.85), LIGHT_GREY)
    rect(sl, Inches(0.5), Inches(5.85), Pt(5), Inches(0.85), RED_ACCENT)
    txb(sl, "Spike detection and logging are verified end to end on Databricks; Slack delivery is ready "
            "once the webhook secret is configured. The LLM backend is already in place for the "
            "severity and explanation step.",
        Inches(0.75), Inches(5.93), Inches(11.9), Inches(0.75),
        size=14, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 28 — The vision realised
# ═══════════════════════════════════════════════════════════════════════════
def s28_vision_realised():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "The Vision, Realised")

    # ── TODAY column (left) ───────────────────────────────────────────────
    col_y = Inches(1.65); col_h = Inches(5.0); col_w = Inches(5.7)
    today = sl.shapes.add_shape(1, Inches(0.4), col_y, col_w, col_h)
    today.fill.solid(); today.fill.fore_color.rgb = BOX_GREEN
    today.line.color.rgb = GREEN_BORDER; today.line.width = Pt(1.5)

    txb(sl, "TODAY — Complete", Inches(0.5), col_y + Inches(0.12), col_w - Inches(0.1), Inches(0.42),
        size=16, bold=True, color=GREEN_BORDER, align=PP_ALIGN.CENTER)
    sep = sl.shapes.add_shape(1, Inches(0.6), col_y + Inches(0.55), col_w - Inches(0.4), Pt(1))
    sep.fill.solid(); sep.fill.fore_color.rgb = GREEN_BORDER; sep.line.fill.background()

    today_items = [
        "✅  AI generates DQ rules from data",
        "✅  Batch + stream data validated",
        "✅  Quarantine records analysed",
        "✅  Rule effectiveness tracked",
        "✅  Human governance layer",
        "✅  Spike alerting with audit log",
    ]
    bullet_list(sl, today_items, Inches(0.55), col_y + Inches(0.7), col_w - Inches(0.2),
                Inches(4.0), size=15, space=16, color=RGBColor(0x0D, 0x52, 0x2A), bullet_char="")

    # ── TOMORROW column (right) ───────────────────────────────────────────
    tom = sl.shapes.add_shape(1, Inches(7.2), col_y, col_w, col_h)
    tom.fill.solid(); tom.fill.fore_color.rgb = BOX_BLUE
    tom.line.color.rgb = DARK_BLUE; tom.line.width = Pt(1.5)

    txb(sl, "TOMORROW — Production AI Infusion", Inches(7.3), col_y + Inches(0.12),
        col_w - Inches(0.1), Inches(0.42), size=16, bold=True, color=DARK_BLUE, align=PP_ALIGN.CENTER)
    sep2 = sl.shapes.add_shape(1, Inches(7.4), col_y + Inches(0.55), col_w - Inches(0.4), Pt(1))
    sep2.fill.solid(); sep2.fill.fore_color.rgb = DARK_BLUE; sep2.line.fill.background()

    tomorrow_items = [
        "+  AI detects when rules go stale",
        "+  AI classifies alert severity",
        "+  AI explains root cause in plain English",
        "+  AI suggests rule updates on drift",
        "+  AI scorecard for executive reporting",
    ]
    bullet_list(sl, tomorrow_items, Inches(7.35), col_y + Inches(0.7), col_w - Inches(0.2),
                Inches(4.0), size=15, space=16, color=RGBColor(0x0A, 0x22, 0x44), bullet_char="")

    # ── Arrow between columns ─────────────────────────────────────────────
    txb(sl, "→", Inches(6.2), col_y + Inches(2.0), Inches(0.8), Inches(0.7),
        size=36, bold=True, color=DARK_BLUE, align=PP_ALIGN.CENTER)

    # ── Result banner ─────────────────────────────────────────────────────
    banner = sl.shapes.add_shape(1, Inches(0.4), Inches(6.82), Inches(12.5), Inches(0.5))
    banner.fill.solid(); banner.fill.fore_color.rgb = DARK_BLUE; banner.line.fill.background()
    txb(sl, "Result:  A data quality platform that improves itself — governed by humans, powered by AI, built on Databricks.",
        Inches(0.4), Inches(6.85), Inches(12.5), Inches(0.44),
        size=13.5, bold=True, color=WHITE, align=PP_ALIGN.CENTER)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 29 — What we are asking for
# ═══════════════════════════════════════════════════════════════════════════
def s29_ask():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "What We Are Asking For")
    txb(sl, "Today's ask:  Validate the POC architecture and AI infusion approach.",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38),
        size=17, bold=True, color=DARK_BLUE)

    items = [
        "Does the human-in-the-loop governance model align with enterprise AI adoption standards?",
        "Is the pluggable LLM backend pattern the right approach for multi-cloud / multi-provider deployments?",
        "Which next-stage opportunity (Drift / Scorecard / Governance) aligns best with delivery priorities?",
        "What production integration patterns (source systems, security, monitoring) should shape the next phase?",
    ]
    bullet_list(sl, items, Inches(0.7), Inches(2.1), Inches(12.0), Inches(2.8), size=17, space=14)

    bg = rect(sl, Inches(0.5), Inches(5.3), Inches(12.3), Inches(1.6), LIGHT_GREY)
    rect(sl, Inches(0.5), Inches(5.3), Pt(5), Inches(1.6), RED_ACCENT)
    txb(sl, "The POC proves the pattern works. Every component is built, tested, and running on Databricks.\n"
            "The question is no longer can we do this — it is which direction do we take it next.",
        Inches(0.75), Inches(5.42), Inches(11.9), Inches(1.4),
        size=17, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 30 — Thank you
# ═══════════════════════════════════════════════════════════════════════════
def s30_thankyou():
    sl = prs.slides.add_slide(BLANK)
    rect(sl, 0, 0, W, H, MID_BLUE)
    rect(sl, 0, 0, W, Inches(0.12), RED_ACCENT)
    rect(sl, 0, H - Inches(0.12), W, Inches(0.12), RED_ACCENT)

    txb(sl, "Thank You", Inches(1), Inches(2.5), Inches(11.33), Inches(1.0),
        size=52, bold=True, color=RED_ACCENT, align=PP_ALIGN.CENTER)
    txb(sl, "Questions & Discussion",
        Inches(1), Inches(3.7), Inches(11.33), Inches(0.7),
        size=26, color=LIGHT_TEAL, align=PP_ALIGN.CENTER)
    txb(sl, "Databricks DQX POC  —  AI Infusion in Delivery",
        Inches(1), Inches(4.7), Inches(11.33), Inches(0.5),
        size=16, color=WHITE, align=PP_ALIGN.CENTER)
    txb(sl, "5 phases + alerting complete  ·  105 unit tests  ·  Production-grade architecture  ·  Ready for next phase",
        Inches(1), Inches(5.2), Inches(11.33), Inches(0.5),
        size=13, color=MED_GREY, align=PP_ALIGN.CENTER)


# ── Build all slides ──────────────────────────────────────────────────────────
s01_title()
s02_agenda()
divider("Part 1: The Problem", "Why data quality in retail needs to be smarter")
s04_challenge()
s05_problem()
s06_vision_need()
divider("Part 2: The Vision", "An AI-infused system that writes, validates, and improves its own DQ rules")
s07_ai_vision()
divider("Part 3: What We Built", "A working, end-to-end POC on Databricks")
s08_poc_overview()
s09_tech_stack()
divider("Part 4: Phase-by-Phase Walkthrough", "")
s10_phase1()
s11_phase2()
s12_phase3_overview()
s13_llm_backend()
s14_stat_rules()
s15_human_review()
s16_quarantine()
s17_effectiveness()
divider("Part 5: AI in Action", "Where intelligence is infused — and what business problem each solves")
s18_ai_map()
s19_hitl()
divider("Part 6: Architecture Deep Dive", "")
s20_architecture()
s21_delta_tables()
s22_jobs()
divider("Part 7: Production Considerations", "From POC to enterprise scale")
s23_poc_vs_prod()
divider("Part 8: What We Proved", "")
s24_outcomes()
s25_tests()
divider("Part 9: AI Infusion Opportunities", "Where this platform goes next")
s26_roadmap()
s27_alerting()
s28_vision_realised()
s29_ask()
s30_thankyou()

import os
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "AI_Infusion_Delivery_Assessment.pptx")
prs.save(out)
print(f"Saved: {out}  ({prs.slides.__len__()} slides)")

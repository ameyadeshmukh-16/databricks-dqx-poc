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

    code = (
        "              TODAY                              TARGET\n"
        "    ┌──────────────────────┐         ┌──────────────────────────────┐\n"
        "    │  Data Engineer       │         │  AI analyses data profile    │\n"
        "    │  writes rules        │  →→→→   │  suggests rules              │\n"
        "    │  manually, reactively│         │  learns from violations      │\n"
        "    │  no effectiveness    │         │  continuously self-improves  │\n"
        "    │  feedback            │         │  governed by humans          │\n"
        "    └──────────────────────┘         └──────────────────────────────┘\n"
        "\n"
        "       Reactive, slow, brittle          Proactive, adaptive, governed"
    )
    code_block(sl, code, Inches(0.6), Inches(1.65), Inches(12.1), Inches(3.35), size=11.5)

    txb(sl, "Three shifts required:", Inches(0.7), Inches(5.15), Inches(12.0), Inches(0.4),
        size=17, bold=True, color=DARK_BLUE)
    items = [
        "Batch → Real-time  —  Catch bad data as it arrives, not the next morning",
        "Manual → AI-Generated  —  Let AI analyse the data and write the rules",
        "Static → Self-Improving  —  Rules that evolve as data patterns change",
    ]
    bullet_list(sl, items, Inches(0.7), Inches(5.55), Inches(12.0), Inches(1.7),
                size=16, space=7)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 7 — The AI Infusion Vision
# ═══════════════════════════════════════════════════════════════════════════
def s07_ai_vision():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "The AI Infusion Vision")

    code = (
        "┌────────────────────────────────────────────────────────────────────────┐\n"
        "│                       CONTINUOUS FEEDBACK LOOP                          │\n"
        "│                                                                          │\n"
        "│   Raw Data ──► DQ Validation ──► Quarantine Bad Records                 │\n"
        "│       ▲              │                     │                            │\n"
        "│       │              │                     ▼                            │\n"
        "│  Rules Registry ◄── AI Rule ◄── AI Analyses Patterns in bad records    │\n"
        "│  (promoted rules)   Suggester                                           │\n"
        "│       │                                    │                            │\n"
        "│       │             Rule Effectiveness ◄───┘                            │\n"
        "│       └─────────────  Dashboard                                         │\n"
        "│                    (which rules fire, which don't)                      │\n"
        "└────────────────────────────────────────────────────────────────────────┘"
    )
    code_block(sl, code, Inches(0.6), Inches(1.65), Inches(12.1), Inches(4.1), size=11)

    txb(sl, "AI is not a bolt-on — it is the feedback mechanism that makes the system self-improving.",
        Inches(0.7), Inches(5.9), Inches(12.0), Inches(0.7),
        size=17, bold=True, color=DARK_BLUE, italic=True)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 8 — POC Overview
# ═══════════════════════════════════════════════════════════════════════════
def s08_poc_overview():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "POC Overview — Five Working Phases")

    headers = ["#", "Phase", "What It Does", "Status"]
    rows = [
        ["1", "Batch DQ Validation",        "Validates daily transaction / product data against rules",     "✅ Complete"],
        ["2", "Real-time Streaming",         "Validates live event streams via Kafka / Event Hubs",         "✅ Complete"],
        ["3", "AI Rule Suggester",           "LLM analyses data profile and generates DQ rules",            "✅ Complete"],
        ["4", "Quarantine Analyzer",         "Surfaces patterns in bad records, root-cause insights",        "✅ Complete"],
        ["5", "Rule Effectiveness Dashboard","Tracks which rules fire, trends, zero-fire candidates",        "✅ Complete"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.5), Inches(1.65), Inches(12.3),
                col_widths=[Inches(0.5), Inches(2.6), Inches(6.6), Inches(2.6)],
                hdr_size=14, row_size=13)

    bg = rect(sl, Inches(0.5), Inches(5.55), Inches(12.3), Inches(1.4), LIGHT_GREY)
    rect(sl, Inches(0.5), Inches(5.55), Pt(5), Inches(1.4), RED_ACCENT)
    txb(sl, "All five phases are built, integrated, and running on Databricks. The POC uses generated "
            "retail data (sales transactions + event streams) designed to mirror real-world data patterns at scale.",
        Inches(0.75), Inches(5.65), Inches(11.9), Inches(1.2),
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

    code = (
        "Step 1: Profile      Step 2: Prompt           Step 3: Generate\n"
        "────────────         ──────────────           ────────────────\n"
        "Analyse column  ──►  Build structured    ──►  LLM returns\n"
        "stats: min, max,     prompt with profile      candidate rules\n"
        "nulls, cardinality,  + domain context         in JSON format\n"
        "distributions\n"
        "                                                     │\n"
        "Step 4: Validate     Step 5: Human Review            ▼\n"
        "────────────────     ─────────────────        dq_rules_staging\n"
        "Parse + validate ◄── Engineer: approve ◄───── (status=pending)\n"
        "rule structure       or reject\n"
        "     │\n"
        "     ▼\n"
        "dq_rules_registry  ← promoted and active in all future runs"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(12.3), Inches(4.35), size=11)

    txb(sl, "Two complementary rule sources:  Statistical rules (pure maths, no LLM) "
            "+  LLM rules (domain reasoning on top)",
        Inches(0.5), Inches(6.6), Inches(12.3), Inches(0.6),
        size=15, bold=True, color=DARK_BLUE)


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
        "    if is_numeric and col_min is not None and col_min > 0:\n"
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
        "approve_rule(rule_id, spark)   # inserts into dq_rules_registry\n"
        "reject_rule(rule_id, spark)    # marks status='rejected', preserved for fine-tuning"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(12.3), Inches(4.6), size=10.5)

    txb(sl, "Full audit trail: rule_id · suggested_at · reviewed_at · reviewed_by · llm_backend",
        Inches(0.5), Inches(6.85), Inches(12.3), Inches(0.4),
        size=14, bold=True, color=DARK_BLUE)


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

    code = (
        "                  ┌──────────────────┐\n"
        "  AI Suggests ──► │  dq_rules_       │ ──► Engineer reviews\n"
        "  (statistical    │  staging         │     approve / reject\n"
        "   + LLM rules)   │  status=pending  │          │\n"
        "                  └──────────────────┘          │\n"
        "                                                ▼\n"
        "                                       ┌──────────────────┐\n"
        "                                       │  dq_rules_       │\n"
        "                                       │  registry        │  ← single source of truth\n"
        "                                       │  status=approved │    for batch + streaming\n"
        "                                       └──────────────────┘"
    )
    code_block(sl, code, Inches(0.5), Inches(2.1), Inches(12.3), Inches(3.3), size=11.5)

    txb(sl, "Why this matters architecturally:", Inches(0.6), Inches(5.55), Inches(12.0), Inches(0.38),
        size=15, bold=True, color=DARK_BLUE)
    items = [
        "Full audit trail: rule_id · suggested_at · reviewed_at · reviewed_by · llm_backend",
        "Rejected rules preserved with reason — future fine-tuning data",
        "Promotion pattern is identical to a feature-flag promotion workflow",
        "Zero risk of a hallucinated rule silently entering production",
    ]
    bullet_list(sl, items, Inches(0.7), Inches(5.95), Inches(12.0), Inches(1.35), size=15, space=6)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 20 — Architecture
# ═══════════════════════════════════════════════════════════════════════════
def s20_architecture():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Full System Architecture")

    code = (
        "┌─────────────────────────────────────────────────────────────────────────┐\n"
        "│                  DATABRICKS UNITY CATALOG                                │\n"
        "│    Catalog: dqx_poc_cluster_7405619883480512  /  DB: retail_dqx_poc     │\n"
        "│                                                                           │\n"
        "│  ┌──────────────┐  ┌──────────────────────────────────────────────┐     │\n"
        "│  │ Config Layer │  │             PROCESSING LAYER                  │     │\n"
        "│  │ constants.py │─►│  Batch Pipeline        Streaming Pipeline     │     │\n"
        "│  │ rules_*.yml  │  │  batch_dqx_            kafka_stream_dqx_     │     │\n"
        "│  └──────────────┘  │  validation.py         validation.py         │     │\n"
        "│                    │         └──────────┬───────────────┘          │     │\n"
        "│  ┌──────────────┐  │                   ▼                           │     │\n"
        "│  │   AI Layer   │  │         DQX Rule Engine                       │     │\n"
        "│  │  profiler    │─►│    apply_checks_by_metadata()                 │     │\n"
        "│  │  llm_backend │  │                   │                           │     │\n"
        "│  │  rule_writer │  │       ┌───────────┴───────────┐              │     │\n"
        "│  └──────────────┘  │  sales_clean /         sales_quarantine /    │     │\n"
        "│                    │  stream_events_clean   stream_events_quarant  │     │\n"
        "│  ┌──────────────┐  │                   │                           │     │\n"
        "│  │Metrics Layer │◄─│            dq_rule_metrics                    │     │\n"
        "│  │ rule_metrics │  │            dq_rules_staging                   │     │\n"
        "│  └──────────────┘  │            dq_rules_registry                  │     │\n"
        "│                    └──────────────────────────────────────────────┘     │\n"
        "└─────────────────────────────────────────────────────────────────────────┘\n"
        "        ▲                                                   ▲\n"
        "  Azure Event Hubs (Kafka)                          LLM APIs\n"
        "                                               (Claude / Azure OpenAI)"
    )
    code_block(sl, code, Inches(0.4), Inches(1.65), Inches(12.5), Inches(5.7), size=9.5)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 21 — Delta Table Data Model
# ═══════════════════════════════════════════════════════════════════════════
def s21_delta_tables():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Delta Table Data Model")

    code = (
        "# config/constants.py — single source of truth for all table names\n"
        "CATALOG  = 'dqx_poc_cluster_7405619883480512'\n"
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

    headers = ["Job Name", "Trigger", "Notebook / Script"]
    rows = [
        ["[RTM] Setup Delta Tables",          "Once (infra)",         "notebooks/setup_delta_tables.py"],
        ["[RTM] Batch DQX Validation",        "Scheduled / on-demand","notebooks/batch_dqx_validation.py"],
        ["[RTM] Kafka Stream DQX Validation", "Continuous",           "notebooks/kafka_stream_dqx_validation.py"],
        ["[RTM] AI Rule Suggester",           "On-demand",            "notebooks/ai_rule_suggester.py"],
        ["[RTM] Quarantine Analyzer",         "On-demand / scheduled","notebooks/quarantine_analyzer.py"],
        ["[RTM] Rule Effectiveness Report",   "On-demand / scheduled","notebooks/rule_effectiveness_report.py"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.5), Inches(1.65), Inches(12.3),
                col_widths=[Inches(4.2), Inches(2.8), Inches(5.3)],
                hdr_size=14, row_size=13)

    bg = rect(sl, Inches(0.5), Inches(5.45), Inches(12.3), Inches(1.0), LIGHT_GREY)
    rect(sl, Inches(0.5), Inches(5.45), Pt(5), Inches(1.0), RED_ACCENT)
    txb(sl, "All jobs defined in databricks.yml as a Databricks Asset Bundle — infrastructure as code, "
            "deployable across dev / staging / prod with a single CLI command: databricks bundle deploy",
        Inches(0.75), Inches(5.55), Inches(11.9), Inches(0.85),
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
        ["Rules",      "~15 seeded + AI-suggested",         "Hundreds of rules across dozens of datasets"],
        ["Streaming",  "Single Kafka topic",                "Multiple topics, partitioned, multi-tenant"],
        ["LLM Cost",   "Low (POC usage)",                   "Prompt caching implemented; cost monitoring needed"],
        ["Security",   "Databricks Secrets",                "Enterprise key vault (Azure Key Vault / HashiCorp)"],
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
         "Rules were immediately executable by DQX with zero manual editing."),
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
        y += Inches(0.82)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 25 — Test Coverage
# ═══════════════════════════════════════════════════════════════════════════
def s25_tests():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Test Coverage and Code Quality")
    txb(sl, "48 unit tests across 4 files — all passing locally, no Spark cluster required:",
        Inches(0.6), Inches(1.6), Inches(12.0), Inches(0.38), size=15, bold=True, color=DARK_BLUE)

    headers = ["Test File", "Tests", "What Is Covered"]
    rows = [
        ["tests/test_profiler.py",       "12", "Column statistics, null detection, distribution analysis"],
        ["tests/test_rule_generator.py", "18", "Prompt construction, JSON parse, rule validation"],
        ["tests/test_rule_metrics.py",   "9",  "Metrics dict building, violation % calc, write path"],
        ["tests/test_rule_writer.py",    "9",  "Staging insert, approve/reject lifecycle, registry promotion"],
    ]
    table_slide(sl, headers, rows,
                Inches(0.5), Inches(2.1), Inches(12.3),
                col_widths=[Inches(4.2), Inches(1.1), Inches(7.0)],
                hdr_size=14, row_size=13)

    code = "$ pytest tests/ -v\n...\n48 passed in 3.21s"
    code_block(sl, code, Inches(0.5), Inches(3.9), Inches(5.0), Inches(1.2), size=13)

    txb(sl, "Design principle:", Inches(6.2), Inches(3.9), Inches(6.6), Inches(0.38),
        size=14, bold=True, color=DARK_BLUE)
    txb(sl, "Pure-Python business logic is extracted from Spark-dependent code, enabling "
            "fast offline tests without a running cluster. Spark interactions are mocked at "
            "the boundary using unittest.mock.",
        Inches(6.2), Inches(4.32), Inches(6.6), Inches(1.4),
        size=13, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 26 — Roadmap table
# ═══════════════════════════════════════════════════════════════════════════
def s26_roadmap():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Known AI Infusion Opportunities — Roadmap")

    headers = ["Priority", "Opportunity", "AI Infusion Point", "Business Value"]
    rows = [
        ["A ★", "Automated Alerting",       "AI classifies violation severity → routes alert",         "On-call team notified only when it matters"],
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
    txb(sl, "Recommended next step: A (Alerting) — highest immediate business value, lowest lift. "
            "Wires the existing dq_rule_metrics table to a notification layer. No new ML needed.",
        Inches(0.65), Inches(5.62), Inches(12.1), Inches(0.75),
        size=14, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 27 — Alerting detail
# ═══════════════════════════════════════════════════════════════════════════
def s27_alerting():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "Opportunity A — Automated Alerting")

    code = (
        "  dq_rule_metrics (populated after every batch / stream run)\n"
        "           │\n"
        "           ▼\n"
        "  Alert Evaluation Engine\n"
        "  ┌──────────────────────────────────────────────────────┐\n"
        "  │  if violation_pct > threshold for rule X             │\n"
        "  │  → classify severity (info / warning / critical)     │\n"
        "  │  → deduplicate (don't re-alert same issue twice)     │\n"
        "  │  → route to appropriate channel                      │\n"
        "  └──────────────────────────────────────────────────────┘\n"
        "           ├──► Slack / Teams notification\n"
        "           ├──► PagerDuty / OpsGenie (critical only)\n"
        "           └──► dq_alerts table (full audit log)"
    )
    code_block(sl, code, Inches(0.5), Inches(1.65), Inches(7.2), Inches(4.0), size=11.5)

    txb(sl, "AI infusion in the alert message:", Inches(7.9), Inches(1.65), Inches(5.0), Inches(0.38),
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
    txb(sl, "Existing infrastructure (dq_rule_metrics, LLM backend) is already in place. "
            "Alerting is an orchestration layer on top — estimated 1 sprint to ship.",
        Inches(0.75), Inches(5.93), Inches(11.9), Inches(0.75),
        size=14, italic=True, color=DARK_GREY)


# ═══════════════════════════════════════════════════════════════════════════
# SLIDE 28 — The vision realised
# ═══════════════════════════════════════════════════════════════════════════
def s28_vision_realised():
    sl = prs.slides.add_slide(BLANK)
    slide_title(sl, "The Vision, Realised")

    code = (
        "┌──────────────────────────────────────────────────────────────────────┐\n"
        "│                                                                        │\n"
        "│   TODAY (POC — 5 phases complete)  TOMORROW (Production AI Infusion)  │\n"
        "│   ─────────────────────────────    ───────────────────────────────    │\n"
        "│                                                                        │\n"
        "│   ✅ AI generates DQ rules          + AI detects when rules go stale  │\n"
        "│   ✅ Batch + stream validated        + AI classifies alert severity    │\n"
        "│   ✅ Quarantine records analysed     + AI explains root cause in eng   │\n"
        "│   ✅ Rule effectiveness tracked      + AI suggests rule updates        │\n"
        "│   ✅ Human governance layer          + AI scorecard for exec view      │\n"
        "│                                                                        │\n"
        "│   Result: A data quality platform that improves itself,                │\n"
        "│   governed by humans, powered by AI, built on Databricks.             │\n"
        "│                                                                        │\n"
        "└──────────────────────────────────────────────────────────────────────┘"
    )
    code_block(sl, code, Inches(0.5), Inches(1.65), Inches(12.3), Inches(4.7), size=11.5)


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
        "Which next-stage opportunity (Alerting / Drift / Scorecard) aligns best with delivery priorities?",
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
    txb(sl, "5 phases complete  ·  48 unit tests passing  ·  Production-grade architecture  ·  Ready for next phase",
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

out = "/home/user/databricks-dqx-poc/AI_Infusion_Delivery_Assessment.pptx"
prs.save(out)
print(f"Saved: {out}  ({prs.slides.__len__()} slides)")

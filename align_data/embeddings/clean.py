"""Standalone text cleaning for alignment research articles.

Strips non-semantic garbage (base64, SVGs, Plotly JSON, data URIs, etc.)
while preserving prose, math, and URLs. Zero align_data dependencies --
importable without triggering the full package init.

Regexes extracted from text_splitter.chunks(); keep in sync.
"""
import re
import regex  # atomic grouping prevents catastrophic backtracking


def clean_text(doc: str) -> str:
    doc = re.sub(r"(?:\s*\n){4,}", "\n\n", doc)
    doc = re.sub(r'data:[a-zA-Z0-9/;,=+-]+;base64,[A-Za-z0-9+/=%\s]+', '[data-uri]', doc, flags=re.IGNORECASE)
    doc = re.sub(r'!\[[^\]]*\]\(data:[^)]+\)', '[data-uri-image]', doc, flags=re.IGNORECASE)
    doc = re.sub(r"'{4,}", "'", doc)
    doc = re.sub(r'(?<![:/\w])[A-Za-z0-9+/]{80,}={0,2}', '[base64]', doc)
    doc = re.sub(r'(?:^|["\s])([MLHVCSQTAZmlhvcsqtaz][0-9,.\s-]{100,})(?:["\s]|$)', ' [svg] ', doc)
    doc = re.sub(r'(?:\{"run":\s*\d+,\s*"p":\s*\[[^\]]+\],[^}]+\},?\s*)+', '[embedded-data]', doc)
    doc = re.sub(r'(?:-?\d+\.?\d*,){20,}', '[numeric-data]', doc)
    doc = re.sub(r'\[[0-9,.\s-]{50,}\]', '[json-array]', doc)
    doc = re.sub(r'(?:\[[\d.]+,\s*"#[0-9a-fA-F]{6}"\],?\s*){5,}', '[colorscale]', doc)
    doc = re.sub(r'"(?:x|y|z|color|colorscale|line|marker|mode|type|showlegend)":\s*(?:\[[^\]]{50,}\]|"[^"]{50,}")', '[plotly-prop]', doc)
    doc = regex.sub(r'\{"template":\{"data":(?>[^{}]*+(?:\{[^{}]*+\}[^{}]*+)*+)\}', '[plotly-template]', doc)
    doc = regex.sub(r'\{"[a-z_]+":(?>\[[^\]]*+\]|"[^"]*+"|[\d.]+|true|false|null)(?:,"[a-z_]+":(?>\[[^\]]*+\]|"[^"]*+"|[\d.]+|true|false|null))*+\}', '[plotly-obj]', doc)
    doc = regex.sub(r'"(?:line|marker|colorbar|colorscale|scene|xaxis|yaxis|zaxis|layout|template|data)":\s*\{(?>[^{}]*+(?:\{[^{}]*+\}[^{}]*+)*+)\}', '[plotly-nested]', doc)
    doc = re.sub(r'\{[^{}]*"type":\s*"(?:scatter|scatter3d|heatmap|surface|mesh3d|histogram|contour|bar|pie)"[^{}]*\}', '[plotly-trace]', doc)
    doc = re.sub(r'"text":\s*\["[^"]*"(?:,"[^"]*"){5,}\]', '[plotly-labels]', doc)
    doc = re.sub(r'"[xyz]":\s*\[-?[\d.,\s-]{20,}\]', '[coords]', doc)
    doc = re.sub(r'"(?:showlegend|hoverinfo|mode|scene)":\s*(?:true|false|"[^"]*")', '', doc)
    doc = re.sub(r'(?:\[(?:plotly-\w+|numeric-data|colorscale|coords|json-array)\],?\s*){3,}', '[data-sequence]', doc)

    def strip_long_json_tokens(m):
        token = m.group(0)
        if len(token) > 150 and ('{' in token or '[' in token or '":' in token):
            return '[json-fragment]'
        return token
    doc = re.sub(r'\S{151,}', strip_long_json_tokens, doc)

    doc = (
        doc
        .replace("<|endofprompt|>", "<endofprompt>")
        .replace("<|endoftext|>", "<endoftext>")
    )
    return doc

# user-defined transforms for examples/map.yaml (see docs/map.md, transforms).
# every top-level def becomes a template transform: ${key.slug|site_code}.

# the leading letters of a site slug, uppercased: "fra1" -> "FRA".
def site_code(slug):
    out = ""
    for c in slug.elems():
        if c.isdigit():
            break
        out += c
    return out.upper()

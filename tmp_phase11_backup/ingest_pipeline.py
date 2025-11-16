# backend/app/ingest_pipeline.py

import json, csv, io

from bs4 import BeautifulSoup



def parse_json_text(text):

    return json.loads(text)



def parse_csv_text(text, sep=','):

    f = io.StringIO(text)

    reader = csv.DictReader(f, delimiter=sep)

    return [dict(r) for r in reader]



def parse_txt_text(text):

    # simple heuristics: try lines of key:value or JSON per line

    docs = []

    for line in text.splitlines():

        line=line.strip()

        if not line:

            continue

        try:

            docs.append(json.loads(line))

            continue

        except Exception:

            pass

        if ':' in line:

            parts = [p.strip() for p in line.split(',')]

            d={}

            for p in parts:

                if ':' in p:

                    k,v=p.split(':',1)

                    d[k.strip()] = v.strip()

            if d:

                docs.append(d)

                continue

        # fallthrough: store raw line

        docs.append({"_raw_line": line})

    return docs



def parse_html_text(text):

    soup = BeautifulSoup(text, "html.parser")

    # try to read first table

    table = soup.find("table")

    if table:

        rows = table.find_all("tr")

        headers = [th.get_text(strip=True) for th in rows[0].find_all(['th','td'])]

        docs=[]

        for r in rows[1:]:

            vals = [td.get_text(strip=True) for td in r.find_all('td')]

            docs.append({h:v for h,v in zip(headers, vals)})

        return docs

    # otherwise attempt to parse JSON inside <pre> or script tags

    pre = soup.find("pre")

    if pre:

        return parse_json_text(pre.get_text())

    return []



def parse_file_bytes(path, content_type_hint=None):

    with open(path, "rb") as fh:

        raw = fh.read()

    # try utf-8 decode

    try:

        text = raw.decode("utf-8")

    except:

        text = raw.decode("latin1", errors="ignore")

    # heuristics

    if (content_type_hint and "json" in content_type_hint) or path.lower().endswith(".json"):

        return parse_json_text(text)

    if path.lower().endswith((".csv", ".tsv")):

        sep = '\t' if path.lower().endswith(".tsv") else ','

        return parse_csv_text(text, sep=sep)

    if path.lower().endswith(".html") or "<html" in text[:200].lower():

        return parse_html_text(text)

    if path.lower().endswith(".txt"):

        return parse_txt_text(text)

    # try JSON, then CSV fallback

    try:

        return parse_json_text(text)

    except:

        try:

            return parse_csv_text(text)

        except:

            return parse_txt_text(text)


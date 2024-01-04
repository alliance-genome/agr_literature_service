import requests
from os import environ
import time

root_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?api_key={environ['NCBI_API_KEY']}&db=pubmed&id="

valid_pmid = "28931816"
url = f"{root_url}{valid_pmid}"
response = requests.get(url)
content = response.text.replace("\n", "")
if "<PubmedArticleSet></PubmedArticleSet>" in content:
    print(f"PMID:{valid_pmid} is obsolete length={len(content)}")
else:
    print(f"PMID:{valid_pmid} is valid length={len(content)}")

"""
obsolete pmid returns the following:
b'<?xml version="1.0" ?>\n<!DOCTYPE PubmedArticleSet PUBLIC "-//NLM//DTD PubMedArticle, 1st January 2023//EN" "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_230101.dtd">\n<PubmedArticleSet>\n</PubmedArticleSet>'
"""

f = open("obsolete_pmids/obsolete_pmid.txt")
for line in f:
    pmid = line.strip()
    url = f"{root_url}{pmid}"
    response = requests.get(url)
    # content = str(response.content)
    content = response.text.replace("\n", "")
    time.sleep(6)
    if "<PubmedArticleSet></PubmedArticleSet>" in content:
        print(f"PMID:{pmid} is obsolete length={len(content)}")
    else:
        print(f"PMID:{pmid} is valid length={len(content)}")
f.close()



---
Bon, faut repasser en 3.7 pour sage
-> pyenv install 3.7

Poetry > 1.5 demande python > 3.8 qui ne peut plus ensuite installer des dépendences en python 3.7 !!!
En python3.7 
curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.5.0 python3 -

# install in /Users/molli-p/.local/bin/poetry

poetry env use 3.7
poetry shell !! (ou poetry run sage-debug...)
et l'environnement de travail est correct dans poetry shell

poetry env info

Virtualenv
Python:         3.7.17
Implementation: CPython
Path:           /Users/molli-p/Library/Caches/pypoetry/virtualenvs/sage-engine-YerWT6WO-py3.7
Executable:     /Users/molli-p/Library/Caches/pypoetry/virtualenvs/sage-engine-YerWT6WO-py3.7/bin/python
Valid:          True

System
Platform:   darwin
OS:         posix
Python:     3.7.17
Path:       /Users/molli-p/.pyenv/versions/3.7.17
Executable: /Users/molli-p/.pyenv/versions/3.7.17/bin/python3.7


---

poetry install --extras="hdt"


pb install HDT
-> pip install hdt...

and

poetry install --extras="hdt"

---
Grr, je sais pas pourquoi, la dépendance rdflib5 n'installe un "parserutils.py" correct.
Il faut le reinstaller à la main, a l'endroit d'ou vient l'erreur....
(chez moi dans le cache pypoetry
cd /Users/molli-p/Library/Caches/pypoetry/virtualenvs/sage-engine-YerWT6WO-py3.7/lib/python3.7/site-packages/rdflib/plugin/sparql
)

https://raw.githubusercontent.com/aucampia/rdflib/6f15c9aa9a96a4dcea46d43e9d4293bf12cc61de/rdflib/plugins/sparql/parserutils.py

---
Pratique de copier les tables sous postgres quand on fait des updates pour garder une version no modifiée
Attention
postgres=# CREATE DATABASE watdiv_renamed TEMPLATE watdiv_orig;
postgres=# ALTER TABLE old_table_name RENAME TO new_table_name; // ne pas oublier de renommer les tables !!

--
pas mal de tout tester avec sage-debug (les update aussi)

sage-debug config.yaml http://example.org/watdiv_renamed -q "select * {?s <http://auction.example.org/bid> ?o}"

sage-explain (pas mal aussi)


--
unique relevant full
SELECT * WHERE {
?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory0> .
?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> ?v2 .
?v3 <http://db.uwaterloo.ca/~galuc/wsdbm/likes> ?v0 .  }

VS

unique relevant full naive
SELECT (coalesce( ?altid, ?v0 ) as ?link) ?v3 ?v2 WHERE {
 ?v0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/ProductCategory0> .
 ?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre> ?v2 .
 ?v3 <http://db.uwaterloo.ca/~galuc/wsdbm/likes> ?v0 .
 optional { ?v0  <http://auction.example.org/bid> ?bid;
                 <http://www.w3.org/2002/07/owl#sameAs> ?altid } }
order by ?bid


--
On met du Snakefile.

On appelle avec :
snakemake --configfile config/xp-watdiv.yaml -j1



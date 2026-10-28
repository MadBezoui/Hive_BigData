# -*- coding: utf-8 -*-
"""
Corrections du workshop Hive (réservé au tuteur, à ne pas publier).

Placez ce fichier à côté de hive_workshop.py : l'interface affiche alors les corrections,
le corrigé du sujet 2023, les résultats attendus et la version Python de chaque question.
Sans lui, le script fonctionne en mode élèves.
"""

import datetime as _dt
import math

CORRIGES = {
    1: {
        "solution": "SELECT COUNT(id_empl) AS nombre_employes\nFROM fonction_employes\nWHERE annee_embauche > 2019;",
        "subject": "SELECT count(id_empl) AS nombre_employes FROM fonction_employes WHERE annee_embauche > '2019';",
        "expected": "7 employés (3 en 2020, 3 en 2021, 1 en 2022). Le corrigé du sujet donne le même résultat."
    },
    2: {
        "solution": "SELECT EM.nom, EM.prenom, ES.secteur\nFROM employes EM\nJOIN salaire_employes ES ON EM.id_empl = ES.id_empl\nWHERE CAST(REGEXP_REPLACE(ES.salaire, '[^0-9]', '') AS INT) > 35000;",
        "subject": "SELECT EM.nom, EM.prenom, ES.secteur\nFROM employes EM\nJOIN salaire_employes ES ON EM.id_empl = ES.id_empl\nWHERE CAST(SUBSTR(ES.salaire, 1, 5) AS INT) > 35000;",
        "expected": "24 lignes. Le corrigé du sujet en donne 21 : SUBSTR(…, 1, 5) lit 144000 comme 14400 et perd les ids 13, 30 et 31."
    },
    3: {
        "solution": "CREATE VIEW IF NOT EXISTS vue1 AS\nSELECT e.id_empl AS empID, e.nom AS empNOM, e.prenom AS empPRENOM,\n       f.annee_embauche AS anneeRecrutement\nFROM employes e\nJOIN fonction_employes f ON e.id_empl = f.id_empl\nWHERE f.annee_embauche > 2010;\n\nSELECT v1.empID, v1.empNOM, v1.empPRENOM\nFROM vue1 v1\nJOIN salaire_employes se ON v1.empID = se.id_empl\nWHERE se.secteur = 'education';",
        "subject": "CREATE VIEW IF NOT EXISTS vue1 AS\nSELECT employes.id_empl AS empID, employes.nom AS empNOM,\n  employes.prenom AS empPRENOM, fonction_employes.annee_embauche AS anneeRecrutement,\n  fonction_employes.id_empl AS empID_fonction\nFROM employes\nJOIN fonction_employes ON employes.id_empl = fonction_employes.id_empl\nWHERE fonction_employes.annee_embauche > '2010';\n\nSELECT v1.empID, v1.empNOM, v1.empPRENOM\nFROM vue1 v1\nJOIN salaire_employes SE ON v1.empID = SE.id_empl\nWHERE SE.secteur = 'Education';",
        "expected": "1 ligne : CANSALADE CHARLOTTE HENRIETTE (id 19, 2018). Le corrigé du sujet filtre sur 'Education' avec une majuscule et renvoie 0 ligne."
    },
    4: {
        "solution": "SELECT e1.id_empl, e1.nom, e1.prenom,\n       YEAR(CURRENT_DATE()) - f1.annee_embauche AS experience\nFROM employes e1\nJOIN fonction_employes f1 ON e1.id_empl = f1.id_empl\nORDER BY experience DESC\nLIMIT 1;",
        "subject": "SELECT e1.id_empl, e1.nom, e1.prenom, YEAR(CURRENT_DATE()) - f1.annee_embauche AS experience\nFROM employes e1\nJOIN fonction_employes f1 ON e1.id_empl = f1.id_empl\nORDER BY experience DESC\nLIMIT 1;",
        "expected": "TODORIS VASSILIKI (id 4), embauchée en 1970, sans ex æquo. Même résultat avec le corrigé du sujet."
    },
    5: {
        "solution": "SELECT ROUND(AVG(CAST(REGEXP_REPLACE(salaire, '[^0-9]', '') AS DECIMAL(10, 2))), 2) AS salaire_moyen\nFROM salaire_employes;",
        "subject": "SELECT AVG(CAST(SUBSTRING(salaire_employes.salaire, 1, LENGTH(salaire_employes.salaire) - 1)\n           AS DECIMAL(10, 2))) AS salaire_moyen\nFROM salaire_employes;",
        "expected": "55 696,97 €. Le corrigé du sujet retire un seul caractère : il reste « 20000euro/a », le CAST donne NULL et la moyenne est NULL."
    },
    6: {
        "solution": "SELECT fe.id_empl, fe.fonction, fe.annee_embauche\nFROM fonction_employes fe\nJOIN salaire_employes se ON fe.id_empl = se.id_empl\nWHERE se.sexe = 'F'\nORDER BY fe.annee_embauche DESC;",
        "subject": "SELECT fe.id_empl, fe.fonction, fe.annee_embauche from fonction_employes fe join salaire_employes se\non (fe.id_empl=se.id_empl) and se.sexe='F' order by fe.annee_embauche DESC;",
        "expected": "16 lignes, de technicien (2022) à professeur (1970). Même résultat avec le corrigé du sujet."
    },
    7: {
        "solution": "CREATE VIEW IF NOT EXISTS vue4 AS\nSELECT e.id_empl AS empID, e.nom AS empNOM, e.prenom AS empPRENOM,\n       f.fonction AS empFONCTION\nFROM employes e\nJOIN fonction_employes f ON e.id_empl = f.id_empl;\n\nSELECT v4.empPRENOM, v4.empFONCTION\nFROM vue4 v4\nJOIN salaire_employes se ON v4.empID = se.id_empl\nWHERE se.code_postal BETWEEN 75000 AND 75999;",
        "subject": "CREATE VIEW IF NOT EXISTS vue4 AS\nSELECT employes.id_empl AS empID, employes.nom AS empNOM,\n  employes.prenom AS empPRENOM, fonction_employes.fonction AS empFONCTION\nFROM employes\nJOIN fonction_employes ON employes.id_empl = fonction_employes.id_empl;\n\nSELECT v4.empPRENOM, v4.empNOM, v4.empFONCTION\nFROM vue4 v4\nJOIN salaire_employes se ON v4.empID = se.id_empl\nWHERE se.code_postal = 75000;",
        "expected": "Avec = 75000 (corrigé du sujet) : DOUGLAS LOUIS LEON, taxi. Avec BETWEEN 75000 AND 75999 : + PIERRE, veterinaire (code 75780)."
    },
    8: {
        "solution": "SELECT\n  SUM(CASE WHEN sexe = 'F' THEN sal END) / SUM(CASE WHEN sexe = 'F' THEN anc END) AS ratio_femmes,\n  SUM(CASE WHEN sexe = 'M' THEN sal END) / SUM(CASE WHEN sexe = 'M' THEN anc END) AS ratio_hommes\nFROM (\n  SELECT se.sexe,\n         CAST(REGEXP_REPLACE(se.salaire, '[^0-9]', '') AS DECIMAL(10,2)) AS sal,\n         YEAR(CURRENT_DATE()) - fe.annee_embauche AS anc\n  FROM salaire_employes se\n  JOIN fonction_employes fe ON se.id_empl = fe.id_empl\n) t;",
        "subject": "select sum(substr(se.salaire, 1, 5))/sum((cast(year(current_date()) as int)-fe.annee_embauche)) AS ratio_femmes\nFROM salaire_employes se join fonction_employes fe on (se.id_empl=fe.id_empl) and se.sexe='F';\nselect sum(substr(se.salaire, 1, 5))/sum((cast(year(current_date()) as int)-fe.annee_embauche)) AS ratio_hommes\nFROM salaire_employes se join fonction_employes fe on (se.id_empl=fe.id_empl) and se.sexe='M';",
        "expected": "En 2026 : femmes ≈ 1 699 € par année d'ancienneté, hommes ≈ 3 596 €. Le corrigé du sujet donne ≈ 2 339 € pour les hommes : les trois salaires mal lus sont masculins."
    },
    9: {
        "solution": "SELECT corr(YEAR(CURRENT_DATE()) - fe.annee_embauche,\n            CAST(REGEXP_REPLACE(se.salaire, '[^0-9]', '') AS INT)) AS coefficient_correlation\nFROM salaire_employes se\nJOIN fonction_employes fe ON se.id_empl = fe.id_empl;",
        "subject": "select corr(cast(year(current_date()) as int)-fe.annee_embauche, cast(substr(se.salaire, 1, 5) as int))\nfrom salaire_employes se join fonction_employes fe on (se.id_empl=fe.id_empl);",
        "expected": "r ≈ 0,09 : pas de liaison linéaire entre ancienneté et salaire. Le corrigé du sujet donne r ≈ −0,02."
    },
    10: {
        "solution": "SELECT secteur,\n       ROUND(AVG(CAST(REGEXP_REPLACE(salaire, '[^0-9]', '') AS DECIMAL(10, 2))), 2) AS salaire_moyen,\n       COUNT(*) AS nb_employes\nFROM salaire_employes\nGROUP BY secteur\nORDER BY salaire_moyen DESC;",
        "subject": "select se.secteur, sum(substr(se.salaire, 1, 5))/sum((cast(year(current_date()) as int)-fe.annee_embauche)) AS ratio\nfrom fonction_employes fe join salaire_employes se on (fe.id_empl=se.id_empl)\ngroup by se.secteur;",
        "expected": "En tête : profession juridique privé 135 000 € (2 employés), transports aériens 132 000 € (2). En bas : industrie et fonction publique, 35 000 €. Avec SUBSTR(…, 1, 5), transports aériens tombe à la dernière place."
    },
}


# Les 10 questions en Python pur (sans SQL)

def q1(employes, fonctions, salaires):
    n = sum(1 for f in fonctions if f["annee_embauche"] > 2019)
    return ["nombre_employes"], [[n]]


def q2(employes, fonctions, salaires):
    par_id = {e["id_empl"]: e for e in employes}
    lignes = [[par_id[s["id_empl"]]["nom"], par_id[s["id_empl"]]["prenom"], s["secteur"]]
              for s in salaires if s["euros"] > 35000 and s["id_empl"] in par_id]
    return ["nom", "prenom", "secteur"], lignes


def q3(employes, fonctions, salaires):
    annee = {f["id_empl"]: f["annee_embauche"] for f in fonctions}
    secteur = {s["id_empl"]: s["secteur"] for s in salaires}
    lignes = [[e["id_empl"], e["nom"], e["prenom"]] for e in employes
              if annee.get(e["id_empl"], 0) > 2010 and secteur.get(e["id_empl"]) == "education"]
    return ["empid", "empnom", "empprenom"], lignes


def q4(employes, fonctions, salaires):
    annee_courante = _dt.date.today().year
    par_id = {e["id_empl"]: e for e in employes}
    plus_ancien = min(fonctions, key=lambda f: f["annee_embauche"])
    e = par_id[plus_ancien["id_empl"]]
    return ["id_empl", "nom", "prenom", "experience"], [[e["id_empl"], e["nom"], e["prenom"],
                                                          annee_courante - plus_ancien["annee_embauche"]]]


def q5(employes, fonctions, salaires):
    montants = [s["euros"] for s in salaires]
    return ["salaire_moyen"], [[round(sum(montants) / len(montants), 2)]]


def q6(employes, fonctions, salaires):
    sexe = {s["id_empl"]: s["sexe"] for s in salaires}
    femmes = [f for f in fonctions if sexe.get(f["id_empl"]) == "F"]
    femmes.sort(key=lambda f: f["annee_embauche"], reverse=True)
    return ["id_empl", "fonction", "annee_embauche"], [[f["id_empl"], f["fonction"], f["annee_embauche"]] for f in femmes]


def q7(employes, fonctions, salaires):
    fonction = {f["id_empl"]: f["fonction"] for f in fonctions}
    paris = {s["id_empl"] for s in salaires if 75000 <= s["code_postal"] <= 75999}
    return ["empprenom", "empfonction"], [[e["prenom"], fonction[e["id_empl"]]] for e in employes if e["id_empl"] in paris]


def q8(employes, fonctions, salaires):
    annee_courante = _dt.date.today().year
    annee = {f["id_empl"]: f["annee_embauche"] for f in fonctions}
    ratios = []
    for sexe in ("F", "M"):
        groupe = [s for s in salaires if s["sexe"] == sexe]
        somme_salaires = sum(s["euros"] for s in groupe)
        somme_anciennete = sum(annee_courante - annee[s["id_empl"]] for s in groupe)
        ratios.append(somme_salaires / somme_anciennete)
    return ["ratio_femmes", "ratio_hommes"], [ratios]


def q9(employes, fonctions, salaires):
    annee_courante = _dt.date.today().year
    annee = {f["id_empl"]: f["annee_embauche"] for f in fonctions}
    x = [annee_courante - annee[s["id_empl"]] for s in salaires]
    y = [s["euros"] for s in salaires]
    mx, my = sum(x) / len(x), sum(y) / len(y)
    cov = sum((a - mx) * (b - my) for a, b in zip(x, y))
    r = cov / math.sqrt(sum((a - mx) ** 2 for a in x) * sum((b - my) ** 2 for b in y))
    return ["coefficient_correlation"], [[r]]


def q10(employes, fonctions, salaires):
    par_secteur = {}
    for s in salaires:
        par_secteur.setdefault(s["secteur"], []).append(s["euros"])
    lignes = [[sect, round(sum(v) / len(v), 2), len(v)] for sect, v in par_secteur.items()]
    lignes.sort(key=lambda l: l[1], reverse=True)
    return ["secteur", "salaire_moyen", "nb_employes"], lignes


PY_FUNCS = {1: q1, 2: q2, 3: q3, 4: q4, 5: q5, 6: q6, 7: q7, 8: q8, 9: q9, 10: q10}


def verifier_questions(run, check, QUESTIONS, donnees):
    """Contrôles des résultats attendus de Q1 à Q10 (appelé par « hive_workshop.py verifier »)."""
    year = _dt.date.today().year
    py = {n: PY_FUNCS[n](*donnees) for n in PY_FUNCS}

    def sol(n, key="solution"):
        return run([q for q in QUESTIONS if q["n"] == n][0][key])[-1]

    def close(a, b, eps=0.01):
        return a is not None and b is not None and abs(a - b) < eps

    check("Q1 = 7 (solution, sujet et Python)", sol(1)["rows"] == [[7]] and sol(1, "subject")["rows"] == [[7]] and py[1][1] == [[7]])
    check("Q2 : 24 lignes (sujet : 21, Python : 24)", len(sol(2)["rows"]) == 24 and len(sol(2, "subject")["rows"]) == 21 and len(py[2][1]) == 24)
    check("Q3 : CANSALADE seule (sujet : 0 ligne)", [r[1] for r in sol(3)["rows"]] == ["CANSALADE"]
          and sol(3, "subject")["rows"] == [] and [r[1] for r in py[3][1]] == ["CANSALADE"])
    r4 = sol(4)["rows"]
    check("Q4 : TODORIS, %d ans" % (year - 1970), r4 and r4[0][1] == "TODORIS" and r4[0][3] == year - 1970 and py[4][1][0][1] == "TODORIS")
    check("Q5 : 55696.97 (sujet : NULL)", close(sol(5)["rows"][0][0], 55696.97) and sol(5, "subject")["rows"] == [[None]]
          and close(py[5][1][0][0], 55696.97))
    check("Q6 : 16 lignes, 2022 en tête", len(sol(6)["rows"]) == 16 and sol(6)["rows"][0][2] == 2022 and len(py[6][1]) == 16)
    check("Q7 : DOUGLAS (+ PIERRE avec BETWEEN)", sorted(r[0] for r in sol(7)["rows"]) == ["DOUGLAS LOUIS LEON", "PIERRE"]
          and [r[0] for r in sol(7, "subject")["rows"]] == ["DOUGLAS LOUIS LEON"])
    r8 = sol(8)["rows"][0]
    check("Q8 : ratios cohérents avec Python (%.2f / %.2f)" % (r8[0], r8[1]), close(r8[0], py[8][1][0][0]) and close(r8[1], py[8][1][0][1]))
    if year == 2026:
        check("Q8 en 2026 : 1699.21 / 3596.39", close(r8[0], 1699.21) and close(r8[1], 3596.39))
    check("Q9 : r ≈ 0.089 (sujet ≈ -0.023)", close(sol(9)["rows"][0][0], 0.0894, 0.001)
          and close(sol(9, "subject")["rows"][0][0], -0.023, 0.001) and close(py[9][1][0][0], 0.0894, 0.001))
    r10 = sol(10)["rows"]
    check("Q10 : juridique privé en tête à 135000", r10 and r10[0][0].startswith("profession juridique priv") and close(r10[0][1], 135000))

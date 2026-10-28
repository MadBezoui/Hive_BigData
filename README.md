# Mini-Hive · le workshop Hive sans Hadoop

Un seul fichier Python pour pratiquer Apache Hive sans machine virtuelle Cloudera. Il suffit de Python 3.8 ou plus récent : aucune dépendance n'est nécessaire, la bibliothèque standard suffit.

Le script rejoue un workshop d'initiation à Hive (bases et tables, partitions et buckets, analyse de données) et propose une interface web locale.

![Aperçu de l'interface](docs/apercu.png)

## Démarrer

```bash
python3 hive_workshop.py            # ouvre l'interface sur http://localhost:8765
```

Le script lit les fichiers de données du workshop dans un dossier `donnees/`, placé à côté du script ou dans le dossier parent. On peut aussi lui indiquer un autre dossier avec `--donnees CHEMIN`. Les fichiers attendus :

| Fichier | Contenu | Séparateur | En-tête |
|---|---|---|---|
| `emploi_population.txt` | genre ; date de naissance ; fonction ; salaire ; code postal ; id | `;` | non |
| `personne.txt` | id ; nom ; prénom | `;` | non |
| `internautes_log.txt` | id ; prénom ; site ; date (JJ-MM-AAAA) ; pays | `;` | non |
| `employes.txt` | id_empl ; nom ; prénom ; date de naissance | `;` | non |
| `fonction_employes.txt` | id_empl ; année d'embauche ; fonction | `;` | oui |
| `salaire_employes.txt` | sexe ; secteur ; salaire (`20000euro/an`) ; code postal ; id_empl | `;` | oui |

Ces données sont fournies en séance et ne font pas partie du dépôt.

## Commandes

| Commande | Effet |
|---|---|
| `python3 hive_workshop.py` | Interface web |
| `python3 hive_workshop.py rejouer` | Rejoue toutes les étapes dans le terminal |
| `python3 hive_workshop.py console` | Invite `hive>` interactive, comme le Hive CLI |
| `python3 hive_workshop.py sql "SHOW DATABASES;"` | Exécute des instructions puis quitte |
| `python3 hive_workshop.py verifier` | Contrôles automatiques sur les données |
| `python3 hive_workshop.py reset` | Vide le HDFS simulé et le Metastore |

## Ce qui est simulé

- **HDFS** : un vrai dossier, `mini_hdfs/hdfs/`, où l'on voit apparaître les répertoires de partitions et les fichiers de buckets. Commandes prises en charge : `hdfs dfs` et `hadoop fs` avec `-ls [-R]`, `-mkdir [-p]`, `-copyFromLocal`, `-put`, `-cat`, `-rm [-r]`, `-mv`, `-cp`, `-du`, `-chmod`. Les chemins locaux de la VM (`/home/cloudera/…`) sont cherchés dans `donnees/`.
- **Metastore** : un fichier JSON.
- **HiveQL** :
  - Définition : `CREATE DATABASE`, `CREATE [EXTERNAL] TABLE` (avec `LOCATION`, `PARTITIONED BY`, `CLUSTERED BY … INTO n BUCKETS`, `skip.header.line.count`), CTAS, `CREATE VIEW`, `DROP`.
  - Données : `LOAD DATA`, `INSERT OVERWRITE|INTO … PARTITION`, `SET`.
  - Consultation : `DESCRIBE [FORMATTED]`, `SHOW PARTITIONS`, `SHOW TBLPROPERTIES`.
  - Requêtes : `SELECT` avec jointures, vues, `GROUP BY`, `CASE WHEN`, `corr()`, `COLLECT_SET`, `REGEXP_REPLACE` et `INPUT__FILE__NAME`.
- **Comportements de Hive 1.1 reproduits** :
  - Un `CAST` raté renvoie `NULL`.
  - `/` renvoie toujours un nombre décimal.
  - Les colonnes manquantes valent `NULL`.
  - `LOAD DATA INPATH` déplace le fichier.
  - Supprimer une table interne efface ses données ; une table externe les garde.
  - CTAS externe refusé (Error 10070).
  - Mode strict des partitions dynamiques (Error 10096).
  - Buckets par `hash % N`, avec des fichiers vides.
  - Sans `hive.enforce.bucketing`, un seul fichier.
  - Erreur « Not a file » quand un répertoire `LOCATION` contient un sous-répertoire.

Les requêtes sont traduites puis exécutées par SQLite. La distribution, MapReduce, YARN et les temps de réponse ne sont pas simulés. Pour chaque `SELECT`, le script indique combien de fichiers Hive lirait, y compris quand des partitions sont éliminées.

## Corrections

Le dépôt ne contient pas les corrections. Si un fichier `corriges.py` est placé à côté du script, l'interface passe en mode tuteur et affiche alors :
- les solutions ;
- le corrigé d'origine du sujet ;
- les résultats attendus ;
- une version en Python pur de chaque question.

Sans ce fichier, l'interface est en mode élèves.

# Interface web — DataLake Météo

Site **Next.js 14** (App Router, export statique) déployable sur **Vercel**.

## Pourquoi un export statique

Vercel n'a **aucun accès réseau** au cluster : HDFS, Kafka et Spark tournent sur
`localhost`, derrière Docker. Les données voyagent donc **avec le site** :
`make export-web` lit les tables Gold et écrit des JSON dans `public/data/`,
que le build incorpore.

Conséquences assumées :

- le site est **toujours en ligne**, même cluster éteint — utile le jour de la soutenance ;
- il affiche un **instantané**, daté dans l'en-tête pour que personne ne le prenne
  pour du temps réel ;
- rafraîchir = `make export-web` puis redéployer (un `git push` suffit sur Vercel).

## En local

```bash
make export-web     # génère public/data/*.json depuis les tables Gold
make web-install    # npm install, dans un conteneur node
make web-dev        # http://localhost:3000
```

Node tourne **dans un conteneur** (`profiles: ["web"]`, il ne démarre donc pas
avec `make all`). Rien à installer sur la machine, et `node_modules` reste dans
un volume Docker plutôt que sur le disque monté.

## Déploiement Vercel

1. Pousser le dépôt sur GitHub.
2. Sur vercel.com : **New Project** → importer le dépôt.
3. **Root Directory : `web`** (le seul réglage à ne pas oublier).
4. Framework *Next.js* détecté automatiquement ; aucune variable d'environnement
   n'est requise — le site ne contacte aucun service à l'exécution.

## Design des graphiques

Palette catégorielle **validée** (séparation daltonisme et contraste, en clair et
en sombre) : une couleur par ville, dans un **ordre fixe** — un filtre ne repeint
jamais les séries restantes. Trois teintes passent sous 3:1 sur la surface claire :
la règle de secours s'applique, d'où la **bascule tableau** présente sur les
graphiques concernés. Les couleurs de statut (alerte / extrême) sont réservées et
toujours accompagnées d'une pastille **et** d'un libellé — jamais la couleur seule.
